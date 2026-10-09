package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var riskStatuses = []string{"open", "confirmed", "resolved", "dismissed", "ignored", "manually_resolved"}
var riskSeverities = []string{"high", "medium", "low"}

// The issues API caps a query at a 2 day window, 7 days back.
const issueWindow = 48 * time.Hour

// Each name is both a config toggle and a collection step label.
const (
	MetricClusters     = "clusters"
	MetricRisks        = "risks"
	MetricRisksActive  = "risks_active"
	MetricRisksByCheck = "risks_by_check"
	MetricIssues       = "issues"
)

var MetricNames = []string{MetricClusters, MetricRisks, MetricRisksActive, MetricRisksByCheck, MetricIssues}

// ParseEnabled starts with everything on, applies explicit config values, then the disabled list.
func ParseEnabled(cfg map[string]bool, disabled []string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, n := range MetricNames {
		out[n] = true
	}
	set := func(name string, on bool) error {
		if _, ok := out[name]; !ok {
			return fmt.Errorf("unknown metric group %q, want one of %v", name, MetricNames)
		}
		out[name] = on
		return nil
	}
	for n, on := range cfg {
		if err := set(n, on); err != nil {
			return nil, err
		}
	}
	for _, n := range disabled {
		if err := set(n, false); err != nil {
			return nil, err
		}
	}
	return out, nil
}

type Collector struct {
	api      *Client
	interval time.Duration
	enabled  map[string]bool

	clusters     prometheus.Gauge
	risks        *prometheus.GaugeVec
	risksActive  *prometheus.GaugeVec
	risksByCheck *prometheus.GaugeVec
	issuesOpen   *prometheus.GaugeVec
	issuesClosed *prometheus.CounterVec
	stepDuration *prometheus.HistogramVec
	scrapeErrors prometheus.Counter
	lastSuccess  prometheus.Gauge

	// The API gives issues no ID, so closed issues are deduped on this key.
	seenClosed  map[string]time.Time
	firstIssues bool
}

func NewCollector(api *Client, interval time.Duration, enabled map[string]bool, reg prometheus.Registerer) *Collector {
	f := func(name, help string, labels ...string) *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "komodor_" + name, Help: help}, labels)
	}
	c := &Collector{
		api: api, interval: interval, enabled: enabled, seenClosed: map[string]time.Time{}, firstIssues: true,
		clusters:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "komodor_clusters", Help: "Clusters connected to Komodor."}),
		risks:        f("reliability_risks", "Reliability risks by status and severity.", "status", "severity"),
		risksActive:  f("reliability_risks_active", "Open and confirmed reliability risks by cluster and severity.", "cluster", "severity"),
		risksByCheck: f("reliability_risks_by_check", "Open and confirmed reliability risks by check type.", "check_type"),
		issuesOpen:   f("issues_open", "Open issues by cluster and type (issues older than 2 days are not seen).", "cluster", "type"),
		issuesClosed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "komodor_issues_closed_total", Help: "Issues observed closing since the exporter started."}, []string{"cluster", "type"}),
		stepDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "komodor_exporter_collection_step_duration_seconds", Help: "Duration of each collection step.",
			Buckets: durationBuckets}, []string{"step"}),
		scrapeErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "komodor_exporter_errors_total", Help: "Failed collection attempts."}),
		lastSuccess:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "komodor_exporter_last_success_timestamp_seconds", Help: "Unix time of the last fully successful collection."}),
	}
	// Disabled groups are never registered, so their series do not appear in /metrics at all.
	reg.MustRegister(c.stepDuration, c.scrapeErrors, c.lastSuccess)
	groups := map[string][]prometheus.Collector{
		MetricClusters:     {c.clusters},
		MetricRisks:        {c.risks},
		MetricRisksActive:  {c.risksActive},
		MetricRisksByCheck: {c.risksByCheck},
		MetricIssues:       {c.issuesOpen, c.issuesClosed},
	}
	var on []string
	for _, n := range MetricNames {
		if enabled[n] {
			reg.MustRegister(groups[n]...)
			on = append(on, n)
		}
	}
	if len(on) == 0 {
		slog.Warn("all metric groups are disabled; the exporter will not call the Komodor API")
	} else {
		slog.Info("metric groups enabled", "groups", on)
	}
	return c
}

func (c *Collector) Run(ctx context.Context) {
	slog.Info("collector started", "interval", c.interval.String())
	t := time.NewTicker(c.interval)
	defer t.Stop()
	for {
		start := time.Now()
		pollCtx, cancel := context.WithTimeout(ctx, c.interval)
		err := c.collect(pollCtx)
		cancel()
		if err != nil {
			c.scrapeErrors.Inc()
			slog.Error("collection failed", "err", err, "duration", time.Since(start).String())
		} else {
			c.lastSuccess.SetToCurrentTime()
			slog.Info("collection complete", "duration", time.Since(start).String())
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

var activeStatuses = []string{"open", "confirmed"}

// fanout runs fn for 0..n-1 concurrently; the client's semaphore bounds the real API concurrency.
func fanout(n int, fn func(i int) error) error {
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = fn(i)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// collect runs the independent steps concurrently once the cluster list is known. A step publishes
// only when it fully succeeds, so a failed step keeps its previous values instead of reporting zeros.
func (c *Collector) collect(ctx context.Context) error {
	// The cluster list is a dependency of other groups, so it is fetched if any of them needs it.
	var clusters []string
	if c.enabled[MetricClusters] || c.enabled[MetricRisksActive] || c.enabled[MetricIssues] {
		stepStart := time.Now()
		var err error
		clusters, err = c.api.Clusters(ctx)
		c.stepDuration.WithLabelValues(MetricClusters).Observe(time.Since(stepStart).Seconds())
		if err != nil {
			return fmt.Errorf("clusters: %w", err)
		}
		if c.enabled[MetricClusters] {
			c.clusters.Set(float64(len(clusters)))
		}
		slog.Debug("collected clusters", "count", len(clusters))
	}

	type step struct {
		name string
		fn   func(context.Context) error
	}
	all := []step{
		{MetricRisks, c.collectRiskCounts},
		{MetricRisksActive, func(ctx context.Context) error { return c.collectActiveRisks(ctx, clusters) }},
		{MetricRisksByCheck, c.collectRisksByCheck},
		{MetricIssues, func(ctx context.Context) error { return c.collectIssues(ctx, clusters) }},
	}
	var steps []step
	for _, s := range all {
		if c.enabled[s.name] {
			steps = append(steps, s)
		}
	}
	return fanout(len(steps), func(i int) error {
		s := steps[i]
		start := time.Now()
		err := s.fn(ctx)
		c.stepDuration.WithLabelValues(s.name).Observe(time.Since(start).Seconds())
		slog.Debug("collection step done", "step", s.name, "duration", time.Since(start).String(), "ok", err == nil)
		if err != nil {
			return fmt.Errorf("%s: %w", s.name, err)
		}
		return nil
	})
}

// counts runs one count query per item. A failed item is reported in errs and left out of the publish,
// so one slow or failing query keeps its previous value instead of blanking the whole group.
func counts(n int, fn func(i int) (int, error)) (vals []int, errs []error) {
	vals, errs = make([]int, n), make([]error, n)
	_ = fanout(n, func(i int) error {
		vals[i], errs[i] = fn(i)
		return nil
	})
	return vals, errs
}

func (c *Collector) collectRiskCounts(ctx context.Context) error {
	ns := len(riskSeverities)
	vals, errs := counts(len(riskStatuses)*ns, func(i int) (int, error) {
		return c.api.RiskCount(ctx, RiskFilter{Statuses: []string{riskStatuses[i/ns]}, Severity: riskSeverities[i%ns]})
	})
	for i, v := range vals {
		if errs[i] == nil {
			c.risks.WithLabelValues(riskStatuses[i/ns], riskSeverities[i%ns]).Set(float64(v))
		}
	}
	return errors.Join(errs...)
}

func (c *Collector) collectActiveRisks(ctx context.Context, clusters []string) error {
	ns := len(riskSeverities)
	vals, errs := counts(len(clusters)*ns, func(i int) (int, error) {
		return c.api.RiskCount(ctx, RiskFilter{Statuses: activeStatuses, Cluster: clusters[i/ns], Severity: riskSeverities[i%ns]})
	})
	failed := errors.Join(errs...)
	if failed == nil {
		c.risksActive.Reset()
	}
	for i, v := range vals {
		if errs[i] == nil {
			c.risksActive.WithLabelValues(clusters[i/ns], riskSeverities[i%ns]).Set(float64(v))
		}
	}
	return failed
}

func (c *Collector) collectRisksByCheck(ctx context.Context) error {
	vals, errs := counts(len(CheckTypes), func(i int) (int, error) {
		return c.api.RiskCount(ctx, RiskFilter{Statuses: activeStatuses, CheckType: CheckTypes[i]})
	})
	for i, v := range vals {
		if errs[i] == nil {
			c.risksByCheck.WithLabelValues(CheckTypes[i]).Set(float64(v))
		}
	}
	return errors.Join(errs...)
}

// collectIssues makes one call per cluster and type (open and closed together). A failed pair keeps its
// previous value rather than failing the whole step, because one flaky cluster should not blank the rest.
func (c *Collector) collectIssues(ctx context.Context, clusters []string) error {
	now := time.Now()
	from := now.Add(-issueWindow)
	nt := len(IssueTypes)
	results := make([][]Issue, len(clusters)*nt)
	errs := make([]error, len(results))
	_ = fanout(len(results), func(i int) error {
		cl, typ := clusters[i/nt], IssueTypes[i%nt]
		is, err := c.api.Issues(ctx, cl, typ, []string{"open", "closed"}, from, now)
		if err != nil {
			errs[i] = fmt.Errorf("issues %s/%s: %w", cl, typ, err)
			return nil
		}
		results[i] = is
		return nil
	})
	failed := errors.Join(errs...)

	if failed == nil {
		c.issuesOpen.Reset()
	}
	for i, is := range results {
		if errs[i] != nil {
			continue
		}
		cl, typ := clusters[i/nt], IssueTypes[i%nt]
		open := 0
		for _, iss := range is {
			switch iss.Status {
			case "open":
				open++
			case "closed":
				k := fmt.Sprintf("%s|%s|%d|%s", cl, typ, iss.StartTime, iss.Summary)
				if _, dup := c.seenClosed[k]; dup {
					continue
				}
				c.seenClosed[k] = now
				if !c.firstIssues {
					c.issuesClosed.WithLabelValues(cl, typ).Inc()
				}
			}
		}
		c.issuesOpen.WithLabelValues(cl, typ).Set(float64(open))
		slog.Debug("collected issues", "cluster", cl, "type", typ, "open", open, "total", len(is))
	}
	for k, seen := range c.seenClosed {
		if seen.Before(now.Add(-issueWindow - time.Hour)) {
			delete(c.seenClosed, k)
		}
	}
	if failed == nil {
		c.firstIssues = false
	}
	return failed
}
