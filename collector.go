package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var riskStatuses = []string{"open", "confirmed", "resolved", "dismissed", "ignored", "manually_resolved"}
var riskSeverities = []string{"high", "medium", "low"}

// The issues API caps a query at a 2 day window, 7 days back. Open issues are always read over the
// full window so long-running ones stay in the gauge; only closed issues use the shorter --issues-window.
const maxIssueWindow = 48 * time.Hour

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

	closedWindow time.Duration
	filter       IssueFilter
	warnedSkips  bool

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

func NewCollector(api *Client, interval, closedWindow time.Duration, enabled map[string]bool, filter IssueFilter, reg prometheus.Registerer) *Collector {
	f := func(name, help string, labels ...string) *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "komodor_" + name, Help: help}, labels)
	}
	c := &Collector{
		api: api, interval: interval, closedWindow: closedWindow, enabled: enabled, filter: filter, seenClosed: map[string]time.Time{}, firstIssues: true,
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
	pairs := c.filter.Pairs(clusters)
	if !c.warnedSkips {
		c.warnedSkips = true
		for _, s := range c.filter.UnmatchedSkips(clusters) {
			slog.Warn("skip-issues entry matches no cluster", "entry", s)
		}
	}
	results := make([][]Issue, len(pairs))
	errs := make([]error, len(pairs))
	_ = fanout(len(pairs), func(i int) error {
		cl, typ := pairs[i][0], pairs[i][1]
		is, err := c.fetchIssues(ctx, cl, typ, now)
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
		cl, typ := pairs[i][0], pairs[i][1]
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
		if seen.Before(now.Add(-c.closedWindow - time.Hour)) {
			delete(c.seenClosed, k)
		}
	}
	if failed == nil {
		c.firstIssues = false
	}
	return failed
}

// fetchIssues reads open and closed issues in separate calls: open over the full window (few rows), closed
// over the short window (the bulk of the rows). A failure fetching only closed issues is logged and the
// open issues are still reported, because the open gauge is the more valuable of the two.
func (c *Collector) fetchIssues(ctx context.Context, cluster, typ string, now time.Time) ([]Issue, error) {
	open, err := c.api.Issues(ctx, cluster, typ, []string{"open"}, now.Add(-maxIssueWindow), now)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	closed, err := c.api.Issues(ctx, cluster, typ, []string{"closed"}, now.Add(-c.closedWindow), now)
	if err != nil {
		slog.Warn("closed issues unavailable; open issues still reported", "cluster", cluster, "type", typ, "err", err)
		return open, nil
	}
	return append(open, closed...), nil
}

// IssueFilter selects which cluster and issue-type pairs are queried. The issues API can fail for a
// single pair (a server-side 500), so a known-bad pair can be skipped instead of failing every poll.
type IssueFilter struct {
	types []string
	skips []skip
}

type skip struct{ raw, cluster, typ string }

// NewIssueFilter takes issue types to select (empty means all) and skip entries of the form
// cluster/type, where either side may be "*".
func NewIssueFilter(types, skips []string) (IssueFilter, error) {
	f := IssueFilter{types: types}
	for _, t := range types {
		if !slices.Contains(IssueTypes, t) {
			return f, fmt.Errorf("unknown issue type %q, want one of %v", t, IssueTypes)
		}
	}
	for _, s := range skips {
		i := strings.LastIndex(s, "/")
		if i <= 0 || i == len(s)-1 {
			return f, fmt.Errorf("invalid skip-issues entry %q, want cluster/type (either may be *)", s)
		}
		sk := skip{raw: s, cluster: s[:i], typ: s[i+1:]}
		if sk.typ != "*" && !slices.Contains(IssueTypes, sk.typ) {
			return f, fmt.Errorf("invalid skip-issues entry %q: unknown issue type %q, want one of %v or *", s, sk.typ, IssueTypes)
		}
		f.skips = append(f.skips, sk)
	}
	return f, nil
}

func (s skip) matches(cluster, typ string) bool {
	return (s.cluster == "*" || s.cluster == cluster) && (s.typ == "*" || s.typ == typ)
}

func (f IssueFilter) Pairs(clusters []string) [][2]string {
	types := f.types
	if len(types) == 0 {
		types = IssueTypes
	}
	var out [][2]string
	for _, cl := range clusters {
		for _, t := range types {
			if !slices.ContainsFunc(f.skips, func(s skip) bool { return s.matches(cl, t) }) {
				out = append(out, [2]string{cl, t})
			}
		}
	}
	return out
}

// UnmatchedSkips returns entries naming a cluster that does not exist, which is almost always a typo.
func (f IssueFilter) UnmatchedSkips(clusters []string) []string {
	var out []string
	for _, s := range f.skips {
		if s.cluster != "*" && !slices.Contains(clusters, s.cluster) {
			out = append(out, s.raw)
		}
	}
	return out
}
