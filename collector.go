package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var riskStatuses = []string{"open", "confirmed", "resolved", "dismissed", "ignored", "manually_resolved"}
var riskSeverities = []string{"high", "medium", "low"}

// The issues API caps a query at a 2 day window, 7 days back.
const issueWindow = 48 * time.Hour

type Collector struct {
	api      *Client
	interval time.Duration

	clusters     prometheus.Gauge
	risks        *prometheus.GaugeVec
	risksActive  *prometheus.GaugeVec
	issuesOpen   *prometheus.GaugeVec
	issuesClosed *prometheus.CounterVec
	stepDuration *prometheus.HistogramVec
	scrapeErrors prometheus.Counter
	lastSuccess  prometheus.Gauge

	// The API gives issues no ID, so closed issues are deduped on this key.
	seenClosed  map[string]time.Time
	firstIssues bool
}

func NewCollector(api *Client, interval time.Duration, reg prometheus.Registerer) *Collector {
	f := func(name, help string, labels ...string) *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "komodor_" + name, Help: help}, labels)
	}
	c := &Collector{
		api: api, interval: interval, seenClosed: map[string]time.Time{}, firstIssues: true,
		clusters:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "komodor_clusters", Help: "Clusters connected to Komodor."}),
		risks:       f("reliability_risks", "Reliability risks by status and severity.", "status", "severity"),
		risksActive: f("reliability_risks_active", "Open and confirmed reliability risks by cluster, check and severity.", "cluster", "check_type", "severity"),
		issuesOpen:  f("issues_open", "Open issues by cluster and type (issues older than 2 days are not seen).", "cluster", "type"),
		issuesClosed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "komodor_issues_closed_total", Help: "Issues observed closing since the exporter started."}, []string{"cluster", "type"}),
		stepDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "komodor_exporter_collection_step_duration_seconds", Help: "Duration of each collection step.",
			Buckets: durationBuckets}, []string{"step"}),
		scrapeErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "komodor_exporter_errors_total", Help: "Failed collection attempts."}),
		lastSuccess:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "komodor_exporter_last_success_timestamp_seconds", Help: "Unix time of the last fully successful collection."}),
	}
	reg.MustRegister(c.clusters, c.risks, c.risksActive, c.issuesOpen, c.issuesClosed, c.stepDuration, c.scrapeErrors, c.lastSuccess)
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

// collect fills fresh gauges and only swaps them in per family once that family fully succeeds,
// so a failed poll keeps the previous values instead of reporting zeros.
func (c *Collector) collect(ctx context.Context) error {
	stepStart := time.Now()
	clusters, err := c.api.Clusters(ctx)
	c.stepDuration.WithLabelValues("clusters").Observe(time.Since(stepStart).Seconds())
	if err != nil {
		return fmt.Errorf("clusters: %w", err)
	}
	c.clusters.Set(float64(len(clusters)))
	slog.Debug("collected clusters", "count", len(clusters))

	var firstErr error
	step := func(name string, fn func(context.Context) error) {
		start := time.Now()
		err := fn(ctx)
		c.stepDuration.WithLabelValues(name).Observe(time.Since(start).Seconds())
		slog.Debug("collection step done", "step", name, "duration", time.Since(start).String())
		if err != nil {
			slog.Error("collect step failed", "step", name, "err", err)
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: %w", name, err)
			}
		}
	}
	step("risk_counts", c.collectRiskCounts)
	step("active_risks", c.collectActiveRisks)
	step("issues", func(ctx context.Context) error { return c.collectIssues(ctx, clusters) })
	return firstErr
}

func (c *Collector) collectRiskCounts(ctx context.Context) error {
	type kv struct {
		s, sev string
		n      int
	}
	var res []kv
	for _, s := range riskStatuses {
		for _, sev := range riskSeverities {
			n, err := c.api.RiskCount(ctx, s, sev)
			if err != nil {
				return err
			}
			res = append(res, kv{s, sev, n})
		}
	}
	for _, r := range res {
		c.risks.WithLabelValues(r.s, r.sev).Set(float64(r.n))
	}
	return nil
}

func (c *Collector) collectActiveRisks(ctx context.Context) error {
	type key struct{ cluster, check, sev string }
	counts := map[key]float64{}
	for _, s := range []string{"open", "confirmed"} {
		rs, err := c.api.Risks(ctx, s)
		if err != nil {
			return err
		}
		for _, r := range rs {
			counts[key{r.ClusterName, r.CheckType, r.Severity}]++
		}
	}
	slog.Debug("collected active risks", "series", len(counts))
	c.risksActive.Reset()
	for k, n := range counts {
		c.risksActive.WithLabelValues(k.cluster, k.check, k.sev).Set(n)
	}
	return nil
}

func (c *Collector) collectIssues(ctx context.Context, clusters []string) error {
	now := time.Now()
	from := now.Add(-issueWindow)
	open := map[[2]string]float64{}
	var firstErr error
	for _, cl := range clusters {
		for _, typ := range IssueTypes {
			open[[2]string{cl, typ}] = 0
			is, err := c.api.Issues(ctx, cl, typ, "open", from, now)
			if err != nil {
				firstErr = fmt.Errorf("open issues %s/%s: %w", cl, typ, err)
				continue
			}
			open[[2]string{cl, typ}] = float64(len(is))

			closed, err := c.api.Issues(ctx, cl, typ, "closed", from, now)
			if err != nil {
				firstErr = fmt.Errorf("closed issues %s/%s: %w", cl, typ, err)
				continue
			}
			slog.Debug("collected issues", "cluster", cl, "type", typ, "open", len(is), "closed", len(closed))
			for _, i := range closed {
				k := fmt.Sprintf("%s|%s|%d|%s", cl, typ, i.StartTime, i.Summary)
				if _, dup := c.seenClosed[k]; dup {
					continue
				}
				c.seenClosed[k] = now
				if !c.firstIssues {
					c.issuesClosed.WithLabelValues(cl, typ).Inc()
				}
			}
		}
	}
	for k := range c.seenClosed {
		if c.seenClosed[k].Before(now.Add(-issueWindow - time.Hour)) {
			delete(c.seenClosed, k)
		}
	}
	if firstErr == nil {
		c.firstIssues = false
	}
	c.issuesOpen.Reset()
	for k, n := range open {
		c.issuesOpen.WithLabelValues(k[0], k[1]).Set(n)
	}
	return firstErr
}
