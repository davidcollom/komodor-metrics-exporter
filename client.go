package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/prometheus/client_golang/prometheus"
)

// Some endpoints take minutes, so buckets run well past the usual HTTP range.
var durationBuckets = []float64{.1, .25, .5, 1, 2.5, 5, 10, 20, 30, 60, 120, 300}

type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

func NewClient(baseURL, apiKey string, timeout time.Duration, reg prometheus.Registerer) *Client {
	hist := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "komodor_exporter_api_request_duration_seconds", Help: "Duration of each Komodor API attempt (retries counted separately).",
		Buckets: durationBuckets}, []string{"endpoint", "code"})
	reg.MustRegister(hist)
	rc := retryablehttp.NewClient()
	rc.RetryMax = 4
	rc.RetryWaitMin = 1 * time.Second
	rc.RetryWaitMax = 15 * time.Second
	rc.Logger = leveledLogger{}
	rc.HTTPClient.Timeout = timeout
	rc.HTTPClient.Transport = loggingTransport{http.DefaultTransport, hist}
	return &Client{baseURL: baseURL, apiKey: apiKey, http: rc.StandardClient()}
}

func (c *Client) do(ctx context.Context, method, path string, q url.Values, body, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	u := c.baseURL + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return err
	}
	req.Header.Set("X-API-KEY", c.apiKey)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, msg)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// leveledLogger routes retryablehttp's own logging (retry decisions) into slog.
type leveledLogger struct{}

// A failed attempt is retried, so it is only a warning here; the caller logs the final error.
func (leveledLogger) Error(msg string, kv ...any) { slog.Warn(msg, kv...) }
func (leveledLogger) Info(msg string, kv ...any)  { slog.Info(msg, kv...) }
func (leveledLogger) Debug(msg string, kv ...any) { slog.Debug(msg, kv...) }
func (leveledLogger) Warn(msg string, kv ...any)  { slog.Warn(msg, kv...) }

// loggingTransport logs every attempt at debug; headers are never logged, so the API key stays out.
type loggingTransport struct {
	next http.RoundTripper
	hist *prometheus.HistogramVec
}

func (t loggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := t.next.RoundTrip(req)
	elapsed := time.Since(start)
	attrs := []any{"method", req.Method, "url", req.URL.String(), "duration", elapsed.String()}
	code := "error"
	if err != nil {
		slog.Debug("api request failed", append(attrs, "err", err)...)
	} else {
		code = strconv.Itoa(resp.StatusCode)
		slog.Debug("api request", append(attrs, "status", resp.StatusCode)...)
	}
	t.hist.WithLabelValues(req.URL.Path, code).Observe(elapsed.Seconds())
	return resp, err
}

type Risk struct {
	CheckType   string `json:"checkType"`
	ClusterName string `json:"clusterName"`
	Severity    string `json:"severity"`
}

type riskPage struct {
	TotalResults   int    `json:"totalResults"`
	HasMoreResults bool   `json:"hasMoreResults"`
	Violations     []Risk `json:"violations"`
}

func riskQuery(status, severity string, pageSize, offset int) url.Values {
	q := url.Values{}
	q.Set("pageSize", strconv.Itoa(pageSize))
	q.Set("offset", strconv.Itoa(offset))
	q.Add("status", status)
	if severity != "" {
		q.Add("severity", severity)
	}
	// impactGroupType is required by the API; send all three to match everything.
	for _, t := range []string{"static", "dynamic", "realtime"} {
		q.Add("impactGroupType", t)
	}
	return q
}

// RiskCount uses totalResults from a one-row page so no rows are transferred.
func (c *Client) RiskCount(ctx context.Context, status, severity string) (int, error) {
	var p riskPage
	err := c.do(ctx, http.MethodGet, "/api/v2/health/risks", riskQuery(status, severity, 1, 0), nil, &p)
	return p.TotalResults, err
}

func (c *Client) Risks(ctx context.Context, status string) ([]Risk, error) {
	const size = 500
	var all []Risk
	for off := 0; ; off += size {
		var p riskPage
		if err := c.do(ctx, http.MethodGet, "/api/v2/health/risks", riskQuery(status, "", size, off), nil, &p); err != nil {
			return nil, err
		}
		all = append(all, p.Violations...)
		if !p.HasMoreResults || len(p.Violations) == 0 {
			return all, nil
		}
	}
}

func (c *Client) Clusters(ctx context.Context) ([]string, error) {
	var r struct {
		Data struct {
			Clusters []struct {
				Name string `json:"name"`
			} `json:"clusters"`
		} `json:"data"`
	}
	if err := c.do(ctx, http.MethodGet, "/api/v2/clusters", nil, nil, &r); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(r.Data.Clusters))
	for _, cl := range r.Data.Clusters {
		names = append(names, cl.Name)
	}
	return names, nil
}

type Issue struct {
	Type      string `json:"type"`
	StartTime int64  `json:"startTime"`
	EndTime   int64  `json:"endTime"`
	Summary   string `json:"summary"`
}

var IssueTypes = []string{"availability", "failed-deploy", "node-issue", "pvc-issue", "workflow-issue"}

func (c *Client) Issues(ctx context.Context, cluster, typ, status string, from, to time.Time) ([]Issue, error) {
	const size = 500
	var all []Issue
	for page := 0; ; page++ {
		body := map[string]any{
			"scope": map[string]any{"cluster": cluster},
			"props": map[string]any{
				"type": typ, "statuses": []string{status},
				"fromEpoch": from.Unix(), "toEpoch": to.Unix(),
			},
			"pagination": map[string]any{"pageSize": size, "page": page},
		}
		var r struct {
			Data struct {
				Issues []Issue `json:"issues"`
			} `json:"data"`
			Meta struct {
				NextPage int `json:"nextPage"`
			} `json:"meta"`
		}
		if err := c.do(ctx, http.MethodPost, "/api/v2/clusters/issues/search", nil, body, &r); err != nil {
			return nil, err
		}
		all = append(all, r.Data.Issues...)
		if len(r.Data.Issues) < size || r.Meta.NextPage <= page {
			return all, nil
		}
	}
}
