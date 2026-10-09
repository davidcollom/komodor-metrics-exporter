package komodor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func testClient(url string, concurrency, slow int, reg prometheus.Registerer) *Client {
	return NewClient(ClientOptions{BaseURL: url, APIKey: "k", Timeout: 10 * time.Second,
		Concurrency: concurrency, SlowConcurrency: slow, MaxRetries: 4}, reg)
}

// parallel runs fn for 0..n-1 concurrently and returns the first error.
func parallel(n int, fn func(i int) error) error {
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
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

func TestClientRetries(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls++; calls < 3 {
			http.Error(w, "busy", http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"data":{"clusters":[{"name":"c1"}]}}`))
	}))
	defer srv.Close()
	got, err := testClient(srv.URL, 1, 1, prometheus.NewRegistry()).Clusters(context.Background())
	if err != nil || len(got) != 1 || calls != 3 {
		t.Fatalf("got %v, err %v, calls %d", got, err, calls)
	}
}

func TestConcurrencyLimit(t *testing.T) {
	var inflight, peak atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := inflight.Add(1)
		defer inflight.Add(-1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(30 * time.Millisecond)
		_, _ = w.Write([]byte(`{"totalResults":1}`))
	}))
	defer srv.Close()
	c := testClient(srv.URL, 3, 1, prometheus.NewRegistry())
	if err := parallel(12, func(int) error { _, err := c.RiskCount(context.Background(), RiskFilter{Cluster: "c1"}); return err }); err != nil {
		t.Fatal(err)
	}
	if p := peak.Load(); p < 2 || p > 3 {
		t.Fatalf("peak in-flight = %d, want 2..3", p)
	}
}

func TestGatewayTimeoutIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "timeout", http.StatusGatewayTimeout)
	}))
	defer srv.Close()
	if _, err := testClient(srv.URL, 2, 1, prometheus.NewRegistry()).RiskCount(context.Background(), RiskFilter{}); err == nil {
		t.Fatal("want error")
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("calls = %d, want 1", n)
	}
}

func TestSlowQueriesUseTheirOwnPool(t *testing.T) {
	var slowNow, slowPeak, fastDone atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("clusterName") == "" {
			n := slowNow.Add(1)
			defer slowNow.Add(-1)
			for p := slowPeak.Load(); n > p && !slowPeak.CompareAndSwap(p, n); p = slowPeak.Load() {
			}
			time.Sleep(150 * time.Millisecond)
		} else {
			fastDone.Add(1)
		}
		_, _ = w.Write([]byte(`{"totalResults":1}`))
	}))
	defer srv.Close()
	c := testClient(srv.URL, 4, 1, prometheus.NewRegistry())
	err := parallel(8, func(i int) error {
		_, err := c.RiskCount(context.Background(), RiskFilter{Cluster: map[bool]string{true: "c1"}[i%2 == 0]})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if p := slowPeak.Load(); p != 1 {
		t.Fatalf("account-wide peak in-flight = %d, want 1", p)
	}
	if fastDone.Load() != 4 {
		t.Fatalf("fast calls = %d, want 4", fastDone.Load())
	}
}

func TestFailureKeepsResponseBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "node data unavailable", http.StatusInternalServerError)
	}))
	defer srv.Close()
	c := NewClient(ClientOptions{BaseURL: srv.URL, APIKey: "k", Timeout: time.Second, Concurrency: 1, SlowConcurrency: 1, MaxRetries: 1}, prometheus.NewRegistry())
	_, err := c.Clusters(context.Background())
	if err == nil || !strings.Contains(err.Error(), "node data unavailable") || !strings.Contains(err.Error(), "2 attempt") {
		t.Fatalf("err = %v", err)
	}
}

func TestBackoffIsExponentialWithJitter(t *testing.T) {
	for attempt, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second} {
		for range 50 {
			got := jitterBackoff(time.Second, 15*time.Second, attempt, nil)
			if got < want/2 || got > want {
				t.Fatalf("attempt %d: %v outside [%v, %v]", attempt, got, want/2, want)
			}
		}
	}
}

func noRetryClient(url string) *Client {
	return NewClient(ClientOptions{BaseURL: url, APIKey: "k", Timeout: 5 * time.Second, Concurrency: 2, SlowConcurrency: 1, MaxRetries: 0}, prometheus.NewRegistry())
}

func TestRiskCountSendsFilters(t *testing.T) {
	var got http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = *r.Clone(r.Context())
		_, _ = w.Write([]byte(`{"totalResults":42}`))
	}))
	defer srv.Close()
	n, err := noRetryClient(srv.URL).RiskCount(context.Background(), RiskFilter{
		Statuses: []string{"open", "confirmed"}, Severity: "high", Cluster: "c1", CheckType: "missingPDB"})
	if err != nil || n != 42 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	q := got.URL.Query()
	if got.URL.Path != "/api/v2/health/risks" || got.Header.Get("X-API-KEY") != "k" {
		t.Errorf("path %q key %q", got.URL.Path, got.Header.Get("X-API-KEY"))
	}
	for k, want := range map[string][]string{
		"status": {"open", "confirmed"}, "severity": {"high"}, "clusterName": {"c1"}, "checkType": {"missingPDB"},
		"pageSize": {"1"}, "offset": {"0"}, "impactGroupType": {"static", "dynamic", "realtime"},
	} {
		if fmt.Sprint(q[k]) != fmt.Sprint(want) {
			t.Errorf("%s = %v, want %v", k, q[k], want)
		}
	}
}

func TestRiskCountOmitsEmptyFilters(t *testing.T) {
	var q map[string][]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q = r.URL.Query()
		_, _ = w.Write([]byte(`{"totalResults":1}`))
	}))
	defer srv.Close()
	if _, err := noRetryClient(srv.URL).RiskCount(context.Background(), RiskFilter{}); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"status", "severity", "clusterName", "checkType"} {
		if _, ok := q[k]; ok {
			t.Errorf("%s sent although empty", k)
		}
	}
}

func TestClusters(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"clusters":[{"name":"a"},{"name":"b"}]}}`))
	}))
	defer srv.Close()
	got, err := noRetryClient(srv.URL).Clusters(context.Background())
	if err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("got %v err %v", got, err)
	}
}

func TestClustersSurfacesHTTPErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"Status":"Forbidden"}`, http.StatusForbidden)
	}))
	defer srv.Close()
	_, err := noRetryClient(srv.URL).Clusters(context.Background())
	if err == nil || !strings.Contains(err.Error(), "403") || !strings.Contains(err.Error(), "Forbidden") {
		t.Fatalf("err = %v", err)
	}
}

func TestInvalidJSONIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("not json")) }))
	defer srv.Close()
	if _, err := noRetryClient(srv.URL).Clusters(context.Background()); err == nil {
		t.Fatal("want a decode error")
	}
}

func TestIssuesPaginatesAndSendsTheQuery(t *testing.T) {
	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		bodies = append(bodies, b)
		page := int(b["pagination"].(map[string]any)["page"].(float64))
		var issues []Issue
		next := 0
		if page == 0 {
			issues, next = make([]Issue, 500), 1
		} else {
			issues = []Issue{{Summary: "tail"}, {Summary: "tail2"}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"issues": issues}, "meta": map[string]any{"nextPage": next}})
	}))
	defer srv.Close()
	from, to := time.Unix(1000, 0), time.Unix(2000, 0)
	got, err := noRetryClient(srv.URL).Issues(context.Background(), "c1", "node-issue", []string{"open"}, from, to)
	if err != nil || len(got) != 502 {
		t.Fatalf("len=%d err=%v", len(got), err)
	}
	if len(bodies) != 2 {
		t.Fatalf("requests = %d, want 2 pages", len(bodies))
	}
	b := bodies[0]
	props := b["props"].(map[string]any)
	if b["scope"].(map[string]any)["cluster"] != "c1" || props["type"] != "node-issue" ||
		props["fromEpoch"].(float64) != 1000 || props["toEpoch"].(float64) != 2000 || fmt.Sprint(props["statuses"]) != "[open]" {
		t.Errorf("request body = %v", b)
	}
}

func TestIssuesStopsOnShortPageAndSurfacesErrors(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		_, _ = w.Write([]byte(`{"data":{"issues":[{"summary":"x"}]},"meta":{"nextPage":5}}`))
	}))
	if got, err := noRetryClient(srv.URL).Issues(context.Background(), "c", "t", nil, time.Now(), time.Now()); err != nil || len(got) != 1 || calls != 1 {
		t.Fatalf("got %v err %v calls %d", got, err, calls)
	}
	srv.Close()
	if _, err := noRetryClient(srv.URL).Issues(context.Background(), "c", "t", nil, time.Now(), time.Now()); err == nil {
		t.Fatal("want an error from a closed server")
	}
}

func TestCancelledContextStopsQueuedRequests(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		_, _ = w.Write([]byte(`{"totalResults":1}`))
	}))
	defer srv.Close()
	defer close(release)
	c := NewClient(ClientOptions{BaseURL: srv.URL, APIKey: "k", Timeout: 5 * time.Second, Concurrency: 1, SlowConcurrency: 1}, prometheus.NewRegistry())
	go func() { _, _ = c.RiskCount(context.Background(), RiskFilter{Cluster: "c1"}) }() // takes the only slot
	time.Sleep(100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := c.RiskCount(ctx, RiskFilter{Cluster: "c1"}); err == nil {
		t.Fatal("want the queued request to give up when its context ends")
	}
}

func TestRetryAfterWinsOverTheBackoff(t *testing.T) {
	resp := &http.Response{Header: http.Header{"Retry-After": []string{"3"}}, StatusCode: 429}
	if got := jitterBackoff(time.Second, 15*time.Second, 0, resp); got != 3*time.Second {
		t.Errorf("backoff = %v, want 3s from Retry-After", got)
	}
}

func TestGiveUpWithoutAResponse(t *testing.T) {
	_, err := giveUp(nil, errors.New("connection refused"), 3)
	if err == nil || !strings.Contains(err.Error(), "3 attempt(s)") || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("err = %v", err)
	}
}

func TestRetryLoggerWritesToSlog(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	l := leveledLogger{}
	l.Debug("d-msg", "k", "v")
	l.Info("i-msg")
	l.Warn("w-msg")
	l.Error("e-msg")
	for _, want := range []string{"level=DEBUG", "d-msg", "i-msg", "w-msg", "e-msg"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("log output missing %q: %s", want, buf.String())
		}
	}
	if strings.Contains(buf.String(), "level=ERROR") {
		t.Error("a retried failure must not log at error level")
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return false }

var _ net.Error = timeoutErr{}

func TestIsTimeout(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"gateway timeout", &StatusError{Code: http.StatusGatewayTimeout}, true},
		{"forbidden", &StatusError{Code: http.StatusForbidden}, false},
		{"server error", &StatusError{Code: http.StatusInternalServerError}, false},
		{"deadline exceeded", fmt.Errorf("wrapped: %w", context.DeadlineExceeded), true},
		{"network timeout", fmt.Errorf("wrapped: %w", timeoutErr{}), true},
		{"other", errors.New("boom"), false},
		{"nil", nil, false},
	} {
		if got := IsTimeout(tc.err); got != tc.want {
			t.Errorf("%s: IsTimeout = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestGatewayTimeoutIsATypedTimeoutError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "<html>504</html>", http.StatusGatewayTimeout)
	}))
	defer srv.Close()
	_, err := noRetryClient(srv.URL).RiskCount(context.Background(), RiskFilter{})
	var se *StatusError
	if !errors.As(err, &se) || se.Code != 504 || !IsTimeout(err) || !strings.Contains(err.Error(), "504") {
		t.Fatalf("err = %v", err)
	}
}

func TestOnlyReadEndpointsAreAllowed(t *testing.T) {
	var hit atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hit.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c := noRetryClient(srv.URL)
	for _, tc := range []struct{ method, path string }{
		{http.MethodDelete, "/api/v2/users/someone"},
		{http.MethodPost, "/api/v2/rbac/roles"},
		{http.MethodPut, "/api/v2/health/risks/123"},
		{http.MethodPost, "/api/v2/users"},
		{http.MethodGet, "/api/v2/users"},
		{http.MethodGet, "/api/v2/audit-log"},
		{http.MethodPost, "/api/v2/clusters"},
		{http.MethodDelete, "/api/v2/clusters"},
		{http.MethodPut, "/api/v2/clusters/issues/search"},
	} {
		err := c.do(context.Background(), tc.method, tc.path, nil, nil, &struct{}{})
		if err == nil || !strings.Contains(err.Error(), "read-only") {
			t.Errorf("%s %s was not refused: %v", tc.method, tc.path, err)
		}
	}
	if hit.Load() != 0 {
		t.Fatalf("%d refused requests still reached the server", hit.Load())
	}
}

func TestTheThreeReadEndpointsStillWork(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.Path)
		_, _ = w.Write([]byte(`{"totalResults":1,"data":{"clusters":[],"issues":[]}}`))
	}))
	defer srv.Close()
	c := noRetryClient(srv.URL)
	ctx := context.Background()
	if _, err := c.Clusters(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RiskCount(ctx, RiskFilter{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Issues(ctx, "c", "node-issue", []string{"open"}, time.Now(), time.Now()); err != nil {
		t.Fatal(err)
	}
	want := []string{"GET /api/v2/clusters", "GET /api/v2/health/risks", "POST /api/v2/clusters/issues/search"}
	if strings.Join(paths, ",") != strings.Join(want, ",") {
		t.Fatalf("requests = %v, want %v", paths, want)
	}
}
