package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func fakeAPI(closed *[]Issue) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-KEY") != "k" {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/api/v2/clusters":
			_, _ = w.Write([]byte(`{"data":{"clusters":[{"name":"c1"},{"name":"c2"}]}}`))
		case "/api/v2/health/risks":
			q := r.URL.Query()
			n := 0
			switch {
			case q.Get("checkType") == "missingPDB":
				n = 5
			case q.Get("clusterName") == "c1" && q.Get("severity") == "high":
				n = 3
			case q.Get("clusterName") == "" && q.Get("severity") == "high" && slices.Equal(q["status"], []string{"open"}):
				n = 7
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"totalResults": n})
		case "/api/v2/clusters/issues/search":
			var b struct {
				Scope struct{ Cluster string }
				Props struct {
					Type     string
					Statuses []string
				}
			}
			_ = json.NewDecoder(r.Body).Decode(&b)
			is := []Issue{}
			if b.Scope.Cluster == "c1" && b.Props.Type == "availability" {
				if b.Props.Statuses[0] == "open" {
					is = []Issue{{Type: "availability", Status: "open", StartTime: 1, Summary: "a"}}
				} else {
					is = *closed
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"issues": is}})
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestCollect(t *testing.T) {
	closed := []Issue{{Type: "availability", Status: "closed", StartTime: 5, Summary: "old"}}
	srv := fakeAPI(&closed)
	defer srv.Close()
	reg := prometheus.NewRegistry()
	c := NewCollector(testClient(srv.URL, 4, 2, reg), time.Minute, time.Hour, allEnabled(t), IssueFilter{}, reg)
	ctx := context.Background()

	if err := c.collect(ctx); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"komodor_exporter_api_request_duration_seconds", "komodor_exporter_collection_step_duration_seconds"} {
		if n, err := testutil.GatherAndCount(reg, name); err != nil || n == 0 {
			t.Errorf("%s: count %d, err %v", name, n, err)
		}
	}
	check := func(name string, got, want float64) {
		t.Helper()
		if got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	check("clusters", testutil.ToFloat64(c.clusters), 2)
	check("risks open/high", testutil.ToFloat64(c.risks.WithLabelValues("open", "high")), 7)
	check("active c1/high", testutil.ToFloat64(c.risksActive.WithLabelValues("c1", "high")), 3)
	check("by check", testutil.ToFloat64(c.risksByCheck.WithLabelValues("missingPDB")), 5)
	check("open issues", testutil.ToFloat64(c.issuesOpen.WithLabelValues("c1", "availability")), 1)
	check("closed after first poll", testutil.ToFloat64(c.issuesClosed.WithLabelValues("c1", "availability")), 0)

	closed = append(closed, Issue{Type: "availability", Status: "closed", StartTime: 9, Summary: "new"})
	for range 2 {
		if err := c.collect(ctx); err != nil {
			t.Fatal(err)
		}
		check("closed after new issue", testutil.ToFloat64(c.issuesClosed.WithLabelValues("c1", "availability")), 1)
	}
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
	if err := fanout(12, func(int) error { _, err := c.RiskCount(context.Background(), RiskFilter{Cluster: "c1"}); return err }); err != nil {
		t.Fatal(err)
	}
	if p := peak.Load(); p < 2 || p > 3 {
		t.Fatalf("peak in-flight = %d, want 2..3", p)
	}
}

func allEnabled(t *testing.T) map[string]bool {
	t.Helper()
	e, err := ParseEnabled(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestParseEnabled(t *testing.T) {
	e, err := ParseEnabled(map[string]bool{"issues": false}, []string{"risks"})
	if err != nil || e["issues"] || e["risks"] || !e["clusters"] || !e["risks_active"] || !e["risks_by_check"] {
		t.Fatalf("got %v, err %v", e, err)
	}
	if _, err := ParseEnabled(map[string]bool{"nope": true}, nil); err == nil {
		t.Fatal("unknown config group accepted")
	}
	if _, err := ParseEnabled(nil, []string{"nope"}); err == nil {
		t.Fatal("unknown disabled group accepted")
	}
}

func TestDisabledGroupsAreNotCalledOrExposed(t *testing.T) {
	var paths sync.Map
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths.Store(r.URL.Path, true)
		_, _ = w.Write([]byte(`{"totalResults":1,"data":{"clusters":[{"name":"c1"}],"issues":[]}}`))
	}))
	defer srv.Close()
	reg := prometheus.NewRegistry()
	enabled, _ := ParseEnabled(nil, []string{"clusters", "risks_active", "issues"})
	c := NewCollector(testClient(srv.URL, 4, 2, reg), time.Minute, time.Hour, enabled, IssueFilter{}, reg)
	if err := c.collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"/api/v2/clusters", "/api/v2/clusters/issues/search"} {
		if _, called := paths.Load(p); called {
			t.Errorf("%s called although its groups are disabled", p)
		}
	}
	if _, called := paths.Load("/api/v2/health/risks"); !called {
		t.Error("risks endpoint not called")
	}
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		if n := mf.GetName(); n == "komodor_clusters" || n == "komodor_issues_open" || n == "komodor_reliability_risks_active" {
			t.Errorf("%s exposed although disabled", n)
		}
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
	err := fanout(8, func(i int) error {
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

func testClient(url string, concurrency, slow int, reg prometheus.Registerer) *Client {
	return NewClient(ClientOptions{BaseURL: url, APIKey: "k", Timeout: 10 * time.Second,
		Concurrency: concurrency, SlowConcurrency: slow, MaxRetries: 4}, reg)
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

func TestIssuesFallBackPerStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/clusters" {
			_, _ = w.Write([]byte(`{"data":{"clusters":[{"name":"c1"}]}}`))
			return
		}
		var b struct{ Props struct{ Statuses []string } }
		_ = json.NewDecoder(r.Body).Decode(&b)
		if b.Props.Statuses[0] == "closed" {
			http.Error(w, `{"Error":"Something went wrong"}`, http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"data":{"issues":[{"type":"node-issue","status":"open","startTime":1,"summary":"x"}]}}`))
	}))
	defer srv.Close()
	reg := prometheus.NewRegistry()
	enabled, _ := ParseEnabled(nil, []string{"risks", "risks_active", "risks_by_check"})
	api := NewClient(ClientOptions{BaseURL: srv.URL, APIKey: "k", Timeout: time.Second, Concurrency: 4, SlowConcurrency: 1}, reg)
	c := NewCollector(api, time.Minute, time.Hour, enabled, IssueFilter{}, reg)
	if err := c.collect(context.Background()); err != nil {
		t.Fatalf("closed-side failure should not fail the poll: %v", err)
	}
	if got := testutil.ToFloat64(c.issuesOpen.WithLabelValues("c1", "node-issue")); got != 1 {
		t.Fatalf("open node-issue = %v, want 1", got)
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

func TestIssueWindows(t *testing.T) {
	from := map[string]int64{}
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/clusters" {
			_, _ = w.Write([]byte(`{"data":{"clusters":[{"name":"c1"}]}}`))
			return
		}
		var b struct {
			Props struct {
				Statuses  []string
				FromEpoch int64
			}
		}
		_ = json.NewDecoder(r.Body).Decode(&b)
		mu.Lock()
		from[b.Props.Statuses[0]] = b.Props.FromEpoch
		mu.Unlock()
		_, _ = w.Write([]byte(`{"data":{"issues":[]}}`))
	}))
	defer srv.Close()
	reg := prometheus.NewRegistry()
	enabled, _ := ParseEnabled(nil, []string{"risks", "risks_active", "risks_by_check"})
	c := NewCollector(testClient(srv.URL, 4, 2, reg), time.Minute, time.Hour, enabled, IssueFilter{}, reg)
	if err := c.collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	if d := now - from["closed"]; d < 3590 || d > 3610 {
		t.Errorf("closed window = %ds, want ~1h", d)
	}
	if d := now - from["open"]; d < 48*3600-10 || d > 48*3600+10 {
		t.Errorf("open window = %ds, want ~48h", d)
	}
}

func TestIssueFilter(t *testing.T) {
	f, err := NewIssueFilter(nil, []string{"cluster-a/node-issue", "*/pvc-issue", "legacy/*"})
	if err != nil {
		t.Fatal(err)
	}
	has := func(ps [][2]string, cl, typ string) bool { return slices.Contains(ps, [2]string{cl, typ}) }
	ps := f.Pairs([]string{"cluster-a", "prod", "legacy"})
	for cl, typ := range map[string]string{"cluster-a": "node-issue", "prod": "pvc-issue", "legacy": "availability"} {
		if has(ps, cl, typ) {
			t.Errorf("%s/%s should be skipped", cl, typ)
		}
	}
	if !has(ps, "cluster-a", "availability") || !has(ps, "prod", "node-issue") || len(ps) != 7 {
		t.Errorf("unexpected pairs: %v", ps)
	}
	if got := f.UnmatchedSkips([]string{"prod"}); !slices.Equal(got, []string{"cluster-a/node-issue", "legacy/*"}) {
		t.Errorf("unmatched = %v", got)
	}

	sel, _ := NewIssueFilter([]string{"availability"}, nil)
	if got := sel.Pairs([]string{"a", "b"}); len(got) != 2 {
		t.Errorf("select pairs = %v", got)
	}
	for _, bad := range [][]string{{"nope"}, {"cluster-a"}, {"/node-issue"}, {"cluster-a/bogus"}} {
		if _, err := NewIssueFilter(nil, bad); err == nil {
			t.Errorf("accepted %v", bad)
		}
	}
	if _, err := NewIssueFilter([]string{"bogus"}, nil); err == nil {
		t.Error("accepted unknown type")
	}
}

func TestSkippedPairsAreNotQueried(t *testing.T) {
	var queried sync.Map
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/clusters" {
			_, _ = w.Write([]byte(`{"data":{"clusters":[{"name":"cluster-a"}]}}`))
			return
		}
		var b struct {
			Props struct{ Type string }
		}
		_ = json.NewDecoder(r.Body).Decode(&b)
		queried.Store(b.Props.Type, true)
		if b.Props.Type == "node-issue" {
			http.Error(w, "broken", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"data":{"issues":[]}}`))
	}))
	defer srv.Close()
	reg := prometheus.NewRegistry()
	enabled, _ := ParseEnabled(nil, []string{"risks", "risks_active", "risks_by_check"})
	f, _ := NewIssueFilter(nil, []string{"cluster-a/node-issue"})
	c := NewCollector(testClient(srv.URL, 4, 2, reg), time.Minute, time.Hour, enabled, f, reg)
	if err := c.collect(context.Background()); err != nil {
		t.Fatalf("poll should succeed with the broken pair skipped: %v", err)
	}
	if _, called := queried.Load("node-issue"); called {
		t.Error("skipped pair was queried")
	}
	if _, called := queried.Load("availability"); !called {
		t.Error("other pairs were not queried")
	}
}
