package collector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/davidcollom/komodor-metrics-exporter/internal/komodor"
)

func testClient(url string, concurrency, slow int, reg prometheus.Registerer) *komodor.Client {
	return komodor.NewClient(komodor.ClientOptions{BaseURL: url, APIKey: "k", Timeout: 10 * time.Second,
		Concurrency: concurrency, SlowConcurrency: slow, MaxRetries: 4}, reg)
}

func fakeAPI(closed *[]komodor.Issue) *httptest.Server {
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
			is := []komodor.Issue{}
			if b.Scope.Cluster == "c1" && b.Props.Type == "availability" {
				if b.Props.Statuses[0] == "open" {
					is = []komodor.Issue{{Type: "availability", Status: "open", StartTime: 1, Summary: "a"}}
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
	closed := []komodor.Issue{{Type: "availability", Status: "closed", StartTime: 5, Summary: "old"}}
	srv := fakeAPI(&closed)
	defer srv.Close()
	reg := prometheus.NewRegistry()
	c := New(testClient(srv.URL, 4, 2, reg), time.Minute, time.Hour, allEnabled(t), IssueFilter{}, reg)
	ctx := context.Background()

	if err := c.collect(ctx); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"komodor_exporter_api_request_duration_seconds", "komodor_exporter_collection_step_duration_seconds"} {
		if n, err := testutil.GatherAndCount(reg, name); err != nil || n == 0 {
			t.Errorf("%s: count %d, err %v", name, n, err)
		}
	}
	// Every cluster/type pair exposes a closed counter at 0 from the first poll (2 clusters x 5 types).
	if n := testutil.CollectAndCount(c.issuesClosed); n != 10 {
		t.Errorf("closed counter series = %d, want 10", n)
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

	closed = append(closed, komodor.Issue{Type: "availability", Status: "closed", StartTime: 9, Summary: "new"})
	for range 2 {
		if err := c.collect(ctx); err != nil {
			t.Fatal(err)
		}
		check("closed after new issue", testutil.ToFloat64(c.issuesClosed.WithLabelValues("c1", "availability")), 1)
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
	c := New(testClient(srv.URL, 4, 2, reg), time.Minute, time.Hour, enabled, IssueFilter{}, reg)
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

func TestClosedIssuesFailureStillReportsOpen(t *testing.T) {
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
	api := komodor.NewClient(komodor.ClientOptions{BaseURL: srv.URL, APIKey: "k", Timeout: time.Second, Concurrency: 4, SlowConcurrency: 1}, reg)
	c := New(api, time.Minute, time.Hour, enabled, IssueFilter{}, reg)
	if err := c.collect(context.Background()); err != nil {
		t.Fatalf("closed-side failure should not fail the poll: %v", err)
	}
	if got := testutil.ToFloat64(c.issuesOpen.WithLabelValues("c1", "node-issue")); got != 1 {
		t.Fatalf("open node-issue = %v, want 1", got)
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
	c := New(testClient(srv.URL, 4, 2, reg), time.Minute, time.Hour, enabled, IssueFilter{}, reg)
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
	c := New(testClient(srv.URL, 4, 2, reg), time.Minute, time.Hour, enabled, f, reg)
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

func TestPollRecordsFreshnessEvenWhenItFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "down", http.StatusForbidden)
	}))
	defer srv.Close()
	reg := prometheus.NewRegistry()
	c := New(testClient(srv.URL, 2, 1, reg), time.Minute, time.Hour, allEnabled(t), IssueFilter{}, reg)
	c.poll(context.Background())
	if got := testutil.ToFloat64(c.lastPoll); time.Since(time.Unix(int64(got), 0)) > time.Minute || got == 0 {
		t.Errorf("last_poll = %v, want a recent timestamp", got)
	}
	if got := testutil.ToFloat64(c.pollOK); got != 0 {
		t.Errorf("last_poll_success = %v, want 0", got)
	}
	if got := testutil.ToFloat64(c.lastSuccess); got != 0 {
		t.Errorf("last_success = %v, want 0 after only failures", got)
	}
}
