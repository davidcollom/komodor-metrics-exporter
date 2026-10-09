package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
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

func TestBareIssueTypeSkipsItOnEveryCluster(t *testing.T) {
	f, err := NewIssueFilter(nil, []string{"node-issue"})
	if err != nil {
		t.Fatal(err)
	}
	ps := f.Pairs([]string{"a", "b"})
	if len(ps) != 8 {
		t.Fatalf("pairs = %v, want 4 types x 2 clusters", ps)
	}
	for _, p := range ps {
		if p[1] == "node-issue" {
			t.Errorf("node-issue not skipped on %s", p[0])
		}
	}
	if len(f.UnmatchedSkips([]string{"a"})) != 0 {
		t.Error("a bare type names no cluster, so it can never be unmatched")
	}
	if _, err := NewIssueFilter(nil, []string{"not-a-type"}); err == nil {
		t.Error("a bare word that is not an issue type should still be rejected")
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

func TestRunPollsRepeatedlyUntilCancelled(t *testing.T) {
	var polls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/clusters" {
			polls.Add(1)
		}
		_, _ = w.Write([]byte(`{"totalResults":1,"data":{"clusters":[{"name":"c1"}],"issues":[]}}`))
	}))
	defer srv.Close()
	reg := prometheus.NewRegistry()
	c := New(testClient(srv.URL, 4, 2, reg), 30*time.Millisecond, time.Hour, allEnabled(t), IssueFilter{}, reg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for (polls.Load() < 3 || testutil.ToFloat64(c.pollOK) != 1) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	// Checked before cancelling: a poll interrupted by the cancel is correctly counted as failed.
	ok, lastSuccess := testutil.ToFloat64(c.pollOK), testutil.ToFloat64(c.lastSuccess)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}
	if polls.Load() < 3 {
		t.Fatalf("only %d polls ran", polls.Load())
	}
	if ok != 1 || lastSuccess == 0 {
		t.Error("successful polls should set last_poll_success and last_success")
	}
}

func TestFailedQueryKeepsItsPreviousValueAndPublishesTheRest(t *testing.T) {
	var count atomic.Int32
	count.Store(7)
	var failResolved atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/clusters" {
			_, _ = w.Write([]byte(`{"data":{"clusters":[]}}`))
			return
		}
		if failResolved.Load() && slices.Equal(r.URL.Query()["status"], []string{"resolved"}) {
			http.Error(w, "nope", http.StatusForbidden) // a 4xx is not retried, keeping the test fast
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"totalResults": count.Load()})
	}))
	defer srv.Close()
	reg := prometheus.NewRegistry()
	enabled, _ := ParseEnabled(nil, []string{"clusters", "risks_active", "risks_by_check", "issues"})
	c := New(testClient(srv.URL, 4, 2, reg), time.Minute, time.Hour, enabled, IssueFilter{}, reg)

	if err := c.collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	count.Store(9)
	failResolved.Store(true)
	if err := c.collect(context.Background()); err == nil {
		t.Fatal("the failed query should fail the poll")
	}
	if got := testutil.ToFloat64(c.risks.WithLabelValues("open", "high")); got != 9 {
		t.Errorf("open/high = %v, want the new value 9", got)
	}
	if got := testutil.ToFloat64(c.risks.WithLabelValues("resolved", "high")); got != 7 {
		t.Errorf("resolved/high = %v, want the previous value 7 kept", got)
	}
	c.poll(context.Background())
	if testutil.ToFloat64(c.pollOK) != 0 || testutil.ToFloat64(c.scrapeErrors) != 1 {
		t.Error("a failing poll should set last_poll_success=0 and count an error")
	}
}

func TestClustersFailureFailsThePollButKeepsTheGauge(t *testing.T) {
	var fail atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			http.Error(w, "nope", http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte(`{"data":{"clusters":[{"name":"a"},{"name":"b"}]}}`))
	}))
	defer srv.Close()
	reg := prometheus.NewRegistry()
	enabled, _ := ParseEnabled(nil, []string{"risks", "risks_by_check", "risks_active", "issues"})
	c := New(testClient(srv.URL, 4, 2, reg), time.Minute, time.Hour, enabled, IssueFilter{}, reg)
	if err := c.collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	if err := c.collect(context.Background()); err == nil {
		t.Fatal("want an error")
	}
	if got := testutil.ToFloat64(c.clusters); got != 2 {
		t.Errorf("clusters = %v, want the previous 2 kept", got)
	}
}

func TestAllGroupsDisabledMakesNoAPICalls(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1) }))
	defer srv.Close()
	reg := prometheus.NewRegistry()
	enabled, _ := ParseEnabled(nil, MetricNames)
	c := New(testClient(srv.URL, 4, 2, reg), time.Minute, time.Hour, enabled, IssueFilter{}, reg)
	c.poll(context.Background())
	if calls.Load() != 0 {
		t.Errorf("%d API calls with every group disabled", calls.Load())
	}
	if testutil.ToFloat64(c.pollOK) != 1 {
		t.Error("an empty poll is a successful poll")
	}
}

func TestOldClosedIssuesAreForgotten(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/clusters" {
			_, _ = w.Write([]byte(`{"data":{"clusters":["c1"]}}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"issues":[]}}`))
	}))
	defer srv.Close()
	reg := prometheus.NewRegistry()
	enabled, _ := ParseEnabled(nil, []string{"risks", "risks_by_check", "risks_active", "clusters"})
	c := New(testClient(srv.URL, 4, 2, reg), time.Minute, time.Hour, enabled, IssueFilter{}, reg)
	c.seenClosed["stale"] = time.Now().Add(-3 * time.Hour)
	c.seenClosed["fresh"] = time.Now()
	if err := c.collectIssues(context.Background(), []string{"c1"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.seenClosed["stale"]; ok {
		t.Error("a closed issue older than the window should be pruned")
	}
	if _, ok := c.seenClosed["fresh"]; !ok {
		t.Error("a recent closed issue must be kept for de-duplication")
	}
}

func TestUnmatchedSkipsAreWarnedOnceNotFatal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"issues":[]}}`))
	}))
	defer srv.Close()
	reg := prometheus.NewRegistry()
	enabled, _ := ParseEnabled(nil, []string{"risks", "risks_by_check", "risks_active", "clusters"})
	f, _ := NewIssueFilter(nil, []string{"typo/node-issue"})
	c := New(testClient(srv.URL, 4, 2, reg), time.Minute, time.Hour, enabled, f, reg)
	if err := c.collectIssues(context.Background(), []string{"real"}); err != nil {
		t.Fatal(err)
	}
	if !c.warnedSkips {
		t.Error("the unmatched skip entry should have been checked")
	}
}

func TestPollTimeoutSaysToRaiseThePollInterval(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(400 * time.Millisecond)
		_, _ = w.Write([]byte(`{"data":{"clusters":[]}}`))
	}))
	defer srv.Close()
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(old)

	reg := prometheus.NewRegistry()
	c := New(testClient(srv.URL, 2, 1, reg), 50*time.Millisecond, time.Hour, allEnabled(t), IssueFilter{}, reg)
	c.poll(context.Background())
	if !strings.Contains(buf.String(), "raise --poll-interval") {
		t.Errorf("a poll cut off by its interval should say how to fix it; log: %s", buf.String())
	}
}
