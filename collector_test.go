package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
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
				Props struct{ Type string }
			}
			_ = json.NewDecoder(r.Body).Decode(&b)
			is := []Issue{}
			if b.Scope.Cluster == "c1" && b.Props.Type == "availability" {
				is = append([]Issue{{Type: "availability", Status: "open", StartTime: 1, Summary: "a"}}, *closed...)
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
	c := NewCollector(NewClient(srv.URL, "k", 10*time.Second, 4, reg), time.Minute, allEnabled(t), reg)
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
	got, err := NewClient(srv.URL, "k", 10*time.Second, 1, prometheus.NewRegistry()).Clusters(context.Background())
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
	c := NewClient(srv.URL, "k", 10*time.Second, 3, prometheus.NewRegistry())
	if err := fanout(12, func(int) error { _, err := c.RiskCount(context.Background(), RiskFilter{}); return err }); err != nil {
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
	c := NewCollector(NewClient(srv.URL, "k", 10*time.Second, 4, reg), time.Minute, enabled, reg)
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
