package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func fakeAPI(t *testing.T, closed *[]Issue) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-KEY") != "k" {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/api/v2/clusters":
			_, _ = w.Write([]byte(`{"data":{"clusters":[{"name":"c1"},{"name":"c2"}]}}`))
		case "/api/v2/health/risks":
			sev := r.URL.Query().Get("severity")
			if sev == "" { // paged listing
				_, _ = w.Write([]byte(`{"totalResults":2,"hasMoreResults":false,"violations":[{"checkType":"missingPDB","clusterName":"c1","severity":"high"},{"checkType":"missingPDB","clusterName":"c1","severity":"high"}]}`))
				return
			}
			n := 0
			if sev == "high" && r.URL.Query().Get("status") == "open" {
				n = 7
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"totalResults": n, "violations": []any{}})
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
					is = []Issue{{Type: "availability", StartTime: 1, Summary: "a"}}
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
	closed := []Issue{{Type: "availability", StartTime: 5, Summary: "old"}}
	srv := fakeAPI(t, &closed)
	defer srv.Close()
	c := NewCollector(NewClient(srv.URL, "k"), time.Minute, prometheus.NewRegistry())
	ctx := context.Background()

	if err := c.collect(ctx); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(c.clusters); got != 2 {
		t.Errorf("clusters = %v", got)
	}
	if got := testutil.ToFloat64(c.risks.WithLabelValues("open", "high")); got != 7 {
		t.Errorf("risks open/high = %v", got)
	}
	// 2 rows from "open" + 2 from "confirmed", same key.
	if got := testutil.ToFloat64(c.risksActive.WithLabelValues("c1", "missingPDB", "high")); got != 4 {
		t.Errorf("active risks = %v", got)
	}
	if got := testutil.ToFloat64(c.issuesOpen.WithLabelValues("c1", "availability")); got != 1 {
		t.Errorf("open issues = %v", got)
	}
	// Pre-existing closed issues seed the dedupe set without counting.
	if got := testutil.ToFloat64(c.issuesClosed.WithLabelValues("c1", "availability")); got != 0 {
		t.Errorf("closed after first poll = %v", got)
	}

	closed = append(closed, Issue{Type: "availability", StartTime: 9, Summary: "new"})
	if err := c.collect(ctx); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(c.issuesClosed.WithLabelValues("c1", "availability")); got != 1 {
		t.Errorf("closed after second poll = %v, want 1 (no double count)", got)
	}
	if err := c.collect(ctx); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(c.issuesClosed.WithLabelValues("c1", "availability")); got != 1 {
		t.Errorf("closed after third poll = %v, want 1", got)
	}
}
