//go:build live

package komodor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// These tests run against a real Komodor account: go test -tags live -run Live ./internal/komodor/
// with KOMODOR_API_KEY set. The client only ever makes read-only requests (see readOnlyEndpoints).
//
// Output goes to public CI logs, so it is limited to numbers and HTTP status codes: never a cluster
// name, issue text, link or response body. Clusters are referred to by their position only.
func say(format string, args ...any) { fmt.Printf("live: "+format+"\n", args...) }

func liveClient(t *testing.T) *Client {
	t.Helper()
	key := os.Getenv("KOMODOR_API_KEY")
	if key == "" {
		t.Skip("KOMODOR_API_KEY is not set")
	}
	url := os.Getenv("KOMODOR_API_URL")
	if url == "" {
		url = "https://api.komodor.com"
	}
	return NewClient(ClientOptions{BaseURL: url, APIKey: key, Timeout: 2 * time.Minute,
		Concurrency: 4, SlowConcurrency: 1, MaxRetries: 2}, prometheus.NewRegistry())
}

// status reduces an error to something safe to print.
func status(err error) string {
	var se *StatusError
	switch {
	case err == nil:
		return "ok"
	case errors.As(err, &se):
		return fmt.Sprintf("http %d", se.Code)
	case IsTimeout(err):
		return "timeout"
	default:
		return "error"
	}
}

func near(a, b int) bool { // the data moves while the test runs, so allow a little drift
	d := math.Abs(float64(a - b))
	return d <= 3 || d <= 0.02*math.Max(float64(a), float64(b))
}

func sample[T any](all []T, n int) []T { return all[:min(n, len(all))] }

func clusters(t *testing.T, c *Client) []string {
	t.Helper()
	cl, err := c.Clusters(context.Background())
	if err != nil {
		t.Fatalf("listing clusters: %s", status(err))
	}
	if len(cl) == 0 {
		t.Fatal("the account has no clusters")
	}
	say("clusters=%d", len(cl))
	return cl
}

func TestLiveRiskFiltersAreHonoured(t *testing.T) {
	c := liveClient(t)
	ctx := context.Background()
	cl := clusters(t, c)

	count := func(f RiskFilter) (int, error) { return c.RiskCount(ctx, f) }
	open := []string{"open"}

	perCluster := make([]int, len(cl))
	var wg sync.WaitGroup
	var failed int
	var mu sync.Mutex
	for i := range cl {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := count(RiskFilter{Statuses: open, Cluster: cl[i]})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failed++
				return
			}
			perCluster[i] = n
		}()
	}
	wg.Wait()
	sum := 0
	for _, n := range perCluster {
		sum += n
	}
	say("open risks summed over %d clusters=%d (failed queries=%d)", len(cl), sum, failed)
	if failed > 0 {
		t.Errorf("%d per-cluster queries failed", failed)
	}

	total, err := count(RiskFilter{Statuses: open})
	switch {
	case IsTimeout(err):
		say("account-wide open risks: %s (expected on large accounts; the exporter falls back to per-cluster sums)", status(err))
	case err != nil:
		t.Errorf("account-wide open risks: %s", status(err))
	default:
		say("account-wide open risks=%d per-cluster sum=%d", total, sum)
		if !near(total, sum) {
			t.Errorf("account-wide count %d does not match the per-cluster sum %d: clusterName filter or totalResults is not behaving as assumed", total, sum)
		}
	}

	for i, name := range sample(cl, 3) {
		base, err := count(RiskFilter{Statuses: open, Cluster: name})
		if err != nil {
			t.Errorf("cluster #%d open: %s", i+1, status(err))
			continue
		}
		bySeverity := 0
		for _, sev := range []string{"high", "medium", "low"} {
			n, err := count(RiskFilter{Statuses: open, Cluster: name, Severity: sev})
			if err != nil {
				t.Errorf("cluster #%d severity %s: %s", i+1, sev, status(err))
			}
			bySeverity += n
		}
		byCheck := 0
		for _, ct := range CheckTypes {
			n, err := count(RiskFilter{Statuses: open, Cluster: name, CheckType: ct})
			if err != nil {
				t.Errorf("cluster #%d check type: %s", i+1, status(err))
				break
			}
			byCheck += n
		}
		oc, _ := count(RiskFilter{Statuses: []string{"open", "confirmed"}, Cluster: name})
		cf, _ := count(RiskFilter{Statuses: []string{"confirmed"}, Cluster: name})
		say("cluster #%d: open=%d sum-by-severity=%d sum-by-check-type=%d open+confirmed=%d open(%d)+confirmed(%d)",
			i+1, base, bySeverity, byCheck, oc, base, cf)
		if !near(base, bySeverity) {
			t.Errorf("cluster #%d: severity filter does not partition the open risks (%d vs %d)", i+1, base, bySeverity)
		}
		if !near(base, byCheck) {
			t.Errorf("cluster #%d: check type filter does not partition the open risks (%d vs %d)", i+1, base, byCheck)
		}
		if !near(oc, base+cf) {
			t.Errorf("cluster #%d: a multi-value status filter is not the sum of its parts (%d vs %d)", i+1, oc, base+cf)
		}
	}
}

func TestLiveIssueQueries(t *testing.T) {
	c := liveClient(t)
	cl := sample(clusters(t, c), 5)
	now := time.Now()
	calls, ok, failures := 0, 0, map[string]int{}
	for _, name := range cl {
		for _, typ := range IssueTypes {
			calls++
			if _, err := c.Issues(context.Background(), name, typ, []string{"open"}, now.Add(-48*time.Hour), now); err != nil {
				failures[status(err)]++
				continue
			}
			ok++
		}
	}
	say("open issue queries: %d calls over %d clusters, %d ok, failures by kind=%v", calls, len(cl), ok, failures)
	if ok == 0 {
		t.Fatal("no issue query succeeded")
	}
}

// Answers whether fromEpoch/toEpoch filter closed issues by start time, end time or overlap: look for
// issues that closed in the last hour but started earlier, and check whether a 1 h window returns them.
func TestLiveClosedWindowSemantics(t *testing.T) {
	c := liveClient(t)
	cl := sample(clusters(t, c), 10)
	ctx := context.Background()
	now := time.Now()
	hour := now.Add(-time.Hour)
	type key struct {
		typ, summary string
		start        int64
	}
	var recent, longLived, longLivedIn1h, queries, failures int
	for _, name := range cl {
		for _, typ := range IssueTypes {
			queries += 2
			wide, err1 := c.Issues(ctx, name, typ, []string{"closed"}, now.Add(-48*time.Hour), now)
			narrow, err2 := c.Issues(ctx, name, typ, []string{"closed"}, hour, now)
			if err1 != nil || err2 != nil {
				failures++
				continue
			}
			inNarrow := map[key]bool{}
			for _, i := range narrow {
				inNarrow[key{i.Type, i.Summary, i.StartTime}] = true
			}
			for _, i := range wide {
				if i.EndTime < hour.Unix() {
					continue
				}
				recent++
				if i.StartTime < hour.Unix() {
					longLived++
					if inNarrow[key{i.Type, i.Summary, i.StartTime}] {
						longLivedIn1h++
					}
				}
			}
		}
	}
	say("closed-window sample: %d clusters, %d queries, %d pairs failed", len(cl), queries, failures)
	say("closed in the last hour (by end time)=%d, of which started earlier=%d, of those returned by a 1 h window=%d", recent, longLived, longLivedIn1h)
	switch {
	case longLived == 0:
		say("verdict: inconclusive, no long-lived issue closed in the last hour in this sample")
	case longLivedIn1h == longLived:
		say("verdict: a 1 h window returns issues that started earlier, so the filter uses end time or overlap")
	case longLivedIn1h == 0:
		say("verdict: a 1 h window misses issues that started earlier, so the filter uses START time; closed counters undercount long-lived issues")
	default:
		say("verdict: mixed result, inspect")
	}
}
