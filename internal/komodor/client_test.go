package komodor

import (
	"context"
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
