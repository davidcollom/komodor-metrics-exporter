package cmd

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
)

// execute runs the command with a clean KOMODOR_* environment so the host's settings cannot leak in.
func execute(t *testing.T, args ...string) (string, error) {
	t.Helper()
	for _, kv := range os.Environ() {
		if k, _, _ := strings.Cut(kv, "="); strings.HasPrefix(k, "KOMODOR_") {
			t.Setenv(k, "")
		}
	}
	cmd := newRootCmd("test")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(context.Background())
	return out.String(), err
}

func TestInvalidSettingsAreRejectedBeforeAnythingStarts(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"no api key", nil, "API key is required"},
		{"short poll interval", []string{"--api-key", "k", "--poll-interval", "5s"}, "at least 30s"},
		{"window below 2x interval", []string{"--api-key", "k", "--poll-interval", "5m", "--issues-window", "5m"}, "--issues-window"},
		{"window above the API maximum", []string{"--api-key", "k", "--issues-window", "72h"}, "--issues-window"},
		{"unknown metric group", []string{"--api-key", "k", "--disable", "bogus"}, `unknown metric group "bogus"`},
		{"unknown issue type", []string{"--api-key", "k", "--issue-types", "nope"}, `unknown issue type "nope"`},
		{"malformed skip entry", []string{"--api-key", "k", "--skip-issues", "justacluster"}, "invalid skip-issues entry"},
		{"bad log level", []string{"--api-key", "k", "--log-level", "loud"}, "invalid --log-level"},
		{"bad log format", []string{"--api-key", "k", "--log-format", "xml"}, "invalid --log-format"},
		{"missing config file", []string{"--api-key", "k", "--config", "/no/such/file.yaml"}, "read config"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := execute(t, tc.args...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestConfigFileValuesAreUsed(t *testing.T) {
	cfg := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(cfg, []byte("poll-interval: 5s\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Rejected only because the file's 5s is below the minimum, which proves the file was read.
	if _, err := execute(t, "--api-key", "k", "--config", cfg); err == nil || !strings.Contains(err.Error(), "at least 30s") {
		t.Fatalf("err = %v", err)
	}
}

func TestEnvListsAreSplitOnCommas(t *testing.T) {
	t.Setenv("KOMODOR_DISABLE", "issues,bogus")
	t.Setenv("KOMODOR_API_KEY", "k")
	cmd := newRootCmd("test")
	cmd.SetArgs(nil)
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	err := cmd.ExecuteContext(context.Background())
	if err == nil || !strings.Contains(err.Error(), `"bogus"`) {
		t.Fatalf("err = %v; the second comma-separated value should have been validated", err)
	}
}

func TestMetricGroupsFromTheConfigFileAreValidated(t *testing.T) {
	cfg := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(cfg, []byte("metrics:\n  nope: false\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := execute(t, "--api-key", "k", "--config", cfg); err == nil || !strings.Contains(err.Error(), `"nope"`) {
		t.Fatalf("err = %v", err)
	}
}

func TestVersionFlag(t *testing.T) {
	out, err := execute(t, "--version")
	if err != nil || !strings.Contains(out, "version test") {
		t.Fatalf("out %q err %v", out, err)
	}
}

func TestSplitList(t *testing.T) {
	v := viper.New()
	v.Set("x", []string{"a,b", " c ", "", ",d,"})
	if got := fmt.Sprint(splitList(v, "x")); got != "[a b c d]" {
		t.Errorf("got %v", got)
	}
	if got := splitList(v, "missing"); len(got) != 0 {
		t.Errorf("got %v, want nothing", got)
	}
}

func TestSetupLogger(t *testing.T) {
	for _, tc := range []struct {
		level, format string
		ok            bool
	}{
		{"debug", "json", true}, {"info", "text", true}, {"WARN", "json", true}, {"error", "text", true},
		{"loud", "json", false}, {"info", "xml", false},
	} {
		if err := setupLogger(tc.level, tc.format); (err == nil) != tc.ok {
			t.Errorf("setupLogger(%q,%q) err = %v, want ok=%v", tc.level, tc.format, err, tc.ok)
		}
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// A full start: the exporter polls a fake Komodor API, serves /metrics and /healthz, and shuts down cleanly.
func TestRunServesMetricsAndStopsOnCancel(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"totalResults":3,"data":{"clusters":[{"name":"c1"}],"issues":[]}}`))
	}))
	defer api.Close()
	addr := freeAddr(t)

	for _, kv := range os.Environ() {
		if k, _, _ := strings.Cut(kv, "="); strings.HasPrefix(k, "KOMODOR_") {
			t.Setenv(k, "")
		}
	}
	cmd := newRootCmd("test")
	cmd.SetArgs([]string{"--api-key", "k", "--api-url", api.URL, "--listen-addr", addr, "--log-level", "error"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cmd.ExecuteContext(ctx) }()

	get := func(path string) (int, string) {
		resp, err := http.Get("http://" + addr + path)
		if err != nil {
			return 0, ""
		}
		defer resp.Body.Close()
		var b bytes.Buffer
		_, _ = b.ReadFrom(resp.Body)
		return resp.StatusCode, b.String()
	}
	var metrics string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if code, body := get("/metrics"); code == 200 && strings.Contains(body, "komodor_clusters 1") && strings.Contains(body, "komodor_exporter_last_poll_success 1") {
			metrics = body
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if metrics == "" {
		t.Fatal("/metrics never showed a successful poll")
	}
	if code, _ := get("/healthz"); code != 200 {
		t.Errorf("/healthz = %d", code)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("clean shutdown should not be an error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the exporter did not stop after cancel")
	}
}

func TestExecutePrintsTheErrorAndReturnsIt(t *testing.T) {
	for _, kv := range os.Environ() {
		if k, _, _ := strings.Cut(kv, "="); strings.HasPrefix(k, "KOMODOR_") {
			t.Setenv(k, "")
		}
	}
	old := os.Args
	os.Args = []string{"komodor-metrics-exporter"}
	defer func() { os.Args = old }()
	if err := Execute("test"); err == nil {
		t.Fatal("Execute should return the error for a missing API key")
	}
}
