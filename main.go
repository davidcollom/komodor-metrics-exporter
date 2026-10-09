package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var version = "dev"

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	v := viper.New()
	var cfgFile string

	cmd := &cobra.Command{
		Use:           "komodor-metrics-exporter",
		Short:         "Prometheus exporter for the Komodor public API",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			if cfgFile != "" {
				v.SetConfigFile(cfgFile)
				if err := v.ReadInConfig(); err != nil {
					return fmt.Errorf("read config: %w", err)
				}
			}
			return setupLogger(v.GetString("log-level"), v.GetString("log-format"))
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd.Context(), v)
		},
	}

	f := cmd.Flags()
	f.StringVar(&cfgFile, "config", "", "optional config file (yaml/json/toml); flags and env override it")
	f.String("api-key", "", "Komodor API key (prefer the KOMODOR_API_KEY env var; flags show in process listings)")
	f.String("api-url", "https://api.komodor.com", "Komodor API base URL")
	f.Duration("request-timeout", 2*time.Minute, "timeout for each API request attempt; raise it for slow endpoints")
	f.StringSlice("disable", nil, "metric groups to switch off: clusters, risks, risks_active, risks_by_check, issues (or set metrics.<group>: false in the config file)")
	f.Duration("issues-window", time.Hour, "how far back to look for closed issues (max 48h); open issues always use the full 48h")
	f.StringSlice("skip-issues", nil, "cluster/type pairs to skip when querying issues, e.g. my-cluster/node-issue; either side may be * (or skip-issues: [..] in the config file)")
	f.StringSlice("issue-types", nil, "only query these issue types (default all): "+strings.Join(IssueTypes, ", "))
	f.Int("max-retries", 2, "retries per request on 5xx (except 504), 429 and network errors; backoff is 1s, 2s, 4s, ...")
	f.Int("concurrency", 8, "maximum concurrent API requests (cluster-scoped and issues calls)")
	f.Int("slow-concurrency", 2, "maximum concurrent account-wide risk queries, which can take a minute or time out")
	f.Duration("poll-interval", 5*time.Minute, "how often to poll the API (minimum 30s)")
	f.String("listen-addr", ":9090", "address serving /metrics and /healthz")
	f.String("log-level", "info", "debug, info, warn or error; debug logs every API request")
	f.String("log-format", "json", "json or text")

	v.SetEnvPrefix("KOMODOR")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()
	if err := v.BindPFlags(f); err != nil {
		panic(err)
	}
	return cmd
}

func setupLogger(level, format string) error {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return fmt.Errorf("invalid --log-level %q: %w", level, err)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	switch format {
	case "json":
		h = slog.NewJSONHandler(os.Stderr, opts)
	case "text":
		h = slog.NewTextHandler(os.Stderr, opts)
	default:
		return fmt.Errorf("invalid --log-format %q: want json or text", format)
	}
	slog.SetDefault(slog.New(h).With("service", "komodor-metrics-exporter"))
	return nil
}

func run(parent context.Context, v *viper.Viper) error {
	key := v.GetString("api-key")
	if key == "" {
		return errors.New("an API key is required: set KOMODOR_API_KEY")
	}
	interval := v.GetDuration("poll-interval")
	if interval < 30*time.Second {
		return fmt.Errorf("--poll-interval must be at least 30s, got %s", interval)
	}

	window := v.GetDuration("issues-window")
	if window > maxIssueWindow || window < 2*interval {
		return fmt.Errorf("--issues-window must be between 2x the poll interval (%s) and %s, got %s", 2*interval, maxIssueWindow, window)
	}

	cfg := map[string]bool{}
	for name := range v.GetStringMap("metrics") {
		cfg[name] = v.GetBool("metrics." + name)
	}
	enabled, err := ParseEnabled(cfg, splitList(v, "disable"))
	if err != nil {
		return err
	}
	issueFilter, err := NewIssueFilter(splitList(v, "issue-types"), splitList(v, "skip-issues"))
	if err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	col := NewCollector(NewClient(ClientOptions{
		BaseURL: v.GetString("api-url"), APIKey: key, Timeout: v.GetDuration("request-timeout"),
		Concurrency: v.GetInt("concurrency"), SlowConcurrency: v.GetInt("slow-concurrency"), MaxRetries: v.GetInt("max-retries"),
	}, reg), interval, window, enabled, issueFilter, reg)

	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()
	go col.Run(ctx)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	srv := &http.Server{Addr: v.GetString("listen-addr"), Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()

	slog.Info("starting", "version", version, "addr", srv.Addr, "api_url", v.GetString("api-url"), "poll_interval", interval.String(), "request_timeout", v.GetDuration("request-timeout").String(), "concurrency", v.GetInt("concurrency"), "slow_concurrency", v.GetInt("slow-concurrency"), "issues_window", window.String(), "issue_types", splitList(v, "issue-types"), "skip_issues", splitList(v, "skip-issues"))
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	slog.Info("stopped")
	return nil
}

// splitList reads a list setting. Viper splits an env string on whitespace only, so a comma-separated
// KOMODOR_* value needs its own split; flags and config-file lists pass through unchanged.
func splitList(v *viper.Viper, key string) []string {
	var out []string
	for _, item := range v.GetStringSlice(key) {
		for _, s := range strings.Split(item, ",") {
			if s = strings.TrimSpace(s); s != "" {
				out = append(out, s)
			}
		}
	}
	return out
}
