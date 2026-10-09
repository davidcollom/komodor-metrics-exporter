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

	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	col := NewCollector(NewClient(v.GetString("api-url"), key, v.GetDuration("request-timeout"), reg), interval, reg)

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

	slog.Info("starting", "version", version, "addr", srv.Addr, "api_url", v.GetString("api-url"), "poll_interval", interval.String(), "request_timeout", v.GetDuration("request-timeout").String())
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	slog.Info("stopped")
	return nil
}
