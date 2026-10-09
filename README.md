# komodor-metrics-exporter

Prometheus exporter for the [Komodor](https://komodor.com) public API. It polls on an interval and exposes counts you can scrape (Prometheus, or the OpenTelemetry Collector's `prometheus` receiver) to keep history the API itself does not retain.

## Metrics

| Metric | Labels | Notes |
|---|---|---|
| `komodor_clusters` | | Clusters connected to Komodor |
| `komodor_reliability_risks` | `status`, `severity` | All statuses: open, confirmed, resolved, dismissed, ignored, manually_resolved |
| `komodor_reliability_risks_active` | `cluster`, `check_type`, `severity` | Open + confirmed risks |
| `komodor_issues_open` | `cluster`, `type` | See limits below |
| `komodor_issues_closed_total` | `cluster`, `type` | Counter of issues seen closing since start |
| `komodor_exporter_errors_total`, `komodor_exporter_last_success_timestamp_seconds` | | Exporter health |

## Configuration

Every setting is a flag, and also an environment variable with the `KOMODOR_` prefix (`--poll-interval` is `KOMODOR_POLL_INTERVAL`). Precedence: flag, then env, then `--config` file, then default. Run with `--help` for the list.

| Flag | Default | |
|---|---|---|
| `--api-key` | required | Sent as `X-API-KEY`. Prefer `KOMODOR_API_KEY`; flags show up in process listings |
| `--api-url` | `https://api.komodor.com` | |
| `--poll-interval` | `5m` | Minimum `30s` |
| `--listen-addr` | `:9090` | Serves `/metrics` and `/healthz` |
| `--log-level` | `info` | `debug`, `info`, `warn`, `error` |
| `--log-format` | `json` | `json` or `text` |
| `--config` | none | Optional yaml/json/toml file using the same keys, e.g. `poll-interval: 10m` |

## Logging

Structured logs (JSON by default) go to stderr. At `--log-level debug` every API attempt is logged with method, URL, status and duration, along with retry decisions and per-cluster issue counts. Request headers are never logged, so the API key stays out.

```sh
docker run -e KOMODOR_API_KEY=... -p 9090:9090 ghcr.io/davidcollom/komodor-metrics-exporter
```

## Grafana dashboard

Import [`dashboards/komodor-platform.json`](dashboards/komodor-platform.json) into Grafana and pick your Prometheus data source. It shows clusters, risks by status, severity, cluster and check type, open issues, issues closed per hour, and exporter freshness.

## Limits

- The issues API only looks back 2 days per query and returns no issue ID. Issues open for more than 2 days are not counted in `komodor_issues_open`, and closed issues are deduplicated on cluster, type, start time and summary, so `komodor_issues_closed_total` is approximate. Closed issues present at startup are not counted.
- Issues are queried per cluster and type, so API calls per poll grow with cluster count.
- Failed API calls are retried up to 4 times with backoff (429 and 5xx), so a briefly rate-limited poll usually still completes.
- History starts when the exporter does; nothing is backfilled.

## Release

Tag `vX.Y.Z`; GitHub Actions runs GoReleaser to publish binaries and the `ghcr.io` image.
