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

| Env var | Default | |
|---|---|---|
| `KOMODOR_API_KEY` | required | Sent as `X-API-KEY` |
| `KOMODOR_API_URL` | `https://api.komodor.com` | |
| `KOMODOR_POLL_INTERVAL` | `5m` | Minimum `30s` |
| `LISTEN_ADDR` | `:9090` | `/metrics`, `/healthz` |

```sh
docker run -e KOMODOR_API_KEY=... -p 9090:9090 ghcr.io/davidcollom/komodor-metrics-exporter
```

## Limits

- The issues API only looks back 2 days per query and returns no issue ID. Issues open for more than 2 days are not counted in `komodor_issues_open`, and closed issues are deduplicated on cluster, type, start time and summary, so `komodor_issues_closed_total` is approximate. Closed issues present at startup are not counted.
- Issues are queried per cluster and type, so API calls per poll grow with cluster count.
- History starts when the exporter does; nothing is backfilled.

## Release

Tag `vX.Y.Z`; GitHub Actions runs GoReleaser to publish binaries and the `ghcr.io` image.
