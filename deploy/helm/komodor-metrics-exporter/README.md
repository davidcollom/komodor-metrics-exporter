# komodor-metrics-exporter

Prometheus exporter for the Komodor public API. It polls reliability risks, issues and clusters and exposes them on `/metrics`. Source and metric reference: https://github.com/davidcollom/komodor-metrics-exporter

## Install

```sh
helm install komodor-metrics-exporter oci://ghcr.io/davidcollom/charts/komodor-metrics-exporter \
  --version <version> --set apiKey=<komodor-api-key>
```

## API key

The exporter needs a Komodor API key. Provide it one of two ways:

**1. Let the chart create the Secret** by setting `apiKey`. Simple, but the key then passes through your Helm values.

**2. Use a Secret you manage** with `existingSecret`. This is the way to use External Secrets Operator, Sealed Secrets, SOPS, a Vault injector or `kubectl create secret`, because the chart only needs a Kubernetes Secret to exist and knows nothing about the tool that makes it:

```sh
kubectl create secret generic komodor-api --from-literal=api-key=<komodor-api-key>
helm install komodor-metrics-exporter oci://ghcr.io/davidcollom/charts/komodor-metrics-exporter \
  --version <version> --set existingSecret=komodor-api
```

If the key inside your Secret is not called `api-key`, set `existingSecretKey`. With `existingSecret` set the chart creates no Secret and ignores `apiKey`. The Secret must be in the release namespace. If the name or key is wrong the pod stays in `CreateContainerConfigError`.

Example with External Secrets Operator:

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: komodor-api
spec:
  secretStoreRef: {name: my-store, kind: ClusterSecretStore}
  target: {name: komodor-api}
  data:
    - secretKey: api-key
      remoteRef: {key: komodor/api-key}
```

The key is never written to the ConfigMap.

## Configuration

Everything under `config:` is rendered into a ConfigMap and passed to the exporter with `--config`. The defaults are the exporter's own, so an untouched install behaves like the plain binary. `env` entries (`KOMODOR_*`) and flags take precedence over the file.

| Value | Default | Description |
|---|---|---|
| `apiKey` | `""` | Komodor API key; the chart creates a Secret from it |
| `existingSecret` | `""` | Name of an existing Secret holding the key; the chart creates none and ignores `apiKey` |
| `existingSecretKey` | `api-key` | Key inside the Secret |
| `config.poll-interval` | `5m` | How often to poll the API (minimum `30s`) |
| `config.request-timeout` | `2m` | Timeout per API request attempt |
| `config.issues-window` | `1h` | How far back to look for closed issues (at least 2x the poll interval, max `48h`) |
| `config.concurrency` | `8` | Maximum concurrent cluster-scoped and issues requests |
| `config.slow-concurrency` | `2` | Maximum concurrent account-wide risk queries |
| `config.max-retries` | `2` | Retries on 5xx (not 504), 429 and network errors |
| `config.listen-addr` | `:9090` | Address serving `/metrics` and `/healthz` |
| `config.log-level` | `info` | `debug`, `info`, `warn`, `error`; `debug` logs every API request |
| `config.log-format` | `json` | `json` or `text` |
| `config.metrics.<group>` | all `true` | Switch a group off: `clusters`, `risks`, `risks_active`, `risks_by_check`, `issues` |
| `config.skip-issues` | `[]` | `cluster/type` pairs not to query for issues; either side may be `*` |
| `config.issue-types` | `[]` | Only query these issue types (empty means all) |
| `env` | `{}` | Extra environment variables, e.g. `KOMODOR_LOG_LEVEL: debug` |
| `image.repository` / `image.tag` | `ghcr.io/davidcollom/komodor-metrics-exporter` / chart `appVersion` | Container image |
| `service.type` / `service.port` / `service.annotations` | `ClusterIP` / `9090` / `{}` | Service exposing `/metrics` |
| `serviceMonitor.enabled` | `false` | Create a Prometheus Operator ServiceMonitor (also `interval`, `labels`) |
| `resources`, `nodeSelector`, `tolerations`, `affinity`, `podAnnotations`, `imagePullSecrets` | see `values.yaml` | Standard pod settings |

The Deployment runs a single replica, non-root, with a read-only root filesystem and all capabilities dropped. It restarts when the config changes.
