# Operations

> Purpose: Explain how the local stack, image build and scheduling work, and list the budgets and known gaps.
> Audience: operators, contributors
> Last reviewed: 2026-11-20

Sections:

- Local stack
- Scheduling
- Budgets
- Status and known gaps

## Local stack

Everything runs on one local host through `deploy/compose.yaml` (PostgreSQL, Kafka) and
`deploy/compose.monitoring.yaml` (Prometheus, Grafana, Alertmanager). Always pass
`--env-file .env`: Compose otherwise looks for `.env` next to the compose file and stops with
`set TON_SENTINEL_PG_PASSWORD in .env`.

The `app` and API image (Spark plus the project code) is built locally from
`deploy/app.Dockerfile`; no prebuilt image is published. The build context needs the Spark
connector jars listed with their SHA-256 in `deploy/images.lock.json`, and the compose files pin
the image by digest, so a new build has to be pinned there. `uv.lock` is the only Python lock in
the repository. The build context holds `pyproject.toml`, `uv.lock`, `jars/` and
`src/ton_sentinel`; a first build stage exports the hash-pinned runtime set from `uv.lock` with a
digest-pinned `uv` image, and the final stage installs it with `pip --require-hashes`. Integration
tests (`tests/integration`) run inside that `app` container against real PostgreSQL, Kafka and
Spark.

Configuration notes for `deploy/`:

- `compose.yaml` is for local synthetic checks only: no published ports and no external runtime
  egress. The PostgreSQL service runs with `init: true` so docker-init reaps orphaned
  `pg_isready` healthcheck processes; without it the postmaster is PID 1 and crash-restarts its
  backends. The Kafka healthcheck is a TCP probe because a Kafka CLI check starts a JVM per
  interval inside the 0.5 CPU quota.
- `compose.dashboard.yaml` adds the loopback-only dashboard route
  (`docker compose -f deploy/compose.yaml -f deploy/compose.dashboard.yaml up -d api`). Create
  the git-ignored reader DSN file (mode 0600) first. The API reads Kafka end offsets over the
  internal PLAINTEXT listener (no group, no commits, no credentials) and the database volume
  usage through a read-only mount.
- `compose.monitoring.yaml` runs Prometheus, Grafana and Alertmanager on loopback; run
  `deploy/monitoring/init-secrets.sh` first to create the git-ignored Grafana admin password
  file. Image digests are index digests resolved with `docker buildx imagetools inspect`.
- Alert thresholds in `deploy/monitoring/prometheus/rules/alerts.yml` are local defaults for a
  single-node synthetic fixture, not production SLOs; change a threshold by editing the literal
  in the alert's `expr` and its `for`. Alertmanager routes every alert to the in-network webhook
  sink. `deploy/monitoring/prometheus/tests/alerts.test.yml` holds one firing and one non-firing
  case per alert.
- The Helm chart `values.yaml` is an offline hold with zero replicas. The local k3d override
  `deploy/helm/local/values-local-k3s.yaml` is for an amd64 test cluster only; the test creates
  the `sl-secret` secret locally (see `tests/integration/test_local_k3s.py`) and the maintenance
  CronJob stays suspended.

## Scheduling

No scheduler is enabled by default. Ingest and scoring run when started; the bounded live
monitor loop stays off unless explicitly enabled.

## Budgets

Local ceilings are 6 GB RAM and 2 CPU cores, fixed as local host budgets in
`deploy/workload/acceptance.json`. They are not cloud entitlements.

## Status and known gaps

- Runs on one local host: no hosting, no high availability, no public port. Local volumes are
  not backups.
- Detector effectiveness is not measured. There is no labelled anomaly data; the detectors were
  checked for correct arithmetic and on injected synthetic spikes only.
- Source coverage and trader attribution were checked against hand-derived truth on recorded
  samples only. The indexer's action classifier misses some DeDust routes; a trace recognizer
  fills the known gaps.
- Not verified: sustained live load, ARM64, any cloud deployment.
