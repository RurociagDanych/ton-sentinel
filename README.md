# TON Sentinel

TON Sentinel detects unusual DEX swap activity on TON. It reads STON.fi and DeDust swaps from a
TON Center v3 compatible indexer, aggregates them into five-minute windows per token, and flags
windows whose activity departs from recent history, with an explanation of each score. It is a
local-first engineering project, not a trading tool, investment advice or a fraud classifier.

## Architecture

```text
TON indexer (read-only GET) -> source adapter -> PostgreSQL (canonical swaps, delivery outbox)
   -> Kafka -> Spark Structured Streaming (five-minute event-time windows)
   -> PostgreSQL (features, detections, alerts) -> read-only HTTP API and dashboard
   -> Prometheus metrics -> Grafana dashboards and Alertmanager rules
```

- **Source adapter** (`src/ton_sentinel/source`): normalizes STON.fi and DeDust swaps from
  indexer actions and traces. A key, if configured, is sent only to the configured host.
- **PostgreSQL** holds the canonical state: swap history and revisions, the delivery outbox,
  checkpoints, features, detections and alerts. Migrations live in `src/ton_sentinel/db/migrations`.
- **Kafka** buffers delivery; **Spark Structured Streaming** computes windowed features.
- **Detectors** (`src/ton_sentinel/detectors.py`): two explainable robust-score detectors
  (median and MAD over earlier windows), one for swap count and one for unique active wallets.
- **API and dashboard** (`src/ton_sentinel/api.py`, `src/ton_sentinel/dashboard`): GET-only,
  bound to loopback, reading through a least-privilege database role.
- **Monitoring** (`deploy/compose.monitoring.yaml`, `deploy/monitoring`): Prometheus scrapes the
  API metrics; Grafana and Alertmanager run beside it, all bound to loopback.
- **Helm chart** (`deploy/helm/ton-sentinel`), with local k3d values in `deploy/helm/local`.

Unknown traders, missing coverage and short history stay visible as such instead of becoming
zeros or invented counts.

## Quick start

Requirements: Linux x86_64, Python 3.10 or newer, Docker with Compose v2, and Helm 3 for the
static gate.

```bash
git clone <this repository> ton-sentinel && cd ton-sentinel

# 1. Python environment for the host test suite
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt

# 2. Local settings: copy the template and replace the placeholders (.env is git-ignored)
cp .env.example .env

# 3. Local PostgreSQL and Kafka (images pinned by digest)
docker compose --env-file .env -p ton-sentinel-local -f deploy/compose.yaml up -d --wait postgres kafka

# 4. Tests: unit suite and the static gate (Helm on PATH, or set TON_SENTINEL_HELM)
.venv/bin/python -m pytest -q tests/unit
.venv/bin/python scripts/qualify static

# Stop and remove the local stack
docker compose --env-file .env -p ton-sentinel-local -f deploy/compose.yaml down --volumes
```

Always pass `--env-file .env`: Compose otherwise looks for `.env` next to the compose file and
stops with `set TON_SENTINEL_PG_PASSWORD in .env`.

The `app` and API image (Spark plus the project code) is built locally from
`deploy/app.Dockerfile`; no prebuilt image is published. The build context needs the Spark
connector jars listed with their SHA-256 in `deploy/images.lock.json`, and the compose files pin
the image by digest, so a new build has to be pinned there. Integration tests
(`tests/integration`) run inside that `app` container against real PostgreSQL, Kafka and Spark.

## Repository layout

| Path | Contents |
|---|---|
| `src/ton_sentinel/` | Source adapter, database layer and migrations, Spark jobs, detectors, API, dashboard |
| `tests/` | `unit/`, `integration/` (real services), `fixtures/` (recorded public on-chain data, URLs stripped) |
| `scripts/` | `qualify` (static and qualification gates), `local-check`, `local-stack-check` |
| `deploy/` | Compose files, app Dockerfile, image lock, monitoring config, Helm chart, workload envelope |

## Limitations

- **Not production.** It runs on one local host: no hosting, no high availability, no public
  port. Local volumes are not backups. Local budgets are 6 GB RAM and 2 CPU.
- **Detector effectiveness is not measured.** There is no labelled anomaly data. The detectors
  were checked for correct arithmetic and on injected synthetic spikes only, so a flagged window
  is a prompt to look, not evidence of manipulation.
- **No scheduler is enabled by default.** Ingest and scoring run when started; the bounded live
  monitor loop stays off unless explicitly enabled.
- Source coverage and trader attribution were checked against hand-derived truth on recorded
  samples only. The indexer's action classifier misses some DeDust routes; a trace recognizer
  fills the known gaps.
- Not verified: sustained live load, ARM64, any cloud deployment.
- The app image is built locally and not published (see Quick start).

## License

MIT. See [LICENSE](LICENSE).
