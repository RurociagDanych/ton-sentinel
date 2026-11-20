# TON Sentinel

TON Sentinel reads STON.fi and DeDust swaps from a TON Center v3 compatible indexer and
aggregates them into five-minute windows per token. It produces explainable anomaly scores per
window, persisted as detections and alerts. This lets you see which tokens departed from their
recent swap activity, and why. Not a trading tool or fraud classifier.

**Status:** local-first engineering project; see [doc/operations.md](doc/operations.md).

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

## Key design decisions

- **PostgreSQL is canonical state, Kafka is a delivery buffer.** Swaps, revisions, the outbox,
  checkpoints and detections live in PostgreSQL, so Kafka can be replayed or lost without losing
  data. Trade-off: an outbox and checkpoints to maintain, and one more hop per swap.
- **Robust-score detectors (median and MAD over earlier windows).** Two detectors, swap count
  and unique active wallets, each with a score a reader can recompute by hand. Trade-off: less
  sensitive than a learned model, and effectiveness is not measured for lack of labelled data.
- **Unknown stays unknown.** Unknown traders, missing coverage and short history are shown as
  such instead of becoming zeros or invented counts. Trade-off: sparser dashboards.
- **Loopback-only, read-only surface.** The API is GET-only, services bind to loopback, and the
  API reads through a least-privilege database role. Trade-off: no remote access without extra
  setup. Details in [doc/security.md](doc/security.md).
- **Local-first, pinned builds.** Images are pinned by digest and the app image is built
  locally. Trade-off: new builds must be re-pinned, and nothing is published.

## Limitations

- Budgets: 6 GB RAM and 2 CPU cores, local ceilings from `deploy/workload/acceptance.json`.
- No labelled data, so detector quality is unmeasured. A flagged window is a prompt to look.
- Not verified: sustained live load, ARM64, cloud deployment. See
  [doc/operations.md](doc/operations.md).

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

Always pass `--env-file .env`. Image build, pinning and integration tests are described in
[doc/operations.md](doc/operations.md).

## Repository layout

| Path | Contents |
|---|---|
| `src/ton_sentinel/` | Source adapter, database layer and migrations, Spark jobs, detectors, API, dashboard |
| `tests/` | `unit/`, `integration/` (real services), `fixtures/` (recorded public on-chain data, URLs stripped) |
| `scripts/` | `qualify` (static and qualification gates), `local-check`, `local-stack-check` |
| `doc/` | Operations and security notes |
| `deploy/` | Compose files, app Dockerfile, image lock, monitoring config, Helm chart, workload envelope |

## License

MIT. See [LICENSE](LICENSE).
