# Operations

## Local stack

Everything runs on one local host through `deploy/compose.yaml` (PostgreSQL, Kafka) and
`deploy/compose.monitoring.yaml` (Prometheus, Grafana, Alertmanager). Always pass
`--env-file .env`: Compose otherwise looks for `.env` next to the compose file and stops with
`set TON_SENTINEL_PG_PASSWORD in .env`.

The `app` and API image (Spark plus the project code) is built locally from
`deploy/app.Dockerfile`; no prebuilt image is published. The build context needs the Spark
connector jars listed with their SHA-256 in `deploy/images.lock.json`, and the compose files pin
the image by digest, so a new build has to be pinned there. Integration tests
(`tests/integration`) run inside that `app` container against real PostgreSQL, Kafka and Spark.

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
