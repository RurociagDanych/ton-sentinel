# AGENTS.md

## Purpose

Guide for AI coding agents working on TON Sentinel: a local-first pipeline that reads STON.fi and DeDust swaps from a TON Center v3 compatible indexer, aggregates them into five-minute windows per token in Spark, and stores explainable anomaly scores, detections and alerts in PostgreSQL.

## Layout

- `deploy/`: Compose files, app Dockerfile, image lock, monitoring config, Helm chart and local k3d values, workload envelope.
- `docs/`: operations and security notes, tracked normally.
- `scripts/`: `qualify` (static and qualification gates), `local-check`, `local-stack-check` and diagnostic helpers.
- `src/`: the `ton_sentinel` package: source adapter, database layer and migrations, Spark jobs, detectors, API and dashboard.
- `tests/`: `unit/`, `integration/` (real services) and `fixtures/` (recorded public on-chain data, URLs stripped).

## Commands

- Install: `uv sync --locked` (runtime dependencies plus the `dev` group from `uv.lock`).
- Lint: no linter is configured; `uv run python scripts/qualify static` is the static gate.
- Test: `uv run python -m pytest -q tests/unit`; static gate `uv run python scripts/qualify static` (needs Helm 3 on `PATH` or `TON_SENTINEL_HELM`).
- Run: `docker compose --env-file .env -p ton-sentinel-local -f deploy/compose.yaml up -d --wait postgres kafka`; stop with `... down --volumes`.
- Deploy dry run: `docker compose --env-file .env -p ton-sentinel-local -f deploy/compose.yaml config --quiet`. There is no remote deployment.

## Conventions

- Language: English for code, commit messages and docs. Python 3.10 or newer. Source code, Dockerfiles, compose and YAML files carry no comments or docstrings; explanations go in `docs/`.
- Formatters: none configured; follow the style of the surrounding code.
- Commit style: Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`).
- Branch naming: prefixed branches such as `docs/...`, `fix/...`, `chore/...`.
- Test locations: unit tests in `tests/unit`, service-backed tests in `tests/integration` (marked `integration` and run inside the `app` container), recorded data in `tests/fixtures`. Do not reuse a test module file name across directories.

## Guardrails

- Secrets: credentials come only from the git-ignored `.env`; `.env.example` holds placeholders. Never commit `.env`, keys or passwords. `tests/unit/test_publishable_repo.py` enforces these rules.
- Git: no force-push and no history rewrite.
- Deletes: do not delete files or directories without owner approval.
- Generated paths: `.venv/`, `.cache/` and `__pycache__/` are generated and git-ignored.
- Frozen paths: do not change `deploy/` (including the Helm chart) without owner approval.
- Lock file: `uv.lock` is the only authoritative Python lock. Change dependencies in `pyproject.toml` and run `uv lock`. `deploy/app.Dockerfile` exports the image's runtime requirements from `uv.lock` in a build stage (see `docs/OPERATIONS.md`).
