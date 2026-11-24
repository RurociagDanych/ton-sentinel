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

- Language: English for code, commit messages and docs. Python 3.10 or newer.
- Public modules, classes and functions have docstrings. Comments explain why, not what. Tool directives such as # noqa are allowed.
- Formatters: none configured; follow the style of the surrounding code.
- Commit style: Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`, `ci:`, optional scope such as `feat(detector):`).
- PR branches: use `feat/`, `fix/`, `docs/`, or `chore/` followed by a short lowercase kebab-case description (for example, `fix/dedust-window-alignment`). Branch from current `origin/main` and open the PR against `main` unless the task specifies another base.
- PR titles: use `type(scope): short imperative summary` in English (for example, `fix(detector): align five-minute swap window boundaries`). Choose the type for the change and the scope for the affected component, regardless of the branch prefix.
- PR body: follow the default template at [`RurociagDanych/.github` (`PULL_REQUEST_TEMPLATE.md`)](https://github.com/RurociagDanych/.github/blob/main/.github/PULL_REQUEST_TEMPLATE.md) (`Summary`, `Changes`, `Not changed and why`, `Owner decisions applied`, `Verification`, `Open risks`), filling every section and removing HTML comments; never merge PRs (the owner merges).
- Test locations: unit tests in `tests/unit`, service-backed tests in `tests/integration` (marked `integration` and run inside the `app` container), recorded data in `tests/fixtures`. Do not reuse a test module file name across directories.

## Guardrails

- Secrets: credentials come only from the git-ignored `.env`; `.env.example` holds placeholders. Never commit `.env`, keys or passwords. `tests/unit/test_publishable_repo.py` enforces these rules.
- Git: no force-push and no history rewrite on shared branches; never merge PRs (the owner merges).
- Deletes: do not delete files or directories without owner approval.
- Generated paths: `.venv/`, `.cache/` and `__pycache__/` are generated and git-ignored.
- Frozen paths: do not change `deploy/` (including the Helm chart) without owner approval.
- Lock file: `uv.lock` is the only authoritative Python lock. Change dependencies in `pyproject.toml` and run `uv lock`. `deploy/app.Dockerfile` exports the image's runtime requirements from `uv.lock` in a build stage (see `docs/OPERATIONS.md`).
