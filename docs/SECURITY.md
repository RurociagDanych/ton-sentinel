# Security

> Purpose: List the security controls of the local stack and the guards that enforce them.
> Audience: reviewers, operators
> Last reviewed: 2026-11-20

Sections:

- Controls

## Controls

- **Network:** the API, dashboard, Prometheus, Grafana and Alertmanager bind to loopback only.
  The API is GET-only.
- **Database access:** the API reads through a least-privilege database role.
- **Indexer key:** a TON Center key, if configured, is sent only to the configured host. The
  source adapter issues read-only GET requests.
- **Secrets:** credentials come from the environment (`.env`, git-ignored). `.env.example`
  carries placeholders only. Compose reads the database password from
  `TON_SENTINEL_PG_PASSWORD`; no literal password is committed.
- **Fixtures:** recorded public on-chain data with URLs and hosts stripped.
- **Images:** pinned by digest; Spark connector jars pinned by SHA-256 in
  `deploy/images.lock.json`.
- These guards are enforced by `tests/unit/test_publishable_repo.py`.
