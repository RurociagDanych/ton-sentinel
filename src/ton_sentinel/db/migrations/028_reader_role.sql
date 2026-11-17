-- Least-privilege PostgreSQL reader role for the API. Additive and idempotent.
--
-- sentinel_reader is NOLOGIN; operators make a login role a member of it. It gets USAGE on the
-- current schema and SELECT on the allowlist below only: no INSERT/UPDATE/DELETE/TRUNCATE, no
-- CREATE, and no default privileges, so tables added later stay invisible until granted by a
-- deliberate migration. The list must equal READER_TABLES in ton_sentinel.db.serving.
-- If the migrator lacks CREATEROLE and the role is absent, a NOTICE is raised and grants skipped.
-- allowlist:
DO $reader$
DECLARE
    tbl text;
    allowlist text[] := ARRAY[
        'action_window_coverage',
        'action_window_qualification',
        'active_detector_policy',
        'alert_revisions',
        'baseline_inputs',
        'coverage_ranges',
        'delivery_quarantine',
        'detector_current',
        'detector_policies',
        'detector_results',
        'dirty_windows',
        'ingestion_pauses',
        'outbox',
        'recovery_pauses',
        'retention_runs',
        'revision_quarantine',
        'scan_quarantine',
        'served_features',
        'stream_batches',
        'window_admission_refusals'];
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sentinel_reader') THEN
        IF (SELECT rolsuper OR rolcreaterole FROM pg_roles WHERE rolname = current_user) THEN
            CREATE ROLE sentinel_reader NOLOGIN;
        ELSE
            RAISE NOTICE 'sentinel_reader absent and migrator lacks CREATEROLE; reader grants skipped';
            RETURN;
        END IF;
    END IF;
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO sentinel_reader', current_schema());
    FOREACH tbl IN ARRAY allowlist LOOP
        IF to_regclass(format('%I.%I', current_schema(), tbl)) IS NOT NULL THEN
            EXECUTE format('GRANT SELECT ON TABLE %I.%I TO sentinel_reader', current_schema(), tbl);
        ELSE
            RAISE EXCEPTION 'reader allowlist table % is missing', tbl;
        END IF;
    END LOOP;
END
$reader$;
