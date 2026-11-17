-- Row origin of a qualified window. Additive only; no earlier migration is edited.
--
-- ``origin`` says where the observations behind a whole-window qualification came from:
-- 'live' (read from the live source by the production monitor), 'recorded' (a recorded replay)
-- or 'synthetic' (test fixtures). NULL means the origin was not recorded (rows written before
-- this migration); readers must show it as unrecorded, never as live. A synthetic
-- qualification can never be labelled live or recorded.
ALTER TABLE action_window_qualification ADD COLUMN IF NOT EXISTS origin text;

DO $origin$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'action_window_qualification_origin_check'
                     AND conrelid = 'action_window_qualification'::regclass) THEN
        ALTER TABLE action_window_qualification
            ADD CONSTRAINT action_window_qualification_origin_check CHECK (
                (origin IS NULL OR origin IN ('live', 'recorded', 'synthetic'))
                AND NOT (synthetic AND origin IN ('live', 'recorded'))
                AND NOT (NOT synthetic AND origin = 'synthetic'));
    END IF;
END
$origin$;
