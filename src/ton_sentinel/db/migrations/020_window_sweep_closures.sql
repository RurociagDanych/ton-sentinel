-- Which canonical inputs each swept window was last closed on. Written only inside the
-- repair promotion transaction (FK to repair_promotions), so a row proves a canonical repair.
CREATE TABLE IF NOT EXISTS window_sweep_closures (
    network text NOT NULL CHECK (network IN ('mainnet', 'testnet')),
    window_start bigint NOT NULL CHECK (window_start >= 0 AND window_start % 300 = 0),
    reason text NOT NULL CHECK (reason IN ('dirty', 'late', 'evicted', 'idle')),
    fingerprint text NOT NULL CHECK (fingerprint ~ '^sha256:[0-9a-f]{64}$'),
    work_id uuid NOT NULL REFERENCES repair_promotions(work_id),
    closed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (network, window_start)
);
