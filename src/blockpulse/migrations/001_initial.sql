-- M2.1: first query-store tables. Plain SQL; applied once, in filename order, by `blockpulse db-init`.

-- One row per transaction we have observed. Lifecycle fields are evidence-based and may be revised.
CREATE TABLE transactions (
    txid             text PRIMARY KEY,
    first_seen_at    timestamptz NOT NULL,
    last_seen_at     timestamptz NOT NULL,
    lifecycle_status text NOT NULL DEFAULT 'observed_unconfirmed'
        CHECK (lifecycle_status IN ('observed_unconfirmed', 'confirmed', 'superseded', 'removed_unknown_reason', 'unknown'))
);

-- One row per (run, transaction, feature version, rule version). The UNIQUE constraint is what makes
-- re-loading the same results safe: a retry hits the constraint instead of adding a duplicate row.
CREATE TABLE scores (
    id                   bigserial PRIMARY KEY,
    run_id               text NOT NULL,
    txid                 text NOT NULL,
    feature_set_version  text NOT NULL,
    ruleset_version      text NOT NULL,
    features             jsonb NOT NULL,
    signals              jsonb NOT NULL,
    signal_count         integer NOT NULL CHECK (signal_count >= 0),
    is_flagged           boolean NOT NULL,
    source_event_id      text NOT NULL,
    received_at          timestamptz NOT NULL,
    created_at           timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, txid, feature_set_version, ruleset_version)
);

-- No foreign key to transactions on purpose: scores must stay inspectable if transaction rows expire.
CREATE INDEX scores_txid_idx ON scores (txid);
CREATE INDEX scores_received_at_idx ON scores (received_at DESC);
CREATE INDEX scores_flagged_received_idx ON scores (received_at DESC) WHERE is_flagged;
