-- M2.4: lifecycle evidence. Every observation we recorded about a transaction, kept as received.
-- transactions.lifecycle_status is a projection computed from these rows; it can always be rebuilt.

CREATE TABLE lifecycle_events (
    event_id          text PRIMARY KEY,
    run_id            text NOT NULL,
    txid              text NOT NULL,
    event_type        text NOT NULL CHECK (event_type IN ('added', 'removed', 'mined', 'replaced')),
    event_time        timestamptz NOT NULL,
    source_message_id text NOT NULL,
    source_sequence   bigint,
    replacement_txid  text,
    created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX lifecycle_events_txid_idx ON lifecycle_events (txid, event_time);
