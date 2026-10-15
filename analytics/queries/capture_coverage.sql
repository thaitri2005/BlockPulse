-- Per-session capture quality: how long it ran, what it saved, and the recorded gaps (a lower bound on missing data).
SELECT
    session,
    started_at,
    elapsed_seconds,
    stop_reason,
    messages_saved,
    connection_errors,
    recorded_gap_seconds,
    ROUND(100.0 * recorded_gap_seconds / NULLIF(elapsed_seconds, 0), 2) AS gap_pct
FROM capture_sessions
ORDER BY started_at
