-- Latest observed lifecycle state per transaction (latest event wins; at the same time mined > replaced > removed > added).
WITH ranked AS (
    SELECT
        txid,
        event_type,
        ROW_NUMBER() OVER (
            PARTITION BY txid
            ORDER BY event_time DESC,
                CASE event_type WHEN 'mined' THEN 4 WHEN 'replaced' THEN 3 WHEN 'removed' THEN 2 ELSE 1 END DESC,
                event_id
        ) AS position
    FROM lifecycle_events
)
SELECT
    event_type AS latest_state,
    COUNT(*) AS transactions
FROM ranked
WHERE position = 1
GROUP BY event_type
ORDER BY transactions DESC, latest_state
