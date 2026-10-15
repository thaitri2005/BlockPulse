-- How often each structural rule fired, across all loaded partitions.
SELECT
    t.rule_id,
    COUNT(*) AS signals
FROM transactions
CROSS JOIN UNNEST(rule_ids) AS t (rule_id)
GROUP BY t.rule_id
ORDER BY signals DESC, t.rule_id
