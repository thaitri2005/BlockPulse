-- Transactions and structural-rule flags per hour. A flag is a rule match, not evidence of wrongdoing.
SELECT
    dt,
    hour,
    COUNT(*) AS transactions,
    SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END) AS flagged,
    ROUND(100.0 * SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END) / COUNT(*), 2) AS flag_rate_pct
FROM transactions
GROUP BY dt, hour
ORDER BY dt, hour
