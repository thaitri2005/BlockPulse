-- A flag rate outside 0-100% would mean the hourly aggregation is double counting.
select * from {{ ref('fct_hourly_activity') }}
where flag_rate_pct < 0 or flag_rate_pct > 100 or flagged > transactions
