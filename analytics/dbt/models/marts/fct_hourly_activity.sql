-- Transactions, flags, fees and lifecycle events per UTC hour (by receive time). Counts every screened
-- observation, so it matches analytics/queries/hourly_flag_rate.sql exactly.
with screened as (
    select
        dt,
        hour,
        count(*) as transactions,
        count(*) filter (where is_flagged) as flagged,
        median(fee_rate_sat_vb) as median_fee_rate_sat_vb,
        quantile_cont(fee_rate_sat_vb, 0.9) as p90_fee_rate_sat_vb,
        sum(value_out_sat) as value_out_sat
    from {{ ref('stg_transactions') }}
    group by dt, hour
),

events as (
    select
        dt,
        hour,
        count(*) filter (where event_type = 'added') as events_added,
        count(*) filter (where event_type = 'removed') as events_removed,
        count(*) filter (where event_type = 'mined') as events_mined,
        count(*) filter (where event_type = 'replaced') as events_replaced
    from {{ ref('stg_lifecycle_events') }}
    group by dt, hour
)

select
    s.dt,
    s.hour,
    s.transactions,
    s.flagged,
    round(100.0 * s.flagged / s.transactions, 2) as flag_rate_pct,
    s.median_fee_rate_sat_vb,
    s.p90_fee_rate_sat_vb,
    s.value_out_sat,
    coalesce(e.events_added, 0) as events_added,
    coalesce(e.events_removed, 0) as events_removed,
    coalesce(e.events_mined, 0) as events_mined,
    coalesce(e.events_replaced, 0) as events_replaced
from screened as s
left join events as e using (dt, hour)
