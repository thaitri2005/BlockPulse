-- Transactions, flags, fees and lifecycle events per UTC hour (by receive time). Counts every screened
-- observation, so it matches analytics/queries/hourly_flag_rate.sql exactly.
--
-- Normally a table, rebuilt in full. With `--vars '{dts: ["2026-10-13"]}'` it becomes incremental: only the named
-- UTC days are recomputed and replaced (delete+insert on dt), and every other day is left as it is. That is safe
-- for this model alone, because an hour's row depends only on that hour's rows. The other marts need a
-- transaction's whole history, which crosses days, so they are always rebuilt.
{% set days = var('dts', []) %}
{{ config(
    materialized=('incremental' if days else 'table'),
    incremental_strategy='delete+insert',
    unique_key='dt',
    on_schema_change='fail'
) }}
{% set only_days %}
    {% if days and is_incremental() %} where dt in ({% for day in days %}'{{ day }}'{{ ", " if not loop.last }}{% endfor %}) {% endif %}
{% endset %}

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
    {{ only_days }}
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
    {{ only_days }}
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
