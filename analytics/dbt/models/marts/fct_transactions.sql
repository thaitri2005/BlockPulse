-- One row per transaction: its first screened observation, plus its lifecycle. A flag is a structural rule
-- match, not evidence of wrongdoing.
with first_observation as (
    select *
    from {{ ref('stg_transactions') }}
    qualify row_number() over (partition by txid order by received_at, session) = 1
),

sessions_seen as (
    select txid, count(*) as sessions_observed
    from {{ ref('stg_transactions') }}
    group by txid
)

select
    t.txid,
    t.session as first_session,
    t.received_at as first_received_at,
    t.dt,
    t.hour,
    t.n_in,
    t.n_out,
    t.value_out_sat,
    t.fee_sat,
    t.vsize,
    t.fee_rate_sat_vb,
    t.max_equal_output_count,
    t.round_amount_fraction,
    t.rbf_signaled,
    t.is_flagged,
    t.signal_count,
    t.rule_ids,
    t.feature_set_version,
    t.ruleset_version,
    s.sessions_observed,
    coalesce(l.lifecycle_status, 'observed_unconfirmed') as lifecycle_status,
    coalesce(l.was_mined, false) as was_mined,
    coalesce(l.was_replaced, false) as was_replaced,
    l.last_event_at
from first_observation as t
join sessions_seen as s using (txid)
left join {{ ref('int_transaction_lifecycle') }} as l using (txid)
