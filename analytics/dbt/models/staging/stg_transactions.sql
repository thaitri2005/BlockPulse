-- One screened transaction per capture session, typed and renamed. A txid seen in several sessions appears
-- once per session here; marts pick the first observation.
select
    txid,
    session,
    event_id as source_event_id,
    received_at,
    dt,
    cast(hour as integer) as hour,  -- partition values arrive as text
    feature_set_version,
    ruleset_version,
    n_in,
    n_out,
    value_out_sat,
    fee_sat,
    vsize,
    fee_rate_sat_vb,
    max_equal_output_count,
    round_amount_fraction,
    rbf_signaled,
    is_flagged,
    signal_count,
    rule_ids
from {{ source('lake', 'transactions') }}
