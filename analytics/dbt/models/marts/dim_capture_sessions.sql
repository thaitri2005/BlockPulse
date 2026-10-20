-- One row per capture session, with how much of it was actually observed. Recorded gaps are a lower bound
-- on missing coverage.
select
    c.*,
    case when c.elapsed_seconds > 0 then round(100.0 * c.recorded_gap_seconds / c.elapsed_seconds, 2) end as gap_pct,
    starts_with(c.session, 'session-aws-') as collected_on_aws,
    coalesce(t.transactions, 0) as transactions,
    coalesce(t.flagged, 0) as flagged
from {{ ref('stg_capture_sessions') }} as c
left join (
    select session, count(*) as transactions, count(*) filter (where is_flagged) as flagged
    from {{ ref('stg_transactions') }}
    group by session
) as t using (session)
