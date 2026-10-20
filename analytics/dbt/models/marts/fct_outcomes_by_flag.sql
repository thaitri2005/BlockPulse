-- How transactions ended up, split by whether the structural rules flagged them. Describes outcomes only:
-- a different confirmation rate is not evidence about intent.
select
    is_flagged,
    lifecycle_status,
    count(*) as transactions,
    round(100.0 * count(*) / sum(count(*)) over (partition by is_flagged), 2) as pct_within_flag_group
from {{ ref('fct_transactions') }}
group by is_flagged, lifecycle_status
