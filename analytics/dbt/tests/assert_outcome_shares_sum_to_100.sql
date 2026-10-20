select is_flagged, sum(pct_within_flag_group) as total_pct
from {{ ref('fct_outcomes_by_flag') }}
group by is_flagged
having abs(sum(pct_within_flag_group) - 100) > 0.1
