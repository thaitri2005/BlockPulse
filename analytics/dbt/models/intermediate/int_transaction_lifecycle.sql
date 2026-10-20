-- Each transaction's lifecycle from all of its evidence. The latest event wins; at the same time
-- mined > replaced > removed > added. This is the same rule Postgres uses (src/blockpulse/lifecycle.py),
-- so the warehouse and the serving store agree.
with ranked as (
    select
        txid,
        event_type,
        event_time,
        row_number() over (
            partition by txid
            order by event_time desc,
                     case event_type when 'mined' then 4 when 'replaced' then 3 when 'removed' then 2 else 1 end desc,
                     event_id
        ) as position
    from {{ ref('stg_lifecycle_events') }}
),

summary as (
    select
        txid,
        min(event_time) as first_event_at,
        max(event_time) as last_event_at,
        count(*) as events,
        count(*) filter (where event_type = 'mined') > 0 as was_mined,
        count(*) filter (where event_type = 'replaced') > 0 as was_replaced
    from {{ ref('stg_lifecycle_events') }}
    group by txid
)

select
    summary.*,
    ranked.event_type as latest_event_type,
    case ranked.event_type
        when 'added' then 'observed_unconfirmed'
        when 'removed' then 'removed_unknown_reason'
        when 'mined' then 'confirmed'
        else 'superseded'
    end as lifecycle_status
from summary
join ranked on ranked.txid = summary.txid and ranked.position = 1
