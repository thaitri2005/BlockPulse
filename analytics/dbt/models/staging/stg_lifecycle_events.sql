select
    event_id,
    session,
    run_id,
    txid,
    event_type,
    event_time,
    dt,
    cast(hour as integer) as hour,  -- partition values arrive as text
    source_sequence,
    replacement_txid,
    source_message_id
from {{ source('lake', 'lifecycle_events') }}
