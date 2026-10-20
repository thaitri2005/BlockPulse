select
    session,
    run_id,
    source_url,
    started_at,
    ended_at,
    elapsed_seconds,
    stop_reason,
    messages_saved,
    connection_errors,
    recorded_gaps,
    recorded_gap_seconds,
    events_added,
    events_removed,
    events_mined,
    events_replaced
from {{ source('lake', 'capture_sessions') }}
