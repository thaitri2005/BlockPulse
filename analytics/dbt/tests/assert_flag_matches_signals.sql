-- The same contract the lake's quality gate enforces: a row is flagged exactly when it has rule signals.
select * from {{ ref('fct_transactions') }}
where is_flagged <> (signal_count > 0) or signal_count <> len(rule_ids)
