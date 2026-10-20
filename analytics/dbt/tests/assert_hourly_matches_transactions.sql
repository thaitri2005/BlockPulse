-- Reconciliation: hourly totals add up to every screened observation.
with hourly as (select sum(transactions) as n from {{ ref('fct_hourly_activity') }}),
     staged as (select count(*) as n from {{ ref('stg_transactions') }})
select * from hourly, staged where hourly.n <> staged.n
