# Exercise 21: modelling with dbt

The goal is to understand how a pile of SQL files becomes an ordered, tested set of tables, and why the order does not have to be written down anywhere. You will build the dbt project on the scratch lake from exercise 19, ask dbt what one table depends on, follow one column through three layers, and add a test.

**dbt** is a tool that runs SQL `select` statements in dependency order and stores each result as a table or view. Each `.sql` file is a **model**. A **source** is data dbt reads but did not create (here, the lake's Parquet files). The local **warehouse**, the place the results are stored, is a single DuckDB file.

## Run it

From PowerShell in the project folder, using the scratch root (two generated sessions, labelled synthetic):

```powershell
$py = ".\.venv\Scripts\python.exe"
$root = "$env:TEMP\bp-learn"
& $py -m blockpulse dbt --root $root build
```

`blockpulse dbt` is a thin wrapper ([`warehouse.py`](../../src/blockpulse/warehouse.py)): it passes its arguments to dbt and points the project at `$root\lake`, writing the warehouse to `$root\dbt\blockpulse.duckdb`. `build` means: create every model and run every test, in order.

## Open the output

The last line of the build:

```text
Done. PASS=36 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=36
```

That is 8 models and 28 tests, the same counts as on the recorded lake ([dbt guide](../guides/11-dbt.md)). Scroll up through the log and notice the order: the three staging views first, then tests on them, then tables that read them, then tests on those. Nobody wrote that order down. dbt worked it out from the **lineage**: which model reads which. Ask it for everything one mart depends on (the leading `+` means "and its ancestors"):

```powershell
& $py -m blockpulse dbt --root $root ls --select +fct_outcomes_by_flag --resource-type model
```

```text
blockpulse.marts.fct_outcomes_by_flag
blockpulse.marts.fct_transactions
blockpulse.intermediate.int_transaction_lifecycle
blockpulse.staging.stg_lifecycle_events
blockpulse.staging.stg_transactions
```

The same graph decides what happens on a failure: if a model fails, everything downstream of it is skipped and everything else still runs. Now look at a result:

```powershell
& $py -m blockpulse dbt --root $root show --select fct_outcomes_by_flag
```

```text
| is_flagged | lifecycle_status     | transactions | pct_within_flag_g... |
| ---------- | -------------------- | ------------ | -------------------- |
|       True | observed_unconfirmed |         2505 |                  100 |
|      False | observed_unconfirmed |        13495 |                  100 |
```

Every generated transaction is `observed_unconfirmed`, because generated captures contain only `added` events. This table shows that the models ran; it says nothing about how real transactions end up. A flag is a structural rule match, not a judgement.

## Read one model chain

Follow `is_flagged` through the folders under [`analytics/dbt/models/`](../../analytics/dbt/models).

1. **Source.** `staging/_sources.yml` declares `lake.transactions` as a `read_parquet(...)` call over the lake folder. Nothing is copied.
2. **Staging** (`staging/stg_transactions.sql`): one model per source that only renames and fixes types. It ends with `from {{ source('lake', 'transactions') }}`. `is_flagged` passes through unchanged; `hour` is cast from text to integer here, once, so no later model has to.
3. **Mart** (`marts/fct_transactions.sql`): a table shaped for questions. A transaction seen in two sessions appears twice in staging, and this model keeps the first:

   ```sql
   with first_observation as (
       select *
       from {{ ref('stg_transactions') }}
       qualify row_number() over (partition by txid order by received_at, session) = 1
   ),
   ```

4. **Mart on a mart** (`marts/fct_outcomes_by_flag.sql`): groups `{{ ref('fct_transactions') }}` by `is_flagged` and `lifecycle_status`.

`{{ ref('...') }}` is the whole mechanism. dbt replaces it with the real table name and, from the same text, learns that this model must be built after that one. You never write the order. An **intermediate** model (`intermediate/int_transaction_lifecycle.sql`) sits between staging and marts for logic several marts share: here, each transaction's latest lifecycle status.

Two kinds of test guard the chain. **Schema tests** are declared in YAML next to the model (`marts/_marts.yml`: `txid` is `unique` and `not_null`). **Data tests** are SQL files in `tests/` that must return zero rows. Open `tests/assert_flag_matches_signals.sql`:

```sql
select * from {{ ref('fct_transactions') }}
where is_flagged <> (signal_count > 0) or signal_count <> len(rule_ids)
```

You have seen this rule before: it is `flag_matches_signals` in the lake's `ROW_CHECKS`. The same is true of the lifecycle ordering (mined beats replaced beats removed beats added), which is written in `int_transaction_lifecycle.sql`, in `analytics/queries/lifecycle_outcomes.sql`, and in `lifecycle.py` for Postgres. A rule stated in three places can drift three ways. The project does not remove the copies; it adds tests that compare them, such as `tests/test_warehouse.py`.

## Change one small thing

Add a data test. Create `analytics/dbt/tests/assert_fees_nonnegative.sql`:

```sql
-- Amounts are integer satoshis and never negative (the lake contract's amounts_nonnegative, checked downstream).
select * from {{ ref('fct_transactions') }}
where fee_sat < 0 or fee_rate_sat_vb < 0
```

```powershell
& $py -m blockpulse dbt --root $root test --select assert_fees_nonnegative
& $py -m pytest -q tests/test_warehouse.py
```

Then flip `<` to `>=` in your test, run it again to see what a failing data test looks like, and flip it back. Decide whether to keep the file: it is a fourth copy of a rule the lake already enforces.

## Question to carry forward

Staging models are views and marts are tables (`dbt_project.yml`). A view re-reads the Parquet files every time it is queried; a table is a stored copy. One mart, `fct_hourly_activity`, can also be refreshed for named days only (`dbt run --select fct_hourly_activity --vars '{dts: [...]}'`, see "Refreshing only some days" in the dbt guide). After `process-pending` adds a new session to the lake, which models already show it and which are stale until the next `dbt build`? What would you want a scheduler to do about that (exercise 24)?

## What was verified

Run on 2026-11-24 on Linux against a fresh scratch lake, after `synth.py` began writing a full capture summary: plain `dbt build` (PASS=36, no errors or skips), `dbt ls --select +fct_outcomes_by_flag --resource-type model`, `dbt show --select fct_outcomes_by_flag`, `dbt run --select fct_hourly_activity --vars '{dts: ["2026-01-01"]}'` (1 incremental model, passed; run with bash quoting, the PowerShell quoting in the dbt guide was not tried), and `pytest tests/test_warehouse.py`. The new test file was not created; its query was run with `dbt show --inline` against `fct_transactions` and returned no rows, which is what a passing data test needs. The `>=` variant was not run, and no model was made to fail, so the skip-on-failure sentence was not re-checked in this run. It was observed earlier the same day, before the generator fix, when one failing staging view caused exactly its six dependants to be skipped.
