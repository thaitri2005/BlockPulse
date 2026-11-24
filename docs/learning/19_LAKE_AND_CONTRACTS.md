# Exercise 19: lake layout and data contracts

The goal is to understand where analytics data lives after processing, why it is laid out in folders named `dt=...` and `hour=...`, and what happens to a row that does not meet the rules. You will build a small lake from generated captures, break one row on purpose, read the function that refuses it, and then add a rule of your own.

A **data lake** is a folder tree of data files that query engines read in place. **Parquet** is a file format that stores data column by column with types, unlike **JSONL**, which stores one JSON object per line as text. A **data contract** is a written, checked statement of what a table's rows must look like.

## Run it

Exercise 18 left one generated session in a scratch folder. Add a second and process it. From PowerShell in the project folder:

```powershell
$py = ".\.venv\Scripts\python.exe"
$root = "$env:TEMP\bp-learn"
& $py -m blockpulse synth-capture --output "$root\raw\session-synth-20260101T010000Z" --messages 2000 --seed 2
& $py -m blockpulse process-pending --root $root
```

If the scratch folder is gone, run exercise 18's `synth-capture` line first. Both captures are generated and labelled `source_mode: synthetic`. Everything below shows mechanics only; the recorded results are in the [lake guide](../guides/05-lake-and-athena.md).

## Open the output

List the files under `$root\lake` (`Get-ChildItem $root\lake -Recurse -File`):

```text
lake/_quality/session-synth-20260101T000000Z.json
lake/capture_sessions/dt=2026-01-01/session-synth-20260101T000000Z.parquet
lake/lifecycle_events/dt=2026-01-01/hour=00/session-synth-20260101T000000Z.parquet
lake/transactions/dt=2026-01-01/hour=00/session-synth-20260101T000000Z.parquet
```

(and the same four for the second session). Three things to notice.

**The folder names carry data.** `dt=2026-01-01/hour=00` is a **Hive partition**: a column value written into the path. An engine asked for one day can skip every other folder without opening a file, which is called **partition pruning**. The generator starts every capture at midnight on 2026-01-01, so both sessions land in the same hour.

**One file per session per partition.** Two sessions made two files in that hour, not 16,000. Writing one tiny file per message is the **small files** problem: engines spend more time opening files than reading data.

**The size.** For the first session, `screened.jsonl` is 5,241,337 bytes and the `transactions` Parquet file holding the same 8,000 rows is 697,262 bytes.

Now open `lake\_quality\session-synth-20260101T000000Z.json`. Each table has an entry like this one:

```json
"transactions": {
  "failed_checks": {},
  "partitions": [
    "dt=2026-01-01/hour=00"
  ],
  "rows_in": 8000,
  "rows_quarantined": 0,
  "rows_written": 8000
}
```

Every row passed. To see the contract do something, you need a row that breaks it.

## Make one bad row

Work on a copy, so the scratch lake stays clean. Copy the first session's three stage folders, without the lake, and save this as `$env:TEMP\bp-learn\break_one_row.py`:

```python
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
lines = path.read_text(encoding="utf-8").splitlines()
row = json.loads(lines[0])
print("txid", row["txid"][:16], "fee_sat was", row["features"]["fee_sat"])
row["features"]["fee_sat"] = -1          # an amount can never be negative
lines[0] = json.dumps(row)
path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
```

```powershell
$s = "session-synth-20260101T000000Z"
$bad = "$env:TEMP\bp-learn-bad"
foreach ($d in "raw", "processed", "screened") { Copy-Item "$root\$d\$s" "$bad\$d\$s" -Recurse }
& $py "$root\break_one_row.py" "$bad\screened\$s\screened.jsonl"
& $py -m blockpulse process-pending --root $bad
```

The script prints `txid 5d0f2f6e57dfbdb2 fee_sat was 21187` and changes that one value. `process-pending` finds the screened output present and only the lake missing, so it runs just that stage and reports `"quarantined": {"transactions": 1, ...}`. The copy's quality report now says:

```json
"failed_checks": {
  "check:amounts_nonnegative": 1
},
...
"rows_in": 8000,
"rows_quarantined": 1,
"rows_written": 7999
```

and `lake\_quarantine\transactions\session-synth-20260101T000000Z.jsonl` holds the refused row with `"errors": ["check: amounts_nonnegative"]`. **Quarantine** means setting a bad row aside with its reasons, as opposed to writing it anyway or quietly repairing it.

## Read one function

Open [`src/blockpulse/lake.py`](../../src/blockpulse/lake.py) and follow `fee_sat` for that row.

`transaction_rows()` flattens the screened record, so `features.fee_sat` becomes the column `fee_sat` with the value `-1`. `validate()` first walks the table's declared columns:

```python
for column in table.columns:
    try:
        typed[column.name] = _coerce(column, row.get(column.name))
    except ValueError as error:
        errors.append(f"type: {error}")
        failures[f"type:{column.name}"] += 1
```

`-1` is an integer and the column is a `bigint`, so the type pass accepts it. (A missing value would stop here: `_coerce()` raises for any column not declared `nullable=True`.) Then come the row checks in `ROW_CHECKS`:

```python
("amounts_nonnegative", lambda r: r["value_out_sat"] >= 0 and r["fee_sat"] >= 0 and r["fee_rate_sat_vb"] >= 0),
```

This one returns `False`, so `check: amounts_nonnegative` is appended to the row's errors and counted. Errors are collected, not raised, so one bad row never stops the rest. A row with any error goes to `bad`; the others go to `good`.

Notice what `validate()` does not do. It does not clamp `-1` to `0` or substitute a default for a missing value. A repaired value would look like a real measurement in every later query.

Then read `build_session()`: good rows are grouped by `_partition()`, sorted and written, and the quality report is written last, which is how `process-pending` knows the stage finished.

## Change one small thing

First see that a rebuild changes nothing. Back in the clean scratch root:

```powershell
$file = "$root\lake\transactions\dt=2026-01-01\hour=00\session-synth-20260101T000000Z.parquet"
(Get-FileHash $file).Hash
Remove-Item "$root\lake\_quality\session-synth-20260101T000000Z.json"
& $py -m blockpulse process-pending --root $root
(Get-FileHash $file).Hash
```

The two hashes are equal. The file name comes from the session and the rows are sorted, so the same input gives the same bytes: the stage is idempotent, and re-running old sessions (a **backfill**) is safe.

Now add a rule of your own. In `ROW_CHECKS["transactions"]`, add:

```python
("fee_rate_plausible", lambda r: r["fee_rate_sat_vb"] <= 15),
```

Delete the same quality report and run `process-pending` again. On this generated session the report shows `rows_written: 5633`, `rows_quarantined: 2367`, and `"check:fee_rate_plausible": 2367`. The threshold is arbitrary (generated fee rates run from 1 to 21 sat/vB).

Remove the line, delete the report once more, rerun `process-pending` so the scratch lake is whole again, and run:

```powershell
& $py -m pytest -q tests/test_lake.py
```

A row check does not change the columns, so `lake_schema.json` stays as it is. You can delete `$env:TEMP\bp-learn-bad` when you are done.

## Question to carry forward

On the 94 recorded sessions the gate quarantined 0 of 8,499,067 rows. Does that mean the gate is unnecessary? What would `_quarantine/` have told you, and how soon, if a change to `features.py` had started producing a negative fee?

## What was verified

Run on 2026-11-24 on Linux with a fresh scratch root, after `synth.py` began writing a full capture summary: both `synth-capture` commands, `process-pending` (0 rows quarantined in all three tables), the file listing, sizes and quality report quoted above, `break_one_row.py` on a copy followed by `process-pending` (1 row quarantined for `amounts_nonnegative`, 7,999 written), the rebuild with identical SHA-256 before and after, and `pytest tests/test_lake.py`. The `fee_rate_plausible` counts come from adding the check in memory and calling `build_session()` on a copy of the scratch root, not from editing `lake.py`. The PowerShell forms (`Copy-Item`, `Get-FileHash`, `Remove-Item`) were not run; their Linux equivalents were.
