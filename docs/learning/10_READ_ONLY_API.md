# Exercise 10: a read-only API and a dashboard

The goal is to understand how stored results are served without letting a reader change or overload the database. You will page through alerts with a cursor, see bad input rejected before any query runs, and read one dashboard panel as plain SQL. The API holds no feature or model logic: it only reads what the loaders stored.

## Run it

Part of this works with no database at all. From the project folder:

```powershell
$py = ".\.venv\Scripts\python.exe"
& $py -m pytest -q tests/test_api.py
& $py -c "from fastapi.testclient import TestClient; from blockpulse.api import create_app; c = TestClient(create_app('postgresql://nobody@127.0.0.1:1/none')); print(c.get('/healthz').status_code, c.get('/alerts?limit=0').status_code, c.get('/alerts?limit=500').status_code, c.get('/alerts?cursor=abc').status_code, c.get('/tx/not-a-txid').status_code, c.get('/readyz').status_code, c.get('/alerts').status_code)"
```

The second command builds the app against a database address that cannot answer, and calls it in-process (`TestClient` runs the app without opening a port).

For real rows you need Postgres. Prerequisite, from [guide 04](../guides/04-postgres-api-dashboard.md): `pip install -e ".[m2,dev]"` and `docker compose --profile m2 up -d --wait`. Then load the offline demo and start the API:

```powershell
powershell -File scripts/demo_m2.ps1
$env:BLOCKPULSE_DSN = "postgresql://blockpulse:blockpulse_dev@127.0.0.1:5432/blockpulse_demo"
& $py -m blockpulse serve        # http://127.0.0.1:8000/docs
```

The fixture produces no flagged rows, so `/alerts` on the demo database returns an empty list. To see pages, either point `BLOCKPULSE_DSN` at your real database, or set `BLOCKPULSE_TEST_DSN` as guide 04 describes and run `pytest -q tests/test_api.py`: its fixture loads five flagged rows and walks three pages.

## Open the output

Without a database, the test file reports:

```text
2 passed, 4 skipped, 1 warning in 0.39s
```

and the one-liner prints:

```text
200 422 422 422 422 503 503
```

Read those seven numbers against the seven requests. `/healthz` answers 200: the process is up. Four requests are rejected with 422, the HTTP status for "your input is not valid": `limit=0`, `limit=500`, a cursor that does not decode, and a txid that is not 64 hex characters. The last two, `/readyz` and a valid `/alerts`, answer 503 ("service unavailable") because only they reached for the database.

That ordering is the point. The body of the invalid-limit response is:

```text
{'detail': [{'type': 'greater_than_equal', 'loc': ['query', 'limit'], 'msg': 'Input should be greater than or equal to 1', 'input': '0', 'ctx': {'ge': 1}}]}
```

No connection was attempted for it. `LazyConnection` in [api.py](../../src/blockpulse/api.py) opens the connection on the first query, so validation always happens first. It also sets `read_only = True`: a **read-only connection** is one where Postgres itself refuses any statement that writes, so a bug in the API cannot damage the data.

## Read one function

Open `list_alerts()` in `api.py` and trace the `cursor` parameter.

**Keyset pagination** means asking for "the rows after this exact row" instead of "skip N rows". The **cursor** is the position of the last row you saw, handed back to the server. Here it is the pair `(received_at, id)`, packed into text by `encode_cursor()`:

```powershell
& $py -c "from datetime import datetime, timezone; from blockpulse.api import encode_cursor, decode_cursor; c = encode_cursor(datetime(2026, 10, 9, 2, 40, 4, tzinfo=timezone.utc), 4); print(c); print(decode_cursor(c))"
```

```text
WyIyMDI2LTEwLTA5VDAyOjQwOjA0KzAwOjAwIiwgNF0=
(datetime.datetime(2026, 10, 9, 2, 40, 4, tzinfo=datetime.timezone.utc), 4)
```

The cursor is base64 of a small JSON list, not a secret. Follow it through the function:

```python
if cursor is not None:
    cursor_time, cursor_id = decode_cursor(cursor)
    clauses.append("(received_at, id) < (%s, %s)")
    params += [cursor_time, cursor_id]
rows = conn.execute(
    f"SELECT {SCORE_COLUMNS} FROM scores WHERE {' AND '.join(clauses)} "
    "ORDER BY received_at DESC, id DESC LIMIT %s", (*params, limit + 1),
).fetchall()
```

Three decisions to notice:

- The comparison `(received_at, id) < (...)` uses the same two columns as the `ORDER BY`. `id` breaks ties between rows with the same time, so no row is skipped or repeated.
- It asks for `limit + 1` rows. The extra row is never returned; its presence says another page exists, which is how `next_cursor` becomes `None` on the last page.
- Values travel as parameters (`%s`), never pasted into the SQL text. Only fixed clause strings are joined.

With `OFFSET`, a new alert arriving between two requests shifts every row down by one and you see a duplicate. With a keyset the next page starts from a fixed row, so new arrivals cannot disturb it.

**The dashboard uses the same idea of plain reads.** Open `observability/grafana/dashboards/blockpulse-overview.json` and find the panel "Transactions by lifecycle status":

```sql
SELECT lifecycle_status, count(*) AS transactions FROM transactions
WHERE $__timeFilter(last_seen_at) GROUP BY 1 ORDER BY 2 DESC
```

`$__timeFilter(...)` is a Grafana macro that expands to the time range picked in the dashboard. Everything else is SQL you could run in `psql`. The panel reads the projection from Exercise 9; it computes nothing itself.

## Change one small thing

In `list_alerts()`, change the limit bound from `le=200` to `le=100`. Rerun the one-liner from "Run it" with `/alerts?limit=150` added to the list and confirm it now answers 422, still without a database. Then:

```powershell
& $py -m pytest -q tests/test_api.py
```

The tests should still pass, since none asks for more than 100 rows. Open `http://127.0.0.1:8000/docs` if the API is running and check the documented maximum changed too: the validation rule and the documentation come from the same annotation. Put `le=200` back afterwards, or update the route table in guide 04 if you decide to keep the new limit.

## Question to carry forward

`since` and `detector` are filters; the cursor is a position. What happens if a caller sends a cursor from one query together with a different `detector` value? Is the result wrong, or merely a valid page of a different list? What would the cursor have to contain for the API to notice?

## What was run for this page

- Ran (Linux, 2026-11-24, no database): `pytest -q tests/test_api.py` (2 passed, 4 skipped), the seven-request one-liner, and the cursor round trip. The outputs above are copied from those runs. The dashboard SQL is copied from the JSON file.
- Not run (no Postgres in that session): `scripts/demo_m2.ps1`, `blockpulse serve`, the four database tests, and Grafana. The `le=100` edit was not applied, because that session did not modify existing files; `limit=500` being rejected by the current bound was checked.
