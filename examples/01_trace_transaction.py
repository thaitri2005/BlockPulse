"""Follow one transaction: public API -> JSON -> features -> local files.

Run live:    python examples/01_trace_transaction.py
Run sample:  python examples/01_trace_transaction.py --sample
Run offline: python examples/01_trace_transaction.py --replay
Only Python's standard library is needed.
"""

import argparse
import json
from pathlib import Path
import sys
from urllib.request import Request, urlopen

# Keep this standalone exercise runnable without an editable installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from blockpulse.features import FEATURE_SET_VERSION, extract_features

API_BASE = "https://mempool.space/api"
DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "learning"
RAW_PATH = DATA_DIR / "transaction.json"
FEATURES_PATH = DATA_DIR / "features.json"
SAMPLE_PATH = Path(__file__).resolve().parent / "fixtures" / "transaction.json"


def fetch_json(url):
    """Make one HTTP GET request and decode its JSON response."""
    print(f"  GET {url}")
    request = Request(url, headers={"User-Agent": "BlockPulse-learning/0.1"})
    with urlopen(request, timeout=20) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--replay", action="store_true", help="Read the saved transaction offline."
    )
    mode.add_argument(
        "--sample", action="store_true", help="Use the bundled synthetic example offline."
    )
    args = parser.parse_args()

    if args.sample:
        print("[1/4] Read the bundled SYNTHETIC sample (not a real transaction).")
        transaction = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
        print("[2/4] Use its illustrative transaction fields; no HTTP requests.")
    elif args.replay:
        print("[1/4] Read the saved transaction (no HTTP requests).")
        transaction = json.loads(RAW_PATH.read_text(encoding="utf-8"))
        print("[2/4] Use the transaction details already in that file.")
    else:
        print("[1/4] Ask the source for recent mempool transaction summaries.")
        recent = fetch_json(f"{API_BASE}/mempool/recent")
        if not recent:
            raise ValueError("No recent transactions returned. Try again later.")
        txid = recent[0]["txid"]

        print("[2/4] Fetch the full JSON details for one transaction ID.")
        transaction = fetch_json(f"{API_BASE}/tx/{txid}")
        if transaction["txid"] != txid:
            raise ValueError("The returned transaction ID does not match the request.")

    if "_example_note" in transaction:
        print(f"  Sample note: {transaction['_example_note']}")
    print("[3/4] Calculate features using the same function in every mode.")
    result = {
        "txid": transaction["txid"],
        "feature_set_version": FEATURE_SET_VERSION,
        "features": extract_features(transaction),
    }
    print(json.dumps(result, indent=2))
    print(f"  Confirmed in this saved response: {transaction['status']['confirmed']}")

    print("[4/4] Save the data so we can inspect and replay it.")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not args.replay:
        RAW_PATH.write_text(json.dumps(transaction, indent=2) + "\n", encoding="utf-8")
    FEATURES_PATH.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(f"  Source JSON: {RAW_PATH}")
    print(f"  Features:    {FEATURES_PATH}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise SystemExit(f"Could not finish the exercise: {error}") from error
