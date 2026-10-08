"""Deterministic transaction features shared by examples and offline processing."""

FEATURE_SET_VERSION = "transaction-v1"
FEATURE_NAMES = (
    "n_in", "n_out", "value_out_sat", "fee_sat", "vsize", "fee_rate_sat_vb"
)


def integer(value: object, name: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def extract_features(transaction: dict) -> dict:
    """Validate required fields; never turn missing amounts into zeros."""
    if not isinstance(transaction, dict):
        raise ValueError("Transaction must be an object")
    for name in ("vin", "vout"):
        items = transaction.get(name)
        if not isinstance(items, list) or not items:
            raise ValueError(f"{name} must be a nonempty list")
        if any(not isinstance(item, dict) for item in items):
            raise ValueError(f"Each {name} entry must be an object")
    if any(item.get("is_coinbase") for item in transaction["vin"]):
        raise ValueError("Coinbase transactions are outside the mempool feature set")

    weight = integer(transaction.get("weight"), "weight", minimum=1)
    fee_sat = integer(transaction.get("fee"), "fee")
    vsize = (weight + 3) // 4
    value_out = sum(
        integer(output.get("value"), "output value")
        for output in transaction["vout"]
    )
    return {
        "n_in": len(transaction["vin"]),
        "n_out": len(transaction["vout"]),
        "value_out_sat": value_out,
        "fee_sat": fee_sat,
        "vsize": vsize,
        "fee_rate_sat_vb": round(fee_sat / vsize, 6),
    }
