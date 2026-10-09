"""Deterministic transaction features shared by examples and offline processing."""

FEATURE_SET_VERSION = "transaction-v2"
FEATURE_NAMES = (
    "n_in", "n_out", "value_out_sat", "fee_sat", "vsize", "fee_rate_sat_vb",
    "max_equal_output_count", "round_amount_fraction", "rbf_signaled",
)
ROUND_AMOUNT_UNIT_SAT = 100_000
RBF_SEQUENCE_THRESHOLD = 0xFFFFFFFE


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
    positive_output_values = [output["value"] for output in transaction["vout"] if output["value"] > 0]
    equal_value_counts = {}
    for value in positive_output_values:
        equal_value_counts[value] = equal_value_counts.get(value, 0) + 1
    max_equal_output_count = max(equal_value_counts.values(), default=0)
    round_amount_fraction = (
        sum(value % ROUND_AMOUNT_UNIT_SAT == 0 for value in positive_output_values)
        / len(positive_output_values)
        if positive_output_values else 0.0
    )
    sequences = [item.get("sequence") for item in transaction["vin"]]
    valid_sequences = [type(value) is int and 0 <= value <= 0xFFFFFFFF for value in sequences]
    if any(valid and value < RBF_SEQUENCE_THRESHOLD for value, valid in zip(sequences, valid_sequences)):
        rbf_signaled = True
    elif all(valid_sequences):
        rbf_signaled = False
    else:
        rbf_signaled = None
    return {
        "n_in": len(transaction["vin"]),
        "n_out": len(transaction["vout"]),
        "value_out_sat": value_out,
        "fee_sat": fee_sat,
        "vsize": vsize,
        "fee_rate_sat_vb": round(fee_sat / vsize, 6),
        "max_equal_output_count": max_equal_output_count,
        "round_amount_fraction": round(round_amount_fraction, 6),
        "rbf_signaled": rbf_signaled,
    }
