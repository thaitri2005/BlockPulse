"""Small, explainable transaction-structure screening rules."""

from dataclasses import asdict, dataclass
from math import isfinite
from numbers import Real

RULESET_VERSION = "structural-rules-v1"


@dataclass(frozen=True)
class DetectorConfig:
    many_outputs_min: int = 10
    many_outputs_max_inputs: int = 2
    many_inputs_min: int = 10
    many_inputs_max_outputs: int = 2
    repeated_value_min: int = 3
    repeated_value_min_inputs: int = 3

    def __post_init__(self):
        for name, value in asdict(self).items():
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")


def _integer(row: dict, name: str) -> int:
    value = row.get(name)
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    return value


def _validate_features(row: dict) -> None:
    if not isinstance(row, dict):
        raise ValueError("Feature row must be an object")
    for name in ("n_in", "n_out", "value_out_sat", "fee_sat", "vsize", "max_equal_output_count"):
        _integer(row, name)
    for name in ("fee_rate_sat_vb", "round_amount_fraction"):
        value = row.get(name)
        if isinstance(value, bool) or not isinstance(value, Real) or not isfinite(value) or value < 0:
            raise ValueError(f"{name} must be a finite nonnegative number")
    if row["n_in"] < 1 or row["n_out"] < 1 or row["vsize"] < 1:
        raise ValueError("n_in, n_out, and vsize must be positive")
    if row["max_equal_output_count"] > row["n_out"] or row["round_amount_fraction"] > 1:
        raise ValueError("Feature values are outside their valid ranges")
    rbf_signaled = row.get("rbf_signaled")
    if rbf_signaled is not True and rbf_signaled is not False and rbf_signaled is not None:
        raise ValueError("rbf_signaled must be true, false, or unknown")


def screen_transaction(features: dict, config: DetectorConfig | None = None) -> dict:
    """Flag documented shapes. Signals describe structure, not intent."""
    config = config or DetectorConfig()
    _validate_features(features)
    signals = []
    n_in, n_out = features["n_in"], features["n_out"]
    repeated = features["max_equal_output_count"]
    if n_in <= config.many_outputs_max_inputs and n_out >= config.many_outputs_min:
        signals.append({
            "rule_id": "many_outputs",
            "explanation": f"At most {config.many_outputs_max_inputs} input(s) fund {n_out} outputs; the rule threshold is {config.many_outputs_min} outputs.",
        })
    if n_in >= config.many_inputs_min and n_out <= config.many_inputs_max_outputs:
        signals.append({
            "rule_id": "many_inputs",
            "explanation": f"{n_in} inputs fund at most {n_out} outputs; the rule threshold is {config.many_inputs_min} inputs.",
        })
    if n_in >= config.repeated_value_min_inputs and repeated >= config.repeated_value_min:
        signals.append({
            "rule_id": "repeated_output_values",
            "explanation": f"{repeated} positive-value outputs share one amount and the transaction has at least {config.repeated_value_min_inputs} inputs; this can occur in benign transactions and is only a structural heuristic.",
        })
    return {
        "ruleset_version": RULESET_VERSION,
        "signal_count": len(signals),
        "is_flagged": bool(signals),
        "signals": signals,
        "interpretation": "Unusual transaction structure is not evidence of wrongdoing.",
    }
