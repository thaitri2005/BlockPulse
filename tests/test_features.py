from copy import deepcopy

import pytest

from blockpulse.features import extract_features


def test_known_values_and_virtual_size_ceiling(transaction):
    assert extract_features(transaction) == {
        "n_in": 1, "n_out": 2, "value_out_sat": 100000,
        "fee_sat": 603, "vsize": 201, "fee_rate_sat_vb": 3.0,
        "max_equal_output_count": 1, "round_amount_fraction": 0.0,
        "rbf_signaled": True,
    }


def test_pure_and_independent_of_confirmation(transaction):
    original = deepcopy(transaction)
    features = extract_features(transaction)
    assert transaction == original
    transaction["status"] = {"confirmed": True, "block_height": 999999}
    assert extract_features(transaction) == features


@pytest.mark.parametrize("field,value", [("weight", 0), ("weight", True), ("fee", -1), ("fee", None), ("fee", 3.5), ("vin", []), ("vout", [])])
def test_bad_required_fields_are_not_filled_with_zero(transaction, field, value):
    transaction[field] = value
    with pytest.raises(ValueError):
        extract_features(transaction)


def test_missing_output_value_and_coinbase_are_rejected(transaction):
    transaction["vout"][0].pop("value")
    with pytest.raises(ValueError, match="output value"):
        extract_features(transaction)
    transaction["vin"][0]["is_coinbase"] = True
    with pytest.raises(ValueError, match="Coinbase"):
        extract_features(transaction)


def test_structural_features_and_unknown_rbf(transaction):
    transaction["vout"] = [{"value": 100000}, {"value": 100000}, {"value": 125000}, {"value": 0}]
    features = extract_features(transaction)
    assert features["max_equal_output_count"] == 2
    assert features["round_amount_fraction"] == 0.666667
    del transaction["vin"][0]["sequence"]
    assert extract_features(transaction)["rbf_signaled"] is None
    transaction["vin"][0]["sequence"] = 0xFFFFFFFE
    assert extract_features(transaction)["rbf_signaled"] is False
