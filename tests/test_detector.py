import pytest

from blockpulse.detector import DetectorConfig, screen_transaction


def features(**overrides):
    row = {
        "n_in": 1, "n_out": 2, "value_out_sat": 100000, "fee_sat": 603,
        "vsize": 201, "fee_rate_sat_vb": 3.0, "max_equal_output_count": 1,
        "round_amount_fraction": 0.5, "rbf_signaled": True,
    }
    row.update(overrides)
    return row


def test_default_rules_explain_fanout_fanin_and_repeated_values():
    result = screen_transaction(features(n_out=12))
    assert result["is_flagged"] is True
    assert result["signals"][0]["rule_id"] == "many_outputs"
    assert "12 outputs" in result["signals"][0]["explanation"]

    result = screen_transaction(features(n_in=168))
    assert result["signals"][0]["rule_id"] == "many_inputs"
    assert "168 inputs" in result["signals"][0]["explanation"]

    result = screen_transaction(features(n_in=3, n_out=4, max_equal_output_count=4))
    assert result["signals"][0]["rule_id"] == "repeated_output_values"
    assert "benign" in result["signals"][0]["explanation"]


def test_no_signal_for_ordinary_shape_and_custom_thresholds():
    assert screen_transaction(features())["signals"] == []
    config = DetectorConfig(many_outputs_min=5)
    assert screen_transaction(features(n_out=5), config)["signals"][0]["rule_id"] == "many_outputs"


@pytest.mark.parametrize("value", [0, 1, "true"])
def test_invalid_rbf_value_rejected(value):
    with pytest.raises(ValueError, match="rbf_signaled"):
        screen_transaction(features(rbf_signaled=value))


def test_invalid_threshold_rejected():
    with pytest.raises(ValueError, match="positive integer"):
        DetectorConfig(many_inputs_min=0)
