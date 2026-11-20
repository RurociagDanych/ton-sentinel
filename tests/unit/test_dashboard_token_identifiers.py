"""Real jetton token identifiers are public (DD4) and must be served."""

import pytest

from ton_sentinel import dashboard_payload as dp

HEX = "e3a65933a97b5e4f" * 4


@pytest.mark.parametrize("token", ["jetton:0:" + HEX, "jetton:-1:" + HEX, "jetton:0:" + HEX.upper()])
def test_whole_jetton_token_identifier_is_served(token):
    payload = {"token": token, "tokens": [{"token": token}], "series": [{"token": token}]}
    assert dp.ensure_safe(payload) == payload
    assert dp.find_violations(payload) == []


def test_detector_explanation_naming_a_recorded_token_is_served():
    text = ("swap_count_spike.v1 mainnet/jetton:0:" + HEX + " [1794559800,1794560100): missing "
            "(current_partial); 0 eligible prior windows, 24 required; input sha256:" + HEX)
    payload = {"detections": [{"explanation": text}, {"explanation": text.replace(":0:", ":-1:")}]}
    assert dp.ensure_safe(payload) == payload


@pytest.mark.parametrize("value", [
    "0:" + HEX,
    "-1:" + HEX,
    "owner 0:" + HEX,
    "jetton:0:" + HEX + " paid by 0:" + HEX,
    "jetton: 0:" + HEX,
])
def test_raw_addresses_outside_a_whole_token_identifier_are_still_refused(value):
    with pytest.raises(dp.UnsafePayload):
        dp.ensure_safe({"deep": [{"x": value}]})
