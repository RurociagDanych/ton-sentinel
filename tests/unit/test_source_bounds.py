"""Finite scan contracts reject coercion and unbounded walks."""

import pytest

from ton_sentinel.source.scan import ScanPolicy


def test_policy_requires_integer_half_open_bounds_and_finite_budget():
    policy = ScanPolicy(start=1_700_000_000, end=1_700_000_060,
                        page_size=2, max_pages=4, max_actions=8)
    assert (policy.start, policy.end) == (1_700_000_000, 1_700_000_060)
    assert (policy.page_size, policy.max_pages, policy.max_actions) == (2, 4, 8)
    for values in (
        {"start": True}, {"end": 1_700_000_060.0}, {"page_size": 0},
        {"max_pages": 0}, {"max_actions": -1},
        {"end": 1_700_000_000}, {"end": 2**63},
    ):
        args = dict(start=1_700_000_000, end=1_700_000_060,
                    page_size=2, max_pages=4, max_actions=8)
        args.update(values)
        with pytest.raises(ValueError):
            ScanPolicy(**args)


def test_scan_policy_bounds_the_total_number_of_pages():
    policy = ScanPolicy(start=10, end=20, page_size=2, max_pages=3, max_actions=6)
    assert policy.max_pages == 3
    with pytest.raises(ValueError):
        ScanPolicy(start=10, end=20, page_size=2, max_pages=3, max_actions=7)
