"""Pure decisions of the retention cycle."""

import json

import pytest

from ton_sentinel.contracts import Change, WindowKey
from ton_sentinel.maintenance import (
    CYCLE_POLICY_KEYS, DERIVED_POLICY_KEYS, DeliveryAdmission, choose_vacuum, cycle_policy,
    delivery_reasons, derived_policy, storage_pressure,
)

DAY = 86400
DERIVED = {"derived_retention_seconds": 30 * DAY, "max_windows": 10}
CYCLE = {"event_retention_seconds": 7 * DAY, "guard_retention_seconds": 30 * DAY,
         "derived_retention_seconds": 30 * DAY, "max_windows": 10, "vacuum_max_tables": 2,
         "vacuum_max_bytes": 1 << 20, "storage_budget_bytes": 1 << 30, "min_free_percent": 20}


def test_derived_policy_names_exactly_its_keys_without_defaults():
    assert DERIVED_POLICY_KEYS == frozenset(DERIVED)
    assert derived_policy(dict(DERIVED)) == (30 * DAY, 10)
    for broken in ({"max_windows": 10}, dict(DERIVED, extra=1), [], None):
        with pytest.raises(ValueError, match="must name exactly"):
            derived_policy(broken)


@pytest.mark.parametrize("key", sorted(DERIVED))
@pytest.mark.parametrize("value", [0, -1, 1.5, "10", True, None])
def test_derived_policy_values_are_positive_integers(key, value):
    with pytest.raises(ValueError, match="positive integer"):
        derived_policy(dict(DERIVED, **{key: value}))


def test_cycle_policy_names_every_bound_explicitly():
    assert CYCLE_POLICY_KEYS == frozenset(CYCLE)
    parsed = cycle_policy(dict(CYCLE))
    assert parsed["retention"] == {"event_retention_seconds": 7 * DAY,
                                   "guard_retention_seconds": 30 * DAY, "max_windows": 10}
    assert parsed["derived"] == {"derived_retention_seconds": 30 * DAY, "max_windows": 10}
    assert (parsed["vacuum_max_tables"], parsed["vacuum_max_bytes"]) == (2, 1 << 20)
    assert (parsed["storage_budget_bytes"], parsed["min_free_percent"]) == (1 << 30, 20)
    for key in sorted(CYCLE):
        with pytest.raises(ValueError, match="must name exactly"):
            cycle_policy({k: v for k, v in CYCLE.items() if k != key})
    with pytest.raises(ValueError, match="must name exactly"):
        cycle_policy(dict(CYCLE, extra=1))


def test_derived_history_is_kept_at_least_as_long_as_the_payload():
    with pytest.raises(ValueError, match="derived_retention_seconds must not be shorter"):
        cycle_policy(dict(CYCLE, derived_retention_seconds=7 * DAY - 1))
    assert cycle_policy(dict(CYCLE, derived_retention_seconds=7 * DAY))["derived"][
        "derived_retention_seconds"] == 7 * DAY


@pytest.mark.parametrize("percent", [0, 100, 101, -5, 20.0, True])
def test_free_target_is_a_percentage_strictly_between_0_and_100(percent):
    with pytest.raises(ValueError, match="min_free_percent"):
        cycle_policy(dict(CYCLE, min_free_percent=percent))


def test_low_water_is_exactly_below_twenty_percent_free():
    assert storage_pressure(80, 100, 20) == {
        "used_bytes": 80, "budget_bytes": 100, "free_bytes": 20, "free_percent": "20.00",
        "min_free_percent": 20, "status": "ok"}
    assert storage_pressure(81, 100, 20)["status"] == "low_water"
    assert storage_pressure(81, 100, 20)["free_percent"] == "19.00"
    over = storage_pressure(150, 100, 20)
    assert (over["status"], over["free_bytes"], over["free_percent"]) == (
        "low_water", 0, "0.00")


@pytest.mark.parametrize("args", [(-1, 100, 20), (1, 0, 20), (1, 100, 0), (1.0, 100, 20)])
def test_low_water_inputs_are_checked(args):
    with pytest.raises(ValueError):
        storage_pressure(*args)


TABLES = {"a": {"bytes": 300, "dead_tuples": 5}, "b": {"bytes": 200, "dead_tuples": 9},
          "c": {"bytes": 900, "dead_tuples": 7}, "d": {"bytes": 10, "dead_tuples": 0}}


def test_vacuum_takes_the_most_dead_tuples_first_within_both_budgets():
    chosen, skipped = choose_vacuum(TABLES, max_tables=2, max_bytes=600)
    assert chosen == ["b", "a"]
    assert skipped == {"c": "over_byte_budget", "d": "no_dead_tuples"}
    chosen, skipped = choose_vacuum(TABLES, max_tables=1, max_bytes=10_000)
    assert chosen == ["b"]
    assert skipped == {"c": "over_table_budget", "a": "over_table_budget",
                       "d": "no_dead_tuples"}


def test_vacuum_budgets_are_positive_integers():
    for kwargs in ({"max_tables": 0, "max_bytes": 1}, {"max_tables": 1, "max_bytes": 0},
                   {"max_tables": True, "max_bytes": 1}):
        with pytest.raises(ValueError):
            choose_vacuum(TABLES, **kwargs)


def change(*windows, generation=3, kind="revision"):
    return Change(kind, "ton-sentinel:event:v1:x", "redelivery", generation,
                  tuple(WindowKey("testnet", "TON", w) for w in windows), ())


def test_delivery_reasons_name_every_expiry_that_applies():
    retained = {"payload_from": 0, "guard_retired_before": 0, "guard_generation": None}
    assert delivery_reasons(change(300), **retained) == ()
    assert delivery_reasons(change(300), payload_from=600, guard_retired_before=0,
                            guard_generation=None) == ("payload_expired",)
    assert delivery_reasons(change(300, 900), payload_from=600, guard_retired_before=600,
                            guard_generation=None) == ("guard_retired", "payload_expired")
    assert delivery_reasons(change(900, generation=3), payload_from=600,
                            guard_retired_before=0, guard_generation=3) == (
        "identity_guarded", "stale_generation")
    assert delivery_reasons(change(900, generation=4), payload_from=600,
                            guard_retired_before=0, guard_generation=3) == (
        "identity_guarded",)


def test_only_deliveries_are_classified():
    for kind in ("quarantine", "cleared"):
        with pytest.raises(ValueError, match="not a delivery"):
            delivery_reasons(change(0, kind=kind, generation=None), payload_from=0,
                             guard_retired_before=0, guard_generation=None)
    with pytest.raises(ValueError, match="windows"):
        delivery_reasons(change(), payload_from=0, guard_retired_before=0,
                         guard_generation=None)


def test_delivery_admission_is_admitted_only_without_reasons():
    ok = DeliveryAdmission(event_id="e", admitted=True, status="admitted", reasons=())
    refused = DeliveryAdmission(event_id="e", admitted=False, status="backfill_only",
                                reasons=("payload_expired",))
    assert ok.admitted and not refused.admitted
    with pytest.raises(ValueError):
        DeliveryAdmission(event_id="e", admitted=True, status="admitted",
                          reasons=("payload_expired",))
    with pytest.raises(ValueError):
        DeliveryAdmission(event_id="e", admitted=False, status="backfill_only", reasons=())


def test_maintenance_cli_requires_policy_and_owner(capsys):
    from ton_sentinel.cli import main

    with pytest.raises(SystemExit) as exit_:
        main(["maintenance", "--dsn", "postgresql://never-used/x"])
    assert exit_.value.code == 2
    assert "--policy-file" in capsys.readouterr().err


def test_maintenance_cli_rejects_a_bad_policy_before_connecting(tmp_path, capsys):
    from ton_sentinel.cli import main

    path = tmp_path / "policy.json"
    path.write_text(json.dumps(dict(CYCLE, extra=1)))
    # an unreachable DSN proves the policy is refused before any connection attempt
    code = main(["maintenance", "--dsn", "postgresql://u@127.0.0.1:1/x", "--policy-file",
                 str(path), "--owner", "maintenance", "--now", "604800"])
    assert code == 2
    err = capsys.readouterr().err
    assert "must name exactly" in err and "127.0.0.1" not in err


def test_maintenance_cli_reads_the_dsn_from_the_environment_and_never_prints_it(
        tmp_path, capsys, monkeypatch):
    from ton_sentinel.cli import main

    path = tmp_path / "policy.json"
    path.write_text(json.dumps(CYCLE))
    monkeypatch.setenv("SENTINEL_DATABASE_DSN", "postgresql://u:s3cret@127.0.0.1:1/x")
    code = main(["maintenance", "--policy-file", str(path), "--owner", "maintenance",
                 "--now", "604800"])
    assert code == 1
    out = capsys.readouterr()
    assert "s3cret" not in out.err + out.out
    assert json.loads(out.out)["status"] == "failed"
