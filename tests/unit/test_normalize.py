"""Offline synthetic-v1 economic policy, never deployed protocol evidence."""
import base64

from dataclasses import FrozenInstanceError
from decimal import localcontext
import json
from typing import Any

import pytest

from ton_sentinel.contracts import Action, Contribution, SourceEpoch
from ton_sentinel.normalize import normalize, contributions, semantic_digest

TRACE = base64.b64encode(bytes(range(32))).decode()
ID = base64.b64encode(b'a' * 32).decode()
OTHER = base64.b64encode(b'b' * 32).decode()
EPOCH = SourceEpoch('testnet', 'synthetic:source', 'synthetic:filter',
                    'synthetic:build', 'synthetic:policy')


def payload(**changes) -> dict[str, Any]:
    return dict(schema_version=1, kind='synthetic_swap', synthetic=True,
                network='testnet', trace_id=TRACE, action_id=ID,
                protocol='STON.fi', protocol_version='synthetic-v1',
                action_end=301, status='success', eligible=True,
                route_proven=True, route=['TON', 'jetton:A'],
                input={'asset': 'TON', 'raw_amount': '1000000000',
                       'decimals': 9, 'decimal_provenance': 'synthetic:declared'},
                output={'asset': 'jetton:A', 'raw_amount': '250',
                        'decimals': 2, 'decimal_provenance': 'synthetic:declared'},
                trader=OTHER, trader_method='synthetic:explicit',
                trader_unambiguous=True, metadata={'symbol': 'ignored'}) | changes


@pytest.mark.parametrize('protocol', ['STON.fi', 'DeDust'])
@pytest.mark.parametrize('assets', [('TON', 'jetton:A'), ('jetton:A', 'TON'),
                                   ('jetton:A', 'jetton:B'), ('jetton:A', 'jetton:A')])
def test_bidirectional_routes(protocol, assets):
    p = payload(protocol=protocol)
    p['input']['asset'], p['output']['asset'] = assets
    p['route'] = [assets[0], 'jetton:intermediate', assets[1]]
    action = normalize(p, EPOCH)
    rows = contributions(action)
    assert [c.token for c in rows] == list(dict.fromkeys(assets))
    assert {c.routed_trade_id for c in rows} == {action.identity.event_id}
    assert all(c.count_eligible and c.trader == OTHER for c in rows)
    assert all((c.window_start, c.window_end) == (300, 600) for c in rows)
    assert rows[0].input_raw == '1000000000' and rows[0].input_scaled == '1.000000000'
    assert rows[-1].output_raw == '250' and rows[-1].output_scaled == '2.50'
    assert rows[0].role == ('both' if assets[0] == assets[1] else 'input')
    assert rows[-1].role == ('both' if assets[0] == assets[1] else 'output')
    assert [row.unit for row in rows] == ['raw:' + token for token in dict.fromkeys(assets)]
    if assets[0] != assets[1]:
        assert rows[0].output_raw is rows[0].output_scaled is None
        assert rows[-1].input_raw is rows[-1].input_scaled is None


def test_repeated_cyclic_internal_route_preserves_raw_endpoints_and_counts_once():
    p = payload(route=['TON', 'jetton:A', 'jetton:B', 'jetton:A', 'TON', 'jetton:A'])
    action = normalize(p, EPOCH)
    rows = contributions(action)
    assert (action.input.asset, action.output.asset) == ('TON', 'jetton:A')
    assert [row.token for row in rows] == ['TON', 'jetton:A']
    assert [row.role for row in rows] == ['input', 'output']
    assert [row.routed_trade_id for row in rows] == [action.identity.event_id] * 2


@pytest.mark.parametrize('field', ['sender', 'router', 'aggregator', 'recipient'])
def test_actor_roles_are_never_inferred_as_trader(field):
    p = payload()
    p.pop('trader')
    p.pop('trader_method')
    p.pop('trader_unambiguous')
    p[field] = OTHER
    with pytest.raises(ValueError):
        normalize(p, EPOCH)


def test_missing_trader_attribution_keeps_route_count_but_not_trader_eligibility():
    p = payload()
    for name in ('trader', 'trader_method', 'trader_unambiguous'):
        del p[name]
    action = normalize(p, EPOCH)
    rows = contributions(action)
    assert action.eligible
    assert [row.token for row in rows] == ['TON', 'jetton:A']
    assert all(row.count_eligible for row in rows)
    assert all(row.trader is None and not row.trader_eligible for row in rows)


@pytest.mark.parametrize('field', ['trace_id', 'action_id'])
def test_invalid_source_identity_rejected_by_normalizer(field):
    p = payload(**{field: 'not-a-source-id'})
    with pytest.raises(ValueError):
        normalize(p, EPOCH)


def test_equal_sized_actions_in_one_trace_remain_distinct_contributions():
    first = normalize(payload(), EPOCH)
    second = normalize(payload(action_id=OTHER), EPOCH)
    assert first.identity.trace_id == second.identity.trace_id
    assert first.identity.action_id != second.identity.action_id
    first_rows, second_rows = contributions(first), contributions(second)
    assert [(row.token, row.input_raw, row.output_raw) for row in first_rows] == [
        (row.token, row.input_raw, row.output_raw) for row in second_rows]
    assert {row.event_id for row in first_rows}.isdisjoint(
        row.event_id for row in second_rows)
    assert {row.routed_trade_id for row in first_rows}.isdisjoint(
        row.routed_trade_id for row in second_rows)


@pytest.mark.parametrize('changes,reason', [({'status': 'failed'}, 'status_not_success'),
    ({'status': 'pending'}, 'status_not_success'), ({'eligible': None}, 'eligibility_not_explicit'),
    ({'route_proven': False}, 'route_not_proven'), ({'protocol': 'unknown'}, 'unsupported_protocol'),
    ({'protocol_version': 'v2'}, 'unsupported_protocol')])
def test_fail_closed(changes, reason):
    action = normalize(payload(**changes), EPOCH)
    assert not action.eligible and reason in action.reasons
    assert action.input.asset == 'TON' and action.output.raw_amount == '250'
    assert contributions(action) == ()


@pytest.mark.parametrize('changes', [{'trader': None}, {'trader_unambiguous': False},
                                    {'trader_method': None}])
def test_trader_attribution(changes):
    rows = contributions(normalize(payload(**changes), EPOCH))
    assert len(rows) == 2
    assert all(c.trader is None and not c.trader_eligible for c in rows)


def test_digest_is_semantic_and_identity_stable():
    original = normalize(payload(), EPOCH)
    assert semantic_digest(original) == semantic_digest(normalize(payload(metadata={'name': 'new'}), EPOCH))
    for field, value in [('action_end', 302), ('eligible', False),
                         ('route', ['TON', 'jetton:X', 'jetton:A'])]:
        changed = normalize(payload(**{field: value}), EPOCH)
        assert changed.identity == original.identity
        assert semantic_digest(changed) != semantic_digest(original)
    p = payload()
    p['input']['raw_amount'] = '1000000001'
    assert semantic_digest(normalize(p, EPOCH)) != semantic_digest(original)
    other = normalize(payload(action_id=OTHER), EPOCH)
    assert other.identity != original.identity
    assert semantic_digest(other) != semantic_digest(original)


def test_exact_boundary_missing_decimals_and_no_ambient_rounding():
    p = payload()
    p['input']['raw_amount'] = '9' * 38
    p['input']['decimals'] = 18
    p['output']['decimals'] = None
    p['output']['decimal_provenance'] = None
    with localcontext() as ctx:
        ctx.prec = 2
        rows = contributions(normalize(p, EPOCH))
    assert rows[0].input_scaled == '99999999999999999999.999999999999999999'
    assert rows[1].output_scaled is None and rows[1].output_raw == '250'
    p['input']['raw_amount'] = '1' + '0' * 38
    with pytest.raises(ValueError):
        normalize(p, EPOCH)


def test_numeric_profile_minimum_and_maximum_scale_through_normalizer():
    p = payload()
    p['input']['raw_amount'] = '0'
    p['input']['decimals'] = 38
    p['output']['raw_amount'] = '9' * 38
    p['output']['decimals'] = 38
    with localcontext() as ctx:
        ctx.prec = 1
        rows = contributions(normalize(p, EPOCH))
    assert rows[0].input_raw == '0'
    assert rows[0].input_scaled == '0.' + '0' * 38
    assert rows[1].output_raw == '9' * 38
    assert rows[1].output_scaled == '0.' + '9' * 38


def test_eligible_zero_endpoints_keep_zero_distinct_from_absent_sides():
    p = payload()
    p['input']['raw_amount'] = '0'
    p['output']['raw_amount'] = '0'

    action = normalize(p, EPOCH)
    rows = contributions(action)

    assert action.eligible
    assert len(rows) == 2
    assert all(row.count_eligible for row in rows)
    assert (rows[0].token, rows[0].input_raw, rows[0].output_raw) == (
        'TON', '0', None)
    assert (rows[1].token, rows[1].input_raw, rows[1].output_raw) == (
        'jetton:A', None, '0')


@pytest.mark.parametrize('bad', [None, True, 1, 1.5, '', '-1', '+1', '01', '1.0', '1e3', '١', '9' * 39])
def test_reject_noncanonical_amount(bad):
    p = payload()
    p['input']['raw_amount'] = bad
    with pytest.raises(ValueError):
        normalize(p, EPOCH)


@pytest.mark.parametrize('cls', [Action, Contribution])
def test_frozen_versioned_roundtrip(cls):
    action = normalize(payload(), EPOCH)
    value = action if cls is Action else contributions(action)[0]
    assert cls.from_json(value.to_json()) == value
    with pytest.raises(FrozenInstanceError):
        setattr(value, 'eligible', False)
    for key, bad in [('schema_version', True), ('schema_version', 2), ('kind', 'other'), ('extra', 1)]:
        data = json.loads(value.to_json())
        data[key] = bad
        with pytest.raises(ValueError):
            cls.from_json(json.dumps(data))


def test_absent_decimals_preserve_raw_only():
    p = payload()
    del p['input']['decimals']
    del p['input']['decimal_provenance']
    rows = contributions(normalize(p, EPOCH))
    assert rows[0].input_raw == '1000000000'
    assert rows[0].input_scaled is None


@pytest.mark.parametrize('mutation', ['version', 'synthetic', 'extra', 'network',
    'identity', 'route', 'endpoint', 'time', 'decimals', 'provenance', 'eligibility'])
def test_malformed_synthetic_contract(mutation):
    p = payload()
    if mutation == 'version':
        p['schema_version'] = True
    elif mutation == 'synthetic':
        p['synthetic'] = False
    elif mutation == 'extra':
        p['sender'] = OTHER  # Never an implicit trader fallback.
    elif mutation == 'network':
        p['network'] = 'mainnet'
    elif mutation == 'identity':
        p['action_id'] = 'not-a-hash'
    elif mutation == 'route':
        p['route'] = ['TON', 'wrong']
    elif mutation == 'endpoint':
        p['input']['unexpected'] = 1
    elif mutation == 'time':
        p['action_end'] = True
    elif mutation == 'decimals':
        p['input']['decimals'] = 39
    elif mutation == 'provenance':
        p['input']['decimal_provenance'] = None
    else:
        p['eligible'] = 1
    with pytest.raises(ValueError):
        normalize(p, EPOCH)


def test_absent_assertions_fail_closed_without_losing_endpoint():
    p = payload()
    for name in ('eligible', 'route_proven', 'trader', 'trader_method', 'trader_unambiguous'):
        del p[name]
    action = normalize(p, EPOCH)
    assert action.reasons == ('eligibility_not_explicit', 'route_not_proven')
    assert action.output.asset == 'jetton:A'
    assert not contributions(action)


@pytest.mark.parametrize('seconds,start', [(0, 0), (299, 0), (300, 300), (600, 600)])
def test_utc_half_open_windows(seconds, start):
    rows = contributions(normalize(payload(action_end=seconds), EPOCH))
    assert all(c.window_start == start and c.window_end == start + 300 for c in rows)
