"""Synthetic identity-only truth; no deployed hashes or economic assertions."""

import base64
from dataclasses import FrozenInstanceError, fields
import json

import pytest

from ton_sentinel.contracts import ActionIdentity, SourceEpoch


TRACE = base64.b64encode(bytes(range(32))).decode('ascii')
ACTION = base64.b64encode(bytes([255]) * 32).decode('ascii')
OTHER = base64.b64encode(bytes([254]) * 32).decode('ascii')


def identity(**overrides):
    return ActionIdentity(**({'network': 'mainnet', 'trace_id': TRACE,
                              'action_id': ACTION} | overrides))


def epoch(**overrides):
    return SourceEpoch(**({'network': 'mainnet',
                          'source_fingerprint': 'synthetic:source:α',
                          'filter_fingerprint': 'synthetic:filter',
                          'build_fingerprint': 'synthetic:build',
                          'action_policy_fingerprint': 'synthetic:policy'} | overrides))


def test_precision_and_identity():
    first = identity()
    assert first.event_id == 'ton-sentinel:event:v1:' + json.dumps(
        ['mainnet', TRACE, ACTION], separators=(',', ':'))
    assert first != identity(action_id=OTHER)
    assert first.event_id != identity(action_id=OTHER).event_id
    assert first.event_id != identity(network='testnet').event_id
    assert first.event_id != identity(trace_id=ACTION, action_id=TRACE).event_id
    assert {f.name for f in fields(first)} == {'network', 'trace_id', 'action_id'}
    assert len({first, identity(), identity(action_id=OTHER)}) == 2
    with pytest.raises(FrozenInstanceError):
        first.action_id = OTHER


@pytest.mark.parametrize('network', ['mainnet', 'testnet'])
def test_lossless_versioned_identity_roundtrip(network):
    original = identity(network=network)
    assert ActionIdentity.from_event_id(original.event_id) == original
    assert ActionIdentity.from_json(original.to_json()) == original
    assert json.loads(original.to_json()) == {
        'schema_version': 1, 'kind': 'action_identity', 'network': network,
        'trace_id': TRACE, 'action_id': ACTION}
    assert original.to_json() == identity(network=network).to_json()
    assert (original.trace_id, original.action_id) == (TRACE, ACTION)


@pytest.mark.parametrize('field', ['trace_id', 'action_id'])
@pytest.mark.parametrize('bad', [None, True, 7, b'x', '', 'x', '0' * 64,
    ACTION.rstrip('='), ACTION + '\n', ' ' + ACTION, ACTION.replace('/', '_'),
    base64.b64encode(b'x' * 31).decode(),
    base64.b64encode(b'x' * 33).decode(), ACTION[:-2] + '9='])
def test_malformed_ids_rejected_before_conversion(field, bad):
    with pytest.raises(ValueError):
        identity(**{field: bad})


@pytest.mark.parametrize('bad', [None, True, 1, '', 'MAINNET', ' mainnet', 'mainnet\n', 'unknown'])
@pytest.mark.parametrize('factory', [identity, epoch])
def test_exact_network_validation(factory, bad):
    with pytest.raises(ValueError):
        factory(network=bad)


def test_validation_never_coerces_source_objects():
    class Hostile:
        def __str__(self):
            raise AssertionError('source must not be coerced')
    with pytest.raises(ValueError):
        identity(trace_id=Hostile())
    with pytest.raises(ValueError):
        epoch(build_fingerprint=Hostile())


def test_source_epoch_lossless_and_independent_of_business_identity():
    original = epoch()
    assert SourceEpoch.from_json(original.to_json()) == original
    assert json.loads(original.to_json()) == {
        'schema_version': 1, 'kind': 'source_epoch', 'network': 'mainnet',
        'source_fingerprint': 'synthetic:source:α',
        'filter_fingerprint': 'synthetic:filter',
        'build_fingerprint': 'synthetic:build',
        'action_policy_fingerprint': 'synthetic:policy'}
    for field in ['source_fingerprint', 'filter_fingerprint', 'build_fingerprint',
                  'action_policy_fingerprint']:
        assert epoch(**{field: 'synthetic:changed'}) != original
        for bad in [None, True, 1, '', ' ', 'x\n', '\ud800']:
            with pytest.raises(ValueError):
                epoch(**{field: bad})
    with pytest.raises(FrozenInstanceError):
        original.network = 'testnet'


@pytest.mark.parametrize('factory,cls', [(identity, ActionIdentity), (epoch, SourceEpoch)])
@pytest.mark.parametrize('mutation', ['version', 'bool_version', 'float_version',
    'missing_version', 'extra', 'missing', 'kind', 'duplicate', 'not_object'])
def test_serialization_rejects_schema_drift(factory, cls, mutation):
    payload = json.loads(factory().to_json())
    if mutation == 'version':
        payload['schema_version'] = 2
    elif mutation == 'bool_version':
        payload['schema_version'] = True
    elif mutation == 'float_version':
        payload['schema_version'] = 1.0
    elif mutation == 'missing_version':
        del payload['schema_version']
    elif mutation == 'extra':
        payload['amount'] = '100'
    elif mutation == 'missing':
        del payload['network']
    elif mutation == 'kind':
        payload['kind'] = 'other'
    elif mutation == 'not_object':
        payload = []
    text = json.dumps(payload)
    if mutation == 'duplicate':
        text = text[:-1] + ', "network": "mainnet"}'
    with pytest.raises(ValueError):
        cls.from_json(text)


@pytest.mark.parametrize('bad', [None, b'{}', '', '{', 'null', 'NaN'])
@pytest.mark.parametrize('cls', [ActionIdentity, SourceEpoch])
def test_invalid_json_rejected(cls, bad):
    with pytest.raises(ValueError):
        cls.from_json(bad)


@pytest.mark.parametrize('field', ['trace_id', 'action_id', 'network'])
def test_deserialization_applies_source_validation(field):
    payload = json.loads(identity().to_json())
    payload[field] = ' invalid '
    with pytest.raises(ValueError):
        ActionIdentity.from_json(json.dumps(payload))
    parts = ['mainnet', TRACE, ACTION]
    parts[['network', 'trace_id', 'action_id'].index(field)] = ' invalid '
    with pytest.raises(ValueError):
        ActionIdentity.from_event_id('ton-sentinel:event:v1:' + json.dumps(parts))


@pytest.mark.parametrize('text', ['synthetic:a:b|c', ' synthetic:α ',
                                  'synthetic:"quote"\\slash', 'synthetic:😀'])
def test_opaque_epoch_fingerprints_are_preserved_exactly(text):
    original = epoch(source_fingerprint=text)
    restored = SourceEpoch.from_json(original.to_json())
    assert restored.source_fingerprint == text
    assert restored == original
    assert restored.to_json() == original.to_json()


@pytest.mark.parametrize('factory,cls', [(identity, ActionIdentity), (epoch, SourceEpoch)])
def test_json_kinds_cannot_be_interchanged(factory, cls):
    other = SourceEpoch if cls is ActionIdentity else ActionIdentity
    with pytest.raises(ValueError):
        other.from_json(factory().to_json())


@pytest.mark.parametrize('byte', [0, 1, 127, 128, 254, 255])
def test_synthetic_hash_roundtrip_preserves_exact_source_string(byte):
    # Explicit synthetic provenance; these are not observed TON identifiers.
    fixture = {'synthetic': True,
               'trace_id': base64.b64encode(bytes([byte]) * 32).decode('ascii')}
    original = identity(trace_id=fixture['trace_id'])
    assert ActionIdentity.from_json(original.to_json()).trace_id == fixture['trace_id']
    assert ActionIdentity.from_event_id(original.event_id).trace_id == fixture['trace_id']


@pytest.mark.parametrize('bad', ['', 'ton-sentinel:event:v2:[]',
    'ton-sentinel:event:v1:[]', 'ton-sentinel:event:v1:{}',
    'ton-sentinel:event:v1:["mainnet", null, null]',
    'ton-sentinel:event:v1:["mainnet", "x", "y", "z"]', None])
def test_invalid_event_encoding_rejected(bad):
    with pytest.raises(ValueError):
        ActionIdentity.from_event_id(bad)


def test_independent_synthetic_economic_truth():
    from dataclasses import asdict
    import json
    from pathlib import Path

    from ton_sentinel.contracts import Action, Contribution
    from ton_sentinel.normalize import normalize, contributions

    root = Path(__file__).parents[1] / 'fixtures' / 'truth'
    source = json.loads((root / 'actions.json').read_text())
    expected = json.loads((root / 'expected.json').read_text())
    assert source['synthetic'] is expected['synthetic'] is True
    assert len(source['actions']) == len(expected['expected']) == 5
    epoch = SourceEpoch('testnet', 'synthetic:source', 'synthetic:filter',
                        'synthetic:build', 'synthetic:policy')
    event_ids = set()
    for payload, wanted in zip(source['actions'], expected['expected'], strict=True):
        action = normalize(payload, epoch)
        assert action.eligible is wanted['eligible']
        assert list(action.reasons) == wanted['reasons']
        assert [action.input.asset, action.output.asset] == wanted['endpoints']
        assert action.input.raw_amount == payload['input']['raw_amount']
        assert action.output.raw_amount == payload['output']['raw_amount']
        assert Action.from_json(action.to_json()) == action
        assert action.identity.event_id not in event_ids
        event_ids.add(action.identity.event_id)
        rows = contributions(action)
        actual = []
        for row in rows:
            assert Contribution.from_json(row.to_json()) == row
            data = asdict(row)
            assert data.pop('event_id') == action.identity.event_id
            assert data.pop('routed_trade_id') == action.identity.event_id
            actual.append(data)
        assert actual == wanted['contributions']


def test_explicit_policy_decisions_match_product_readback_and_ignore_arrival_order():
    from copy import deepcopy
    from pathlib import Path

    from ton_sentinel.normalize import normalize, contributions
    from tests.oracle import expected_state

    root = Path(__file__).parents[1] / 'fixtures' / 'truth'
    source = json.loads((root / 'actions.json').read_text())
    decisions = json.loads((root / 'decisions.json').read_text())
    observations = [{'candidate_id': f'action-{i}', 'action': action}
                    for i, action in enumerate(source['actions'])]
    conflict = deepcopy(source['actions'][0])
    conflict['input']['raw_amount'] = '1000000001'
    observations.append({'candidate_id': 'conflicting-amount', 'action': conflict})
    truth = {'synthetic': source['synthetic'], 'observations': observations}
    expected = expected_state(truth, decisions['decisions'])
    assert expected_state(
        {'synthetic': True, 'observations': list(reversed(observations))},
        list(reversed(decisions['decisions']))) == expected

    candidates = {row['candidate_id']: row['action'] for row in observations}
    actual = []
    epoch = SourceEpoch('testnet', 'synthetic:source', 'synthetic:filter',
                        'synthetic:build', 'synthetic:policy')
    from ton_sentinel.config import select_policy_observations
    selected, status = select_policy_observations(observations, decisions['decisions'])
    for selected_row in selected:
        candidate = selected_row['candidate_id']
        action = normalize(candidates[candidate], epoch)
        actual.append({
            'candidate_id': candidate,
            'event_id': action.identity.event_id,
            'eligible': action.eligible,
            'reasons': list(action.reasons),
            'endpoints': [action.input.asset, action.output.asset],
            'contributions': [{name: getattr(row, name) for name in (
                'event_id', 'token', 'role', 'window_start', 'window_end',
                'routed_trade_id', 'count_eligible', 'trader', 'trader_eligible',
                'unit', 'input_raw', 'output_raw', 'input_scaled', 'output_scaled')}
                for row in contributions(action)],
        })
    assert actual == expected['canonical']
    assert status == expected['status']
    assert [row['candidate_id'] for row in selected] == [row['candidate_id'] for row in expected['canonical']]


def test_missing_decision_is_not_zero_rejections_or_complete_coverage():
    from copy import deepcopy
    from pathlib import Path

    from tests.oracle import expected_state

    root = Path(__file__).parents[1] / 'fixtures' / 'truth'
    source = json.loads((root / 'actions.json').read_text())
    decisions = json.loads((root / 'decisions.json').read_text())
    observations = [{'candidate_id': f'action-{i}', 'action': action}
                    for i, action in enumerate(source['actions'])]
    conflict = deepcopy(source['actions'][0])
    conflict['input']['raw_amount'] = '1000000001'
    observations.append({'candidate_id': 'conflicting-amount', 'action': conflict})
    state = expected_state(
        {'synthetic': True, 'observations': observations},
        decisions['decisions'][:-1])
    assert state['status'] == {
        'decision_coverage': 'incomplete', 'accepted': 5, 'rejected': 0,
        'missing': ['conflicting-amount'], 'source_coverage': 'unknown'}


def test_oracle_accepts_decision_list_and_fails_closed_on_source_coverage():
    from pathlib import Path

    from tests.oracle import expected_state

    root = Path(__file__).parents[1] / 'fixtures' / 'truth'
    source = json.loads((root / 'actions.json').read_text())
    decisions = json.loads((root / 'decisions.json').read_text())['decisions']
    observations = [{'candidate_id': f'action-{i}', 'action': action}
                    for i, action in enumerate(source['actions'])]
    observations.append({'candidate_id': 'conflicting-amount', 'action': {
        **source['actions'][0], 'input': {**source['actions'][0]['input'],
                                         'raw_amount': '1000000001'}}})
    result = expected_state({'synthetic': True, 'observations': observations}, decisions)
    assert result['status'] == {
        'decision_coverage': 'complete', 'accepted': 5, 'rejected': 1,
        'missing': [], 'source_coverage': 'unknown'}


def test_oracle_rejects_duplicate_and_unauthoritative_decisions():
    from pathlib import Path

    from tests.oracle import expected_state

    root = Path(__file__).parents[1] / 'fixtures' / 'truth'
    source = json.loads((root / 'actions.json').read_text())
    truth = {'synthetic': True, 'observations': [
        {'candidate_id': f'action-{i}', 'action': action}
        for i, action in enumerate(source['actions'])]}
    decisions = json.loads((root / 'decisions.json').read_text())['decisions']
    with pytest.raises(ValueError):
        expected_state(truth, decisions + [decisions[0]])
    unauthoritative = [dict(row) for row in decisions]
    unauthoritative[0]['authority'] = ''
    with pytest.raises(ValueError):
        expected_state(truth, unauthoritative)


def test_explicit_time_mapping_is_conservative_and_matches_independent_oracle():
    from pathlib import Path

    from tests.oracle import expected_state
    from ton_sentinel.config import evaluate_action_window_policy

    fixture = json.loads((Path(__file__).parents[1] / 'fixtures' / 'truth' / 'coverage.json').read_text())
    policy = fixture['action_time_mapping']
    for scenario in fixture['mapping_scenarios']:
        start, end = scenario['window']
        actual = evaluate_action_window_policy(policy, start, end)
        truth = {
            'synthetic': True,
            'observations': [],
            'coverage_policy': policy,
            'action_window': scenario['window'],
        }
        expected = expected_state(truth, [])['coverage']
        assert actual == expected == scenario['expected']
        assert actual['eligible_zero'] is False
        assert actual['features'] == {'status': 'unsupported', 'computed': False, 'items': None}
        assert actual['alerts'] == {'status': 'unsupported', 'computed': False, 'items': None}


def test_missing_or_invalid_time_mapping_fails_closed():
    from copy import deepcopy

    from ton_sentinel.config import evaluate_action_window_policy

    assert evaluate_action_window_policy(None, 0, 300)['coverage_status'] == 'missing'
    policy = {
        'schema_version': 1,
        'synthetic': True,
        'mappings': [{'action_start': 0, 'action_end': 300, 'status': 'bounded_observed'}],
    }
    invalid = deepcopy(policy)
    invalid['mappings'][0]['status'] = 'unknown'
    result = evaluate_action_window_policy(invalid, 0, 300)
    assert result['coverage_status'] == 'missing'
    assert result['eligible_zero'] is False
    assert result['reason'] == 'invalid_mapping'
    assert evaluate_action_window_policy(policy, 300, 600)['coverage_status'] == 'missing'


def _matrix():
    from pathlib import Path
    root = Path(__file__).parents[1] / 'fixtures' / 'truth'
    return json.loads((root / 'matrix.json').read_text())


def test_matrix_fixture_is_hand_derived_and_covers_required_scenarios():
    data = _matrix()
    assert data['synthetic'] is True and 'derivation' in data
    kinds = {(c['protocol'], c['kind']) for c in data['cases']}
    need = {'buy', 'sell', 'jetton_to_jetton', 'multihop', 'failed', 'pending',
            'missing_eligibility', 'missing_trader', 'cyclic', 'same_trace_equal_a',
            'same_trace_equal_b'}
    for protocol in ('STON.fi', 'DeDust'):
        assert {k for p, k in kinds if p == protocol} == need


def test_matrix_product_and_independent_oracle_match_literal_expectations():
    from ton_sentinel.normalize import normalize, contributions
    from tests.oracle import expected_state
    data = _matrix()
    epoch = SourceEpoch('testnet', 'synthetic:source', 'synthetic:filter',
                        'synthetic:build', 'synthetic:policy')
    for case in data['cases']:
        action = normalize(case['payload'], epoch)
        want = case['expected']
        assert action.eligible is want['eligible'], case['name']
        assert list(action.reasons) == want['reasons'], case['name']
        assert action.identity.event_id == want['event_id'], case['name']
        rows = contributions(action)
        got = [{k: getattr(r, k) for k in want['contributions'][0]} for r in rows] \
            if want['contributions'] else []
        assert got == want['contributions'], case['name']
        assert len({r.token for r in rows}) == len(rows)
    ids = [c['expected']['event_id'] for c in data['cases']]
    assert len(set(ids)) == len(ids)
    obs = [{'candidate_id': c['name'], 'action': c['payload']} for c in data['cases']]
    oracle_rows = {}
    for c in data['cases']:
        state = expected_state({'synthetic': True, 'observations': [
            {'candidate_id': c['name'], 'action': c['payload']}]}, data['decisions'][c['name']])
        oracle_rows[c['name']] = state['canonical'][0]
    for c in data['cases']:
        row = oracle_rows[c['name']]
        assert row['eligible'] is c['expected']['eligible'], c['name']
        assert row['contributions'] == c['expected']['contributions'], c['name']
