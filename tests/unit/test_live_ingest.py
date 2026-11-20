"""The production path accepts only live-normalized real records."""

import base64
import copy
import json
import logging
import os
from collections import Counter, defaultdict
from pathlib import Path

import pytest

from ton_sentinel.contracts import (
    SYNTHETIC_FIXTURE_ENV, Action, ActionIdentity, Endpoint, SourceEpoch, SyntheticInputRefused,
)
from ton_sentinel.normalize import contributions, normalize
from ton_sentinel.source.live import LiveBatch, LivePages, live_swap_batch

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / 'tests' / 'fixtures' / 'live' / 'g3_window.json'
EPOCH = SourceEpoch('mainnet', 'toncenter-v3:/api/v3/actions+/api/v3/traces (recorded G3 W)',
                    'jetton_swap actions + complete DeDust traces', 'task-16-test', 'live_swap v1')
TESTNET = SourceEpoch('testnet', 'synthetic:source', 'synthetic:filter', 'synthetic:build',
                      'synthetic:policy')


@pytest.fixture
def production(monkeypatch):
    """The production process environment: the test-only fixture switch is absent."""
    monkeypatch.delenv(SYNTHETIC_FIXTURE_ENV, raising=False)


@pytest.fixture(scope='module')
def window():
    return json.loads(FIXTURE.read_text())


def _batch(fx, **changes):
    return live_swap_batch(changes.get('actions', fx['action_responses']),
                           changes.get('traces', fx['trace_responses']),
                           network=fx['network'], native_aliases=frozenset(fx['native_aliases']))


def _synthetic():
    return {
        'schema_version': 1, 'kind': 'synthetic_swap', 'synthetic': True, 'network': 'testnet',
        'trace_id': base64.b64encode(bytes([31]) * 32).decode('ascii'),
        'action_id': base64.b64encode(bytes([32]) * 32).decode('ascii'),
        'protocol': 'STON.fi', 'protocol_version': 'synthetic-v1', 'action_end': 11,
        'status': 'success', 'eligible': True, 'route_proven': True, 'route': ['TON', 'jetton:A'],
        'input': {'asset': 'TON', 'raw_amount': '100'},
        'output': {'asset': 'jetton:A', 'raw_amount': '25'},
    }


def _all_pages(pages, start, end, size=100):
    out, offset = [], 0
    while True:
        page = pages.fetch_page(EPOCH, start, end, offset, size)
        out += page
        offset += len(page)
        if len(page) < size:
            return out



def test_synthetic_payload_is_refused_by_normalize_in_the_production_path(production):
    with pytest.raises(SyntheticInputRefused, match='synthetic-v1 .*test-only'):
        normalize(_synthetic(), TESTNET)


def test_synthetic_v1_action_contract_is_refused_in_the_production_path(production, monkeypatch):
    identity = ActionIdentity('testnet', base64.b64encode(bytes([1]) * 32).decode(),
                              base64.b64encode(bytes([2]) * 32).decode())
    args = (identity, TESTNET, 'STON.fi', 'synthetic-v1', 11, 'success', True, True,
            ('TON', 'jetton:A'), Endpoint('TON', '1', None, None),
            Endpoint('jetton:A', '2', None, None), None, None, None)
    with pytest.raises(SyntheticInputRefused):
        Action(*args)
    monkeypatch.setenv(SYNTHETIC_FIXTURE_ENV, 'test-only')
    encoded = Action(*args).to_json()
    monkeypatch.delenv(SYNTHETIC_FIXTURE_ENV)
    with pytest.raises(SyntheticInputRefused):
        Action.from_json(encoded)


def test_only_the_exact_test_value_enables_fixtures(monkeypatch):
    for value in ('1', 'true', 'TEST-ONLY', ' test-only', ''):
        monkeypatch.setenv(SYNTHETIC_FIXTURE_ENV, value)
        with pytest.raises(SyntheticInputRefused):
            normalize(_synthetic(), TESTNET)
    monkeypatch.setenv(SYNTHETIC_FIXTURE_ENV, 'test-only')
    assert normalize(_synthetic(), TESTNET).protocol_version == 'synthetic-v1'


def test_refusal_is_a_value_error_with_an_explicit_message(production):
    with pytest.raises(ValueError) as caught:
        normalize(_synthetic(), TESTNET)
    assert type(caught.value) is SyntheticInputRefused
    assert 'production path accepts only live_swap records' in str(caught.value)


def test_fixture_switch_is_set_only_by_the_test_suite_and_the_probe_harness():
    hits = sorted(str(p.relative_to(ROOT)) for top in ('src', 'deploy', 'scripts')
                  for p in (ROOT / top).rglob('*') if p.is_file() and p.suffix != '.pyc'
                  and SYNTHETIC_FIXTURE_ENV in p.read_text(errors='ignore'))
    # contracts.py defines the switch; k3s_probe.py is the deployment smoke harness (see
    # contracts.py). No deploy manifest or operator script may enable fixtures.
    assert hits == ['src/ton_sentinel/contracts.py', 'src/ton_sentinel/k3s_probe.py']
    assert SYNTHETIC_FIXTURE_ENV in (ROOT / 'tests' / 'conftest.py').read_text()


def test_live_pages_refuse_a_synthetic_payload(production, monkeypatch):
    with pytest.raises(SyntheticInputRefused):
        LivePages(LiveBatch(payloads=(_synthetic(),), unsupported=(), unresolved=()))
    monkeypatch.setenv(SYNTHETIC_FIXTURE_ENV, 'test-only')
    with pytest.raises(SyntheticInputRefused):
        LivePages(LiveBatch(payloads=(_synthetic(),), unsupported=(), unresolved=()))



def test_recorded_window_yields_only_live_swap_records(production, window):
    batch = _batch(window)
    assert all(p['kind'] == 'live_swap' and p['synthetic'] is False for p in batch.payloads)
    actions = [normalize(p, EPOCH) for p in batch.payloads]
    # 559 indexer actions (2 unsupported DEX labels) + 16 recognizer gap fills (closure item 2).
    assert len(actions) == 575
    assert Counter(reason for reason, _id in batch.unsupported) == {'unsupported dex label': 2}
    assert batch.unresolved == ()
    assert sum(a.eligible for a in actions) == 475
    keys = [(a.action_end, a.identity.trace_id, a.identity.action_id) for a in actions]
    assert keys == sorted(keys) and len(set(keys)) == len(keys)


def test_live_pages_serve_eligible_final_records_and_account_for_exclusions(production, window):
    pages = LivePages(_batch(window))
    w0, w1 = window['window']
    served = _all_pages(pages, w0, w1)
    assert len(served) == 475
    assert all(normalize(p, EPOCH).eligible for p in served)
    assert _all_pages(pages, w0, w1, size=7) == served
    reasons = Counter(tuple(r) for _t, _a, r in pages.excluded(EPOCH, w0, w1))
    assert reasons == {('status_not_success',): 86,
                       ('status_not_success', 'route_not_proven'): 2,
                       ('route_not_proven',): 12}
    first = _all_pages(pages, w0, w0 + 300)
    assert first and all(w0 <= p['action_end'] < w0 + 300 for p in first)


def test_pending_live_records_are_served_so_the_scan_fails_closed(production, window):
    responses = copy.deepcopy(window['action_responses'])
    target = responses[0]['actions'][0]
    target['finality'] = 'pending'
    pages = LivePages(_batch(window, actions=responses))
    w0, w1 = window['window']
    served = _all_pages(pages, w0, w1)
    pending = [p for p in served if p['action_id'] == target['action_id']]
    assert len(pending) == 1 and pending[0]['status'] == 'pending'
    assert normalize(pending[0], EPOCH).reasons[0] == 'status_not_success'


def test_live_batch_refuses_conflicting_duplicates(production, window):
    responses = copy.deepcopy(window['action_responses'])
    twin = copy.deepcopy(responses[0]['actions'][0])
    twin['details']['dex_incoming_transfer']['amount'] = '1'
    responses[-1]['actions'].append(twin)
    with pytest.raises(ValueError, match='conflicting'):
        _batch(window, actions=responses)
    same = copy.deepcopy(window['action_responses'])
    same[-1]['actions'].append(copy.deepcopy(same[0]['actions'][0]))
    assert len(_batch(window, actions=same).payloads) == 575



def test_dedust_recognizer_output_reaches_the_product_contract(production, window):
    batch = _batch(window)
    pages = LivePages(batch)
    w0, w1 = window['window']
    served = {p['action_id']: p for p in _all_pages(pages, w0, w1)}
    v1 = [p for p in batch.payloads if p['protocol_version'] == 'toncenter-v3-trace/dedust']
    v2 = [p for p in batch.payloads if p['protocol_version'] == 'toncenter-v3-trace/dedust_cpmm_v2']
    assert len(v1) == 4 and len(v2) == 12
    for p in v1:
        action = normalize(p, EPOCH)
        assert action.protocol == 'DeDust' and action.eligible
        assert p['action_id'] in served
        rows = contributions(action)
        assert rows and {r.token for r in rows} == {action.input.asset, action.output.asset}
    excluded = {a: r for _t, a, r in pages.excluded(EPOCH, w0, w1)}
    assert all(excluded[p['action_id']] == ('route_not_proven',) for p in v2)


def test_recognizer_skips_swaps_the_indexer_already_reports(production, window):
    batch = _batch(window)
    dedust = defaultdict(list)
    for p in batch.payloads:
        if p['protocol'] == 'DeDust':
            dedust[p['trace_id']].append((p['input']['asset'], p['input']['raw_amount']))
    assert all(len(v) == len(set(v)) for v in dedust.values())


def test_recorded_window_matches_the_closure_truth_agreement(production, window):
    pages = LivePages(_batch(window))
    w0, w1 = window['window']
    feats = defaultdict(lambda: [0, set(), 0, None, None])
    for p in _all_pages(pages, w0, w1):
        for c in contributions(normalize(p, EPOCH)):
            f = feats[(c.token, c.window_start)]
            f[0] += 1
            if c.trader:
                f[1].add(c.trader)
            else:
                f[2] += 1
            if c.input_raw is not None:
                f[3] = (f[3] or 0) + int(c.input_raw)
            if c.output_raw is not None:
                f[4] = (f[4] or 0) + int(c.output_raw)
    got = {k: (v[0], len(v[1]), v[2]) for k, v in feats.items()}
    truth = window['truth']
    want = {(x['token'], x['window_start']): (x['swaps'], x['known_traders'], x['unknown_count'])
            for x in truth['window_aggregates_all_dex']}
    keys = set(got) | set(want)
    agreement = {'keys': len(keys), 'equal': sum(got.get(k) == want.get(k) for k in keys),
                 'swaps_equal': sum((got.get(k) or (None,))[0] == (want.get(k) or (None,))[0]
                                    for k in keys)}
    assert agreement == truth['closure_agreement'] == {'keys': 108, 'equal': 106, 'swaps_equal': 108}
    raw = {(x['token'], x['window_start']): (x['input_raw'], x['output_raw'])
           for x in truth['window_aggregates_all_dex']}
    mine = {k: (None if v[3] is None else str(v[3]), None if v[4] is None else str(v[4]))
            for k, v in feats.items()}
    assert mine == raw



def test_source_url_secrets_never_reach_live_payloads_or_logs(production, window, caplog):
    secret = 'not-a-real-key-task16-0123456789'
    responses = copy.deepcopy(window['action_responses'])
    for group in responses:
        group['url'] = f'https://indexer.invalid/api/v3/actions?api_key={secret}&limit=1000'
    caplog.set_level(logging.DEBUG)
    batch = _batch(window, actions=responses)
    pages = LivePages(batch)
    w0, w1 = window['window']
    served = _all_pages(pages, w0, w1)
    text = json.dumps([batch.payloads, served, pages.excluded(EPOCH, w0, w1)])
    assert secret not in text and 'api_key' not in text
    assert {p['metadata']['source_route'] for p in batch.payloads} <= {'/api/v3/actions',
                                                                      '/api/v3/traces'}
    assert secret not in caplog.text


def _files_containing(paths, values):
    """Relative paths of files that contain any value; the values never leave this helper."""
    return [str(p.relative_to(ROOT)) for p in paths
            if any(v in p.read_text(errors='ignore') for v in values)]


def _keyed_values():
    """Key and keyed host from the environment; empty when unset (never printed)."""
    from urllib.parse import urlparse
    key = os.environ.get('TONCENTER_API_KEY', '').strip()
    host = urlparse(os.environ.get('TONCENTER_BASE_URL', '').strip()).hostname or ''
    return [v for v in (key, host) if len(v) >= 6]


def test_toncenter_key_and_keyed_host_are_absent_from_tracked_fixtures():
    paths = [p for p in (ROOT / 'tests' / 'fixtures').rglob('*') if p.is_file()]
    assert paths
    values = _keyed_values()
    if not values:
        pytest.skip('TONCENTER_API_KEY / TONCENTER_BASE_URL not set in the environment')
    leaked = _files_containing(paths, values)
    assert leaked == [], 'key or keyed host found in: ' + ', '.join(leaked)
