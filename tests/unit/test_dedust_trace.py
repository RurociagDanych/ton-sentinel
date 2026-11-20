"""DeDust swaps recognized from complete TON Center v3 traces."""

import base64
import copy
import json

import pytest

from ton_sentinel.contracts import LIVE_PROTOCOL_VERSIONS, SourceEpoch
from ton_sentinel.normalize import contributions, normalize
from ton_sentinel.source.dedust_trace import (
    TRACE_V1, TRACE_V2, recognize_dedust_trace,
)
from ton_sentinel.source.toncenter_swaps import toncenter_swap_payload

EPOCH = SourceEpoch('mainnet', 'toncenter-v3:/api/v3/traces', 'trace_id batch, complete traces',
                    'test-build', 'dedust-trace-recognizer-v1')
USDT = 'jetton:0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe'


def fixture(prefix):
    with open('tests/fixtures/live/dedust_traces.json') as fh:
        doc = json.load(fh)
    for item in doc['traces']:
        if base64.b64decode(item['trace']['trace_id']).hex().startswith(prefix):
            return copy.deepcopy(item)
    raise KeyError(prefix)


def recognize(item, **kw):
    kw.setdefault('metadata', item['metadata'])
    return recognize_dedust_trace(item['trace'], network='mainnet', **kw)


def b64(raw_hex):
    return base64.b64encode(bytes.fromhex(raw_hex)).decode()


def test_v1_hop_missed_by_the_indexer_matches_the_hand_rules():
    result = recognize(fixture('3fa4e87f6122'))
    assert result.unresolved == ()
    (p,) = result.payloads
    assert (p['kind'], p['synthetic'], p['protocol'], p['protocol_version']) == (
        'live_swap', False, 'DeDust', TRACE_V1)
    assert p['route'] == ['native', 'jetton:0:399161023899e5b8848a675a784cf5a717c59608786d1117c626d0d35b19361e']
    assert (p['input']['raw_amount'], p['output']['raw_amount']) == ('10000000000', '138452137676923')
    assert p['output']['decimals'] == 9
    assert (p['status'], p['eligible'], p['route_proven'], p['action_end']) == (
        'success', True, True, 1794560196)
    assert p['action_id'] == b64('ee7e4f976f4c3b631e1a4b573f0d7d022f7831c5ba098cf90d7b1e738d91cc44')
    assert (p['trader'], p['trader_unambiguous']) == (
        'Mep7WN0BP+a8uHWmH5VloLiiyvKCafbbNL503tf0HIs=', True)
    action = normalize(p, EPOCH)
    assert action.eligible
    assert [c.token for c in contributions(action)] == p['route']


def test_v1_two_hop_chain_is_one_routed_trade():
    (p,) = recognize(fixture('6c75a436c9e1')).payloads
    assert p['route'] == [USDT, 'native',
                          'jetton:0:d3061b39da35c0f52ecfdf1b92bc2a21818a9b92541d2502d0f7c03736577b40']
    assert p['route_proven'] is True
    rows = contributions(normalize(p, EPOCH))
    assert [(r.token, r.role) for r in rows] == [(USDT, 'input'), (p['route'][-1], 'output')]


def test_cpmm_v2_hop_is_recognized_but_never_route_proven():
    result = recognize(fixture('b2aac189776f'))
    (p,) = result.payloads
    assert (p['protocol'], p['protocol_version']) == ('DeDust', TRACE_V2)
    assert p['route'] == ['native', USDT]
    assert (p['input']['raw_amount'], p['output']['raw_amount']) == ('3051762133', '3508108')
    assert (p['route_proven'], p['trader_unambiguous']) == (False, False)
    action = normalize(p, EPOCH)
    assert action.reasons == ('route_not_proven',)
    assert contributions(action) == ()


def test_hops_the_indexer_covers_are_not_emitted_twice_and_agree_with_it():
    item = fixture('115bd99201ce')
    (indexed,) = item['indexer_dedust_actions']
    q = toncenter_swap_payload(indexed, network='mainnet', metadata=item['metadata'])
    covered = frozenset({(q['input']['asset'], q['input']['raw_amount'])})
    assert recognize(item, covered=covered).payloads == ()
    (p,) = recognize(item).payloads
    assert p['route'] == q['route'] == ['native', p['route'][1], USDT, 'native']
    for side in ('input', 'output'):
        assert p[side] == q[side]
    assert (p['route_proven'], q['route_proven']) == (True, True)
    assert p['trader'] == q['trader'] and p['trader_unambiguous'] == q['trader_unambiguous']


def test_rejected_hop_without_swap_event_is_left_to_the_indexer():
    result = recognize(fixture('4e9b6a7d929e'))
    assert result.payloads == () and result.unresolved == ()


@pytest.mark.parametrize('mutation', ['trace_state', 'finality', 'emulated'])
def test_incomplete_or_unfinalized_trace_is_pending_and_ineligible(mutation):
    item = fixture('3fa4e87f6122')
    tx = next(iter(item['trace']['transactions'].values()))
    if mutation == 'trace_state':
        item['trace']['trace_info']['trace_state'] = 'pending'
    elif mutation == 'finality':
        tx['finality'] = 'confirmed'
    else:
        tx['emulated'] = True
    (p,) = recognize(item).payloads
    assert (p['status'], p['eligible']) == ('pending', False)
    assert 'status_not_success' in normalize(p, EPOCH).reasons


def test_chain_without_its_final_payout_is_not_route_proven():
    item = fixture('6c75a436c9e1')
    good = recognize(item).payloads[0]
    last = base64.b64decode(good['action_id']).hex()
    for tx in item['trace']['transactions'].values():
        if base64.b64decode(tx['hash']).hex() == last:
            tx['out_msgs'] = [o for o in tx['out_msgs'] if o['destination'] is None]
    (p,) = recognize(item).payloads
    assert p['route_proven'] is False
    assert normalize(p, EPOCH).reasons == ('route_not_proven',)


def test_unresolvable_v2_assets_are_reported_not_guessed():
    item = fixture('b2aac189776f')
    result = recognize(item, metadata={})
    assert result.payloads == ()
    assert len(result.unresolved) == 1


def test_trace_versions_are_registered_and_unknown_ones_rejected():
    assert LIVE_PROTOCOL_VERSIONS[TRACE_V1] == LIVE_PROTOCOL_VERSIONS[TRACE_V2] == 'DeDust'
    p = recognize(fixture('3fa4e87f6122')).payloads[0]
    with pytest.raises(ValueError):
        normalize(p | {'protocol_version': 'toncenter-v3-trace/unknown'}, EPOCH)


def test_malformed_trace_is_rejected():
    with pytest.raises(ValueError):
        recognize_dedust_trace({'trace_id': 'x'}, network='mainnet')
