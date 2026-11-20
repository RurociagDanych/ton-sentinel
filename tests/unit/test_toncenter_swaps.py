"""Live TON Center v3 `jetton_swap` adapter -> product swap contract."""

import base64
import copy
import json

import pytest

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.normalize import contributions, normalize
from ton_sentinel.source.toncenter_swaps import (
    LIVE_PROTOCOL_VERSIONS, UnsupportedRecord, redact_url, toncenter_swap_payload,
)

EPOCH = SourceEpoch('mainnet', 'toncenter-v3:/api/v3/actions', 'action_type=jetton_swap',
                    'test-build', 'live-adapter-v1')
JETTON = '0:5DAF96FB72C65B5E10E7439537EB86E92F29477E4F0059CBA0F13C21CC231572'
USDT = '0:B113A994B5024A16719F69139328EB759596C38A25F59028B146FECDC3621DFE'
MID = '0:BDF3FA8098D129B54B4F73B5BAC5D1E1FD91EB054169C3916DFC8CCD536D1000'
PTON = '0:671963027F7F85659AB55B821671688601CDCF1EE674FC7FBBB1A776A18D34A3'
USER = '0:F2319F068BC89B1137525085CC04858DED836FAB242EE1C8B463C2BB93D9CDC8'
VAULT = '0:DAE153A74D894BBC32748198CD626E4F5DF4A69AD2FA56CE80FC2644B5708D20'
METADATA = {
    JETTON: {'is_indexed': True, 'token_info': [
        {'valid': True, 'type': 'jetton_masters', 'symbol': 'X', 'extra': {'decimals': '9'}}]},
    USDT: {'is_indexed': True, 'token_info': [
        {'valid': True, 'type': 'jetton_masters', 'symbol': 'USDT', 'extra': {'decimals': '6'}}]},
}
SECRET = 'not-a-real-key-0123456789abcdef'


def record(**changes):
    rec = {
        'trace_id': 'g2QCRwCbef3Xuk+zCaedN9gdi46Cl1/RI4R2zdd1SOI=',
        'action_id': '9ZDr0ZiCA70XaXy5sxXH9u4M9F8g6yx6vjkwTdCtGbA=',
        'start_lt': '116162923000004', 'end_lt': '116162923000016',
        'start_utime': 1794480600, 'end_utime': 1794480600,
        'trace_end_lt': '116162923000016', 'trace_end_utime': 1794480600,
        'trace_mc_seqno_end': 104954071, 'success': True, 'type': 'jetton_swap',
        'finality': 'finalized',
        'details': {
            'dex': 'dedust', 'sender': USER, 'asset_in': JETTON, 'asset_out': None,
            'dex_incoming_transfer': {'asset': JETTON, 'source': USER, 'destination': VAULT,
                                      'source_jetton_wallet': None,
                                      'destination_jetton_wallet': None,
                                      'amount': '1068858253805'},
            'dex_outgoing_transfer': {'asset': None, 'source': VAULT, 'destination': USER,
                                      'source_jetton_wallet': None,
                                      'destination_jetton_wallet': None,
                                      'amount': '61723377211'},
            'peer_swaps': []},
        'trace_external_hash': 'nGRhT5d2fbq5QTMaftN/7xS8JNLuLGGhONxjlwaZlsY=',
    }
    for key, value in changes.items():
        if key in rec['details']:
            rec['details'][key] = value
        else:
            rec[key] = value
    return rec


def adapt(rec, **kw):
    kw.setdefault('metadata', METADATA)
    kw.setdefault('native_aliases', frozenset({PTON}))
    return toncenter_swap_payload(rec, network='mainnet', **kw)


def synthetic_fixture_keys():
    with open('tests/fixtures/truth/actions.json') as fh:
        return set(json.load(fh)['actions'][0])


def test_payload_has_the_synthetic_fixture_fields_and_normalizes_like_truth():
    p = adapt(record())
    assert set(p) == synthetic_fixture_keys()
    assert (p['kind'], p['synthetic'], p['schema_version']) == ('live_swap', False, 1)
    assert (p['protocol'], p['protocol_version']) == ('DeDust', 'toncenter-v3/dedust')
    assert p['protocol_version'] in LIVE_PROTOCOL_VERSIONS
    action = normalize(p, EPOCH)
    assert action.eligible and action.reasons == ()
    assert action.route == ('jetton:0:' + JETTON[2:].lower(), 'native')
    rows = contributions(action)
    assert [(r.token, r.role, r.input_raw, r.input_scaled, r.output_raw, r.output_scaled)
            for r in rows] == [
        ('jetton:0:' + JETTON[2:].lower(), 'input', '1068858253805', '1068.858253805', None, None),
        ('native', 'output', None, None, '61723377211', '61.723377211')]
    trader = base64.b64encode(bytes.fromhex(USER[2:])).decode()
    assert all(r.trader == trader and r.window_start == 1794480600 for r in rows)


def test_multihop_route_comes_from_linked_peer_swaps():
    rec = record(asset_in=None, asset_out=None, peer_swaps=[
        {'asset_in': None, 'amount_in': '250000000', 'asset_out': MID, 'amount_out': '206885030'},
        {'asset_in': MID, 'amount_in': '206885030', 'asset_out': USDT, 'amount_out': '303044'},
        {'asset_in': USDT, 'amount_in': '303044', 'asset_out': None, 'amount_out': '263001601'}])
    rec['details']['dex_incoming_transfer'].update(asset=None, amount='250000000')
    rec['details']['dex_outgoing_transfer'].update(asset=None, amount='263001601')
    action = normalize(adapt(rec), EPOCH)
    assert action.route == ('native', 'jetton:0:' + MID[2:].lower(),
                            'jetton:0:' + USDT[2:].lower(), 'native')
    assert action.eligible
    rows = contributions(action)
    assert [(r.token, r.role) for r in rows] == [('native', 'both')]


def test_broken_peer_chain_is_not_route_proven():
    rec = record(asset_in=None, asset_out=USDT, peer_swaps=[
        {'asset_in': None, 'amount_in': '250000000', 'asset_out': MID, 'amount_out': '206885030'},
        {'asset_in': MID, 'amount_in': '1', 'asset_out': USDT, 'amount_out': '303044'}])
    rec['details']['dex_incoming_transfer'].update(asset=None, amount='250000000')
    rec['details']['dex_outgoing_transfer'].update(asset=USDT, amount='303044')
    action = normalize(adapt(rec), EPOCH)
    assert action.reasons == ('route_not_proven',)
    assert contributions(action) == ()


def test_endpoint_disagreeing_with_transfer_is_not_route_proven():
    rec = record()
    rec['details']['dex_outgoing_transfer']['asset'] = USDT
    assert normalize(adapt(rec), EPOCH).reasons == ('route_not_proven',)


def test_failed_swap_keeps_input_and_intended_output_with_zero_amount():
    rec = record(success=False, asset_in=None, asset_out=USDT)
    rec['details']['dex_incoming_transfer'].update(asset=None, amount='6641071464')
    rec['details']['dex_outgoing_transfer'].update(asset=None, amount='6641071464')
    p = adapt(rec)
    assert p['status'] == 'failed'
    assert (p['output']['asset'], p['output']['raw_amount']) == ('jetton:0:' + USDT[2:].lower(), '0')
    action = normalize(p, EPOCH)
    assert action.reasons == ('status_not_success',)
    assert contributions(action) == ()


def test_pending_record_is_not_eligible():
    p = adapt(record(finality='pending', success=None))
    assert (p['status'], p['eligible']) == ('pending', False)
    reasons = normalize(p, EPOCH).reasons
    assert 'status_not_success' in reasons and 'eligibility_not_explicit' in reasons


def test_pton_alias_is_native_and_decimals_are_never_assumed():
    rec = record(asset_in=PTON, asset_out=MID)
    rec['details']['dex_incoming_transfer'].update(asset=PTON, amount='5')
    rec['details']['dex_outgoing_transfer'].update(asset=MID, amount='7')
    p = adapt(rec)
    assert p['input'] == {'asset': 'native', 'raw_amount': '5', 'decimals': 9,
                          'decimal_provenance': 'ton-protocol:native-nanoton'}
    assert p['output'] == {'asset': 'jetton:0:' + MID[2:].lower(), 'raw_amount': '7'}
    assert contributions(normalize(p, EPOCH))[1].output_scaled is None


def test_unqualified_recognizer_is_never_route_proven():
    p = adapt(record(dex='dedust_v2'))
    assert p['protocol'] == 'DeDust' and p['route_proven'] is False
    assert normalize(p, EPOCH).reasons == ('route_not_proven',)


def test_failed_swap_reporting_the_refund_as_output_is_not_route_proven():
    # The indexer's asset_out of some STON.fi refunds is the refunded input asset, so
    # the intended asset is not observable from the action; fail closed (2 records in phase 3).
    rec = record(success=False, asset_in=None, asset_out=None)
    rec['details']['dex_incoming_transfer'].update(asset=None, amount='6641071464')
    rec['details']['dex_outgoing_transfer'].update(asset=None, amount='6641071464')
    p = adapt(rec)
    assert (p['status'], p['route_proven']) == ('failed', False)
    assert normalize(p, EPOCH).reasons == ('status_not_success', 'route_not_proven')


def test_invalid_indexer_metadata_keeps_the_amount_raw_only():
    # Rule (decimals): only valid indexer jetton_masters metadata or the TON protocol
    # definition; a master marked valid:false stays raw-only, never filled from another source.
    meta = copy.deepcopy(METADATA)
    meta[JETTON]['token_info'][0]['valid'] = False
    p = adapt(record(), metadata=meta)
    assert 'decimals' not in p['input'] and p['output']['decimals'] == 9
    rows = contributions(normalize(p, EPOCH))
    assert rows[0].input_raw == '1068858253805' and rows[0].input_scaled is None


def test_custody_boundary_each_settled_leg_is_its_own_trade():
    # Rule (trade boundary): a leg whose output is paid to the trader ends a trade; a
    # following leg that the trader pays from its own wallet is a new trade, even in one trace.
    leg1 = record(asset_in=None, asset_out=MID)
    leg1['details']['dex_incoming_transfer'].update(asset=None, amount='250000000')
    leg1['details']['dex_outgoing_transfer'].update(asset=MID, amount='206885030')
    leg2 = record(action_id='Dmp5jHxyc3Q6Wcm6v0n9ZbZp0dxQe0k4sO1cS1x0aWU=', asset_in=MID, asset_out=None)
    leg2['details']['dex_incoming_transfer'].update(asset=MID, amount='206885030')
    leg2['details']['dex_outgoing_transfer'].update(asset=None, amount='263001601')
    a1, a2 = (normalize(adapt(r), EPOCH) for r in (leg1, leg2))
    mid = 'jetton:0:' + MID[2:].lower()
    assert (a1.route, a2.route) == (('native', mid), (mid, 'native'))
    assert a1.identity.event_id != a2.identity.event_id
    assert [(c.token, c.role) for c in contributions(a1)] == [('native', 'input'), (mid, 'output')]
    assert [(c.token, c.role) for c in contributions(a2)] == [(mid, 'input'), ('native', 'output')]


def test_trader_is_the_dex_facing_payer_that_receives_the_output():
    # Rule (trader): the account that pays the input into the DEX and also receives the
    # output; the external signer of the trace is not considered.
    p = adapt(record())
    assert p['trader_method'] == 'toncenter-v3:jetton_swap.sender' and p['trader_unambiguous'] is True


def test_trader_requires_sender_to_receive_the_output():
    rec = record()
    rec['details']['dex_outgoing_transfer']['destination'] = VAULT
    rows = contributions(normalize(adapt(rec), EPOCH))
    assert rows and all(r.trader is None and not r.trader_eligible for r in rows)


@pytest.mark.parametrize('mutation', ['dex', 'type', 'amount', 'identity', 'missing'])
def test_out_of_scope_or_malformed_records_are_rejected(mutation):
    rec = record()
    if mutation == 'dex':
        rec['details']['dex'] = 'unknown_dex'
    elif mutation == 'type':
        rec['type'] = 'ton_transfer'
    elif mutation == 'amount':
        rec['details']['dex_incoming_transfer']['amount'] = '-1'
    elif mutation == 'identity':
        rec['trace_id'] = 'not-an-id'
    else:
        del rec['details']['dex_outgoing_transfer']
    with pytest.raises(ValueError):
        normalize(adapt(rec), EPOCH)
    if mutation in ('dex', 'type'):
        with pytest.raises(UnsupportedRecord):
            adapt(rec)


def test_api_key_never_reaches_the_payload_or_recorded_url():
    rec = record()
    rec['api_key'] = SECRET
    url = f'https://toncenter.com/api/v3/actions?account=x&api_key={SECRET}&limit=5'
    p = adapt(rec, source_url=url)
    text = json.dumps(p)
    assert SECRET not in text and SECRET not in redact_url(url)
    assert 'api_key=[REDACTED]' in redact_url(url)
    assert p['metadata']['source_route'] == '/api/v3/actions'


def test_live_contract_is_explicit_and_synthetic_contract_unchanged():
    p = adapt(record())
    for bad in ({'synthetic': True}, {'protocol_version': 'synthetic-v1'},
                {'protocol_version': 'toncenter-v3/unknown'}, {'kind': 'synthetic_swap'}):
        with pytest.raises(ValueError):
            normalize(copy.deepcopy(p) | bad, EPOCH)
    mismatched = normalize(copy.deepcopy(p) | {'protocol': 'STON.fi'}, EPOCH)
    assert 'unsupported_protocol' in mismatched.reasons
