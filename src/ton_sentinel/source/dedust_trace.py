"""DeDust swaps from complete TON Center v3 traces -> product ``live_swap`` payloads."""
from __future__ import annotations

import base64
from typing import NamedTuple

from ton_sentinel.source.boc import Slice, body_slice
from ton_sentinel.source.toncenter_swaps import NATIVE, _endpoint

TRACE_V1 = 'toncenter-v3-trace/dedust'
TRACE_V2 = 'toncenter-v3-trace/dedust_cpmm_v2'
SWAP_EXTERNAL, SWAP_PEER = 0x61ee542d, 0x72aca8aa
SWAP_EVENT_V1, PAY_OUT_FROM_POOL = 0x9c610de3, 0xad4eb6f5
SWAP_EVENT_V2, PAY_NATIVE_V2 = 0x78e79ba4, 0xa5a7cbf8
JETTON_NOTIFY, JETTON_TRANSFER = 0x7362d09c, 0x0f8a7ea5


class TraceRecognition(NamedTuple):
    payloads: tuple
    unresolved: tuple


def _op(msg):
    body = ((msg or {}).get('message_content') or {}).get('body')
    if not body:
        return None, None
    try:
        s = body_slice(body)
    except ValueError:  # not an ordinary-cell body: cannot be a DeDust swap message
        return None, None
    if s.remaining() < 32:
        return None, None
    return s.uint(32), s


def _dedust_asset(s: Slice):
    tag = s.uint(4)
    if tag == 0:
        return NATIVE
    if tag == 1:
        wc = s.int(8)
        return f'jetton:{wc}:{s.uint(256):064x}'
    return None  # extra currency or unknown: unsupported


def _raw(addr):
    if type(addr) is not str or ':' not in addr:
        raise ValueError('raw wc:hex address required')
    wc, hexpart = addr.split(':', 1)
    return f'{int(wc)}:{hexpart.lower()}'


def _b64(raw):
    return base64.b64encode(bytes.fromhex(raw.split(':')[1])).decode('ascii')


def _master(wallet, metadata):
    """Jetton wallet -> asset ID via the indexer's ``jetton_wallets`` metadata, else None."""
    if not wallet:
        return None
    wc, hexpart = _raw(wallet).split(':')
    entry = (metadata or {}).get(f'{wc}:{hexpart.upper()}') or {}
    masters = {(ti.get('extra') or {}).get('jetton') for ti in entry.get('token_info') or []
               if ti.get('type') == 'jetton_wallets' and ti.get('valid', True)}
    masters.discard(None)
    return 'jetton:' + _raw(masters.pop()) if len(masters) == 1 else None


def _exit_ok(tx):
    desc = tx.get('description') or {}
    return (desc.get('compute_ph') or {}).get('exit_code') == 0 and desc.get('aborted') is False


def _v1_hops(txs):
    hops = {}
    for tx in txs:
        op, s = _op(tx.get('in_msg'))
        if op not in (SWAP_EXTERNAL, SWAP_PEER) or not _exit_ok(tx):
            continue
        events, payouts, peers = [], [], []
        for out in tx.get('out_msgs') or []:
            oop, os_ = _op(out)
            if out.get('destination') is None and oop == SWAP_EVENT_V1:
                a_in, a_out = _dedust_asset(os_), _dedust_asset(os_)
                amounts = (os_.coins(), os_.coins())
                events.append((a_in, a_out, amounts[0], amounts[1], os_.ref().address()))
            elif oop == PAY_OUT_FROM_POOL:
                os_.uint(64)
                os_.ref()
                payouts.append((os_.coins(), os_.address()))
            elif oop == SWAP_PEER:
                peers.append(out.get('hash'))
        if len(events) == 1:
            hops[tx['in_msg'].get('hash')] = {'tx': tx, 'start': op == SWAP_EXTERNAL,
                                              'event': events[0], 'payouts': payouts, 'peers': peers}
    return hops


def _v1_trades(txs):
    hops = _v1_hops(txs)
    trades = []
    for hop in sorted(hops.values(), key=lambda h: int(h['tx']['lt'])):
        if not hop['start']:
            continue
        chain = [hop]
        while len(chain[-1]['peers']) == 1 and chain[-1]['peers'][0] in hops and len(chain) < 64:
            chain.append(hops[chain[-1]['peers'][0]])
        events = [h['event'] for h in chain]
        if any(e[0] is None or e[1] is None for e in events):
            trades.append({'unresolved': chain[-1]['tx']['hash']})
            continue
        linked = all((b[0], b[2]) == (a[1], a[3]) for a, b in zip(events, events[1:]))
        last = chain[-1]
        closed = (not last['peers'] and len(last['payouts']) == 1
                  and last['payouts'][0][0] == events[-1][3])
        payer = events[0][4]
        recipient = last['payouts'][0][1] if closed else None
        trades.append({
            'version': TRACE_V1, 'chain': chain, 'route': [events[0][0]] + [e[1] for e in events],
            'input': (events[0][0], str(events[0][2])), 'output': (events[-1][1], str(events[-1][3])),
            'proven': linked and closed, 'payer': payer,
            'unambiguous': (linked and closed and payer is not None and payer.startswith('0:')
                            and len({e[4] for e in events}) == 1 and recipient == payer)})
    return trades


def _v2_trades(txs, metadata):
    trades = []
    for tx in txs:
        for out in tx.get('out_msgs') or []:
            oop, os_ = _op(out)
            if out.get('destination') is not None or oop != SWAP_EVENT_V2:
                continue
            os_.uint(1)
            amount_in, amount_out = os_.coins(), os_.coins()
            initiator, recipient = os_.address(), os_.address()
            in_msg = tx.get('in_msg') or {}
            iop, is_ = _op(in_msg)
            a_in = None
            if iop == JETTON_NOTIFY:
                a_in = _master(in_msg.get('source'), metadata)
            elif iop == PAY_NATIVE_V2:
                is_.uint(64)
                a_in = NATIVE if is_.coins() == amount_in else None
            sends = [(_op(o)[0], o) for o in tx.get('out_msgs') or [] if o.get('destination')]
            jettons = [o for op_, o in sends if op_ == JETTON_TRANSFER]
            natives = [o for op_, o in sends if op_ != JETTON_TRANSFER and recipient
                       and _raw(o['destination']) == recipient and int(o.get('value') or 0) >= amount_out]
            a_out = (_master(jettons[0]['destination'], metadata) if len(jettons) == 1
                     else NATIVE if not jettons and len(natives) == 1 else None)
            if a_in is None or a_out is None:
                trades.append({'unresolved': tx['hash']})
                continue
            trades.append({'version': TRACE_V2, 'chain': [{'tx': tx}], 'route': [a_in, a_out],
                           'input': (a_in, str(amount_in)), 'output': (a_out, str(amount_out)),
                           'proven': False, 'payer': initiator, 'unambiguous': False})
    return trades


def recognize_dedust_trace(trace, *, network, metadata=None, covered=frozenset(),
                           source_url=None) -> TraceRecognition:
    """Recognize DeDust swaps in one toncenter v3 trace; skip those in ``covered``."""
    if type(trace) is not dict or type(trace.get('transactions')) is not dict:
        raise ValueError('trace with transactions required')
    if type(trace.get('trace_id')) is not str:
        raise ValueError('trace_id required')
    txs = list(trace['transactions'].values())
    if any(type(tx) is not dict or type(tx.get('hash')) is not str or type(tx.get('now')) is not int
           for tx in txs):
        raise ValueError('malformed transaction')
    complete = ((trace.get('trace_info') or {}).get('trace_state') == 'complete'
                and trace.get('is_incomplete') is False)
    final = complete and all(tx.get('finality') == 'finalized' and tx.get('emulated') is not True
                             for tx in txs)
    status = 'success' if final else 'pending'
    payloads, unresolved = [], []
    for trade in _v1_trades(txs) + _v2_trades(txs, metadata):
        if 'unresolved' in trade:
            unresolved.append(trade['unresolved'])
            continue
        if trade['input'] in covered:
            continue
        chain_txs = [h['tx'] for h in trade['chain']]
        payer = trade['payer']
        on_basechain = type(payer) is str and payer.startswith('0:')
        payloads.append({
            'schema_version': 1, 'kind': 'live_swap', 'synthetic': False, 'network': network,
            'trace_id': trace['trace_id'], 'action_id': chain_txs[-1]['hash'],
            'protocol': 'DeDust', 'protocol_version': trade['version'],
            'action_end': max(tx['now'] for tx in chain_txs), 'status': status,
            'eligible': final, 'route_proven': bool(trade['proven']),
            'route': trade['route'],
            'input': _endpoint(trade['input'][0], trade['input'][1], metadata),
            'output': _endpoint(trade['output'][0], trade['output'][1], metadata),
            'trader': _b64(payer) if on_basechain else None,
            'trader_method': 'dedust-trace:swap_event.sender' if on_basechain else None,
            'trader_unambiguous': bool(trade['unambiguous']) if on_basechain else None,
            'metadata': {'source_route': '/api/v3/traces', 'recognizer': trade['version'],
                         'hops': len(chain_txs), 'trace_complete': complete},
        })
    return TraceRecognition(tuple(payloads), tuple(unresolved))
