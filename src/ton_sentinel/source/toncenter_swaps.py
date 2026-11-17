"""Live TON Center v3 ``jetton_swap`` action -> product swap payload (``live_swap`` v1).

Reviewed versioned adapter. It maps one
indexer action record to a payload with exactly the fields of the synthetic fixture
(tests/fixtures/truth/actions.json), so ``normalize()`` builds the same ``Action`` contract.

Policy (fail closed):
- Only ``type == 'jetton_swap'`` from a known DEX label is in scope; anything else raises
  ``UnsupportedRecord``. Malformed records raise ``ValueError``.
- ``status``: ``pending`` unless ``finality == 'finalized'``; then ``success``/``failed``.
- ``route_proven`` only for recognizers qualified against independent on-chain rules
  (``QUALIFIED_DEX``) and only when the endpoints equal the DEX transfers and every peer swap links
  to the next one by exact asset and amount. ``dedust_v2`` is recognized but not qualified.
- Failed swaps keep the incoming transfer as input and the intended asset with raw amount 0 as
  output (the refund is not an exchange). A failed record whose ``asset_out`` equals its input
  asset (the indexer reported the refund) is not route-proven.
- Trade boundary (custody rule): one action is one routed trade. Hops the DEX chains internally
  (``peer_swaps``) form one trade; a leg paid out to the trader ends the trade, and a later leg the
  trader pays again is a separate trade, even inside one trace.
- Trader is the DEX-facing payer (action ``sender``) and is unambiguous only when that account
  also receives the outgoing transfer; the external signer of the trace is not used; basechain
  (workchain 0) accounts only.
- Decimals come only from the response metadata (``jetton_masters.extra.decimals``) or the TON
  protocol definition; never assumed. Configured pTON masters are TON (``native_aliases``).
- Secrets: nothing from the request is copied except the redacted route path; ``redact_url``
  masks ``api_key`` query values.
"""
from __future__ import annotations

import base64
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from ton_sentinel.contracts import LIVE_PROTOCOL_VERSIONS

_PREFIX = 'toncenter-v3/'
DEX_PROTOCOL = {version[len(_PREFIX):]: proto for version, proto in LIVE_PROTOCOL_VERSIONS.items()
                if version.startswith(_PREFIX)}
QUALIFIED_DEX = frozenset({'stonfi', 'stonfi_v2', 'dedust'})
NATIVE = 'native'
NATIVE_DECIMALS = (9, 'ton-protocol:native-nanoton')
DECIMALS_PROVENANCE = 'toncenter-v3:metadata.jetton_masters.extra.decimals'
REDACTED = '[REDACTED]'
_RAW_ADDR = re.compile(r'^(-?\d+):([0-9A-Fa-f]{64})$')
_AMOUNT = re.compile(r'^(0|[1-9][0-9]*)$')


class UnsupportedRecord(ValueError):
    """The record is outside the adapter's scope (not a swap, or an unknown DEX)."""


def redact_url(url: str) -> str:
    """Return the URL with secret query values (``api_key``) masked."""
    parts = urlsplit(url)
    query = [(k, REDACTED if k.lower().replace('-', '_') == 'api_key' else v)
             for k, v in parse_qsl(parts.query, keep_blank_values=True)]
    return urlunsplit(parts._replace(query=urlencode(query, safe='[]:')))


def _addr(value, what):
    if type(value) is not str or not _RAW_ADDR.match(value):
        raise ValueError(f'{what} must be a raw wc:hex address')
    wc, hexpart = value.split(':')
    return f'{int(wc)}:{hexpart.lower()}'


def _amount(value, what):
    if type(value) is not str or not _AMOUNT.match(value):
        raise ValueError(f'{what} must be a canonical unsigned integer string')
    return value


def _asset(master, aliases):
    if master is None:
        return NATIVE
    raw = _addr(master, 'asset')
    return NATIVE if raw in aliases else 'jetton:' + raw


def _decimals(asset, metadata):
    if asset == NATIVE:
        return NATIVE_DECIMALS
    raw = asset[len('jetton:'):]
    wc, hexpart = raw.split(':')
    entry = (metadata or {}).get(f'{wc}:{hexpart.upper()}') or {}
    values = {str((ti.get('extra') or {}).get('decimals'))
              for ti in entry.get('token_info') or []
              if ti.get('type') == 'jetton_masters' and ti.get('valid', True)}
    values.discard('None')
    if len(values) == 1:
        (text,) = values
        if text.isdigit() and int(text) <= 38:
            return int(text), DECIMALS_PROVENANCE
    return None


def _endpoint(asset, raw_amount, metadata):
    data = {'asset': asset, 'raw_amount': raw_amount}
    dec = _decimals(asset, metadata)
    if dec is not None:
        data['decimals'], data['decimal_provenance'] = dec
    return data


def toncenter_swap_payload(record, *, network, metadata=None, native_aliases=frozenset(),
                           source_url=None):
    """Map one toncenter v3 action record to a ``live_swap`` payload for ``normalize()``."""
    if type(record) is not dict:
        raise ValueError('action record must be an object')
    if record.get('type') != 'jetton_swap':
        raise UnsupportedRecord('not a jetton_swap action')
    details = record.get('details')
    if type(details) is not dict:
        raise ValueError('jetton_swap details required')
    dex = details.get('dex')
    if dex not in DEX_PROTOCOL:
        raise UnsupportedRecord('unsupported dex label')
    aliases = {_addr(a, 'native alias') for a in native_aliases}
    try:
        incoming, outgoing = details['dex_incoming_transfer'], details['dex_outgoing_transfer']
        sender = details['sender']
        asset_in_raw, asset_out_raw = details['asset_in'], details['asset_out']
        end = record['end_utime']
    except KeyError as exc:
        raise ValueError('incomplete jetton_swap record') from exc
    if type(incoming) is not dict or type(outgoing) is not dict:
        raise ValueError('dex transfers required')
    if type(end) is not int:
        raise ValueError('end_utime must be an integer')
    in_amount = _amount(incoming.get('amount'), 'incoming amount')
    out_amount = _amount(outgoing.get('amount'), 'outgoing amount')
    asset_in, asset_out = _asset(asset_in_raw, aliases), _asset(asset_out_raw, aliases)
    finalized = record.get('finality') == 'finalized'
    success = record.get('success')
    status = 'pending' if not finalized else 'success' if success is True else 'failed'
    proven = dex in QUALIFIED_DEX and _asset(incoming.get('asset'), aliases) == asset_in
    route = [asset_in, asset_out]
    peers = details.get('peer_swaps') or []
    if type(peers) is not list:
        raise ValueError('peer_swaps must be a list')
    if status == 'success':
        proven = proven and _asset(outgoing.get('asset'), aliases) == asset_out
        if peers:
            prev = (asset_in, in_amount)
            for hop in peers:
                hop_in = (_asset(hop.get('asset_in'), aliases), _amount(hop.get('amount_in'), 'peer amount'))
                hop_out = (_asset(hop.get('asset_out'), aliases), _amount(hop.get('amount_out'), 'peer amount'))
                proven = proven and hop_in == prev
                prev = hop_out
            proven = proven and prev == (asset_out, out_amount)
            route = [asset_in] + [_asset(h.get('asset_out'), aliases) for h in peers[:-1]] + [asset_out]
    else:
        # A failed swap whose asset_out is the refunded input asset does not name the intended
        # asset, so its route is not observable from the action.
        proven = proven and not peers and asset_out != asset_in
    output_amount = out_amount if status == 'success' else '0'
    trader, method, unambiguous = None, None, None
    sender_raw = _addr(sender, 'sender')
    if sender_raw.startswith('0:'):
        trader = base64.b64encode(bytes.fromhex(sender_raw[2:])).decode('ascii')
        method = 'toncenter-v3:jetton_swap.sender'
        dest = outgoing.get('destination')
        unambiguous = (status == 'success' and dest is not None
                       and _addr(dest, 'destination') == sender_raw)
    meta = {'source_route': urlsplit(redact_url(source_url)).path if source_url else '/api/v3/actions',
            'dex_label': dex, 'finality': record.get('finality'),
            'trace_end_utime': record.get('trace_end_utime'),
            'end_lt': record.get('end_lt')}
    return {
        'schema_version': 1, 'kind': 'live_swap', 'synthetic': False, 'network': network,
        'trace_id': record.get('trace_id'), 'action_id': record.get('action_id'),
        'protocol': DEX_PROTOCOL[dex], 'protocol_version': 'toncenter-v3/' + dex,
        'action_end': end, 'status': status, 'eligible': finalized,
        'route_proven': bool(proven), 'route': route,
        'input': _endpoint(asset_in, in_amount, metadata),
        'output': _endpoint(asset_out, output_amount, metadata),
        'trader': trader, 'trader_method': method, 'trader_unambiguous': unambiguous,
        'metadata': meta,
    }
