"""Swap payload -> ``Action``: reviewed ``live_swap`` adapter output, or test-only synthetic_swap v1.

Production accepts only ``live_swap`` (source/toncenter_swaps.py, source/dedust_trace.py); a
``synthetic_swap`` payload raises ``SyntheticInputRefused`` unless the test-only fixture switch is
set.

Required: schema_version=1, kind=synthetic_swap, synthetic=true; exact identity,
protocol/version, action_end (UTC integer seconds), status, route (2..64 ordered
opaque asset IDs), input/output {asset,raw_amount,decimals,decimal_provenance}.
Optional eligibility/grouping/trader assertions default unavailable (fail closed).
Only metadata is cosmetic and ignored. Raw amounts admit 0..10**38-1, decimals
0..38 with explicit provenance, never assumed. No USD or cross-unit arithmetic.
"""
from dataclasses import asdict
import hashlib
import json
from typing import Any

from ton_sentinel.contracts import (
    LIVE_PROTOCOL_VERSIONS, Action, ActionIdentity, Contribution, Endpoint, SourceEpoch,
    refuse_synthetic,
)


def normalize(payload: dict[str, Any], epoch: SourceEpoch) -> Action:
    if type(payload) is not dict or type(epoch) is not SourceEpoch:
        raise ValueError('synthetic payload and SourceEpoch required')
    # Validate original IDs first, before inspecting economic fields or converting.
    try:
        identity = ActionIdentity(payload['network'], payload['trace_id'], payload['action_id'])
    except KeyError as exc:
        raise ValueError('missing source identity') from exc
    required = {'schema_version', 'kind', 'synthetic', 'network', 'trace_id',
                'action_id', 'protocol', 'protocol_version', 'action_end',
                'status', 'route', 'input', 'output'}
    optional = {'eligible', 'route_proven', 'trader', 'trader_method',
                'trader_unambiguous', 'metadata'}
    if (not required <= payload.keys() or payload.keys() - required - optional
            or type(payload['schema_version']) is not int
            or payload['schema_version'] != 1):
        raise ValueError('unsupported synthetic input schema')
    if payload['kind'] == 'live_swap':
        # Reviewed live adapter output (source/toncenter_swaps.py): never synthetic and only
        # an exact, registered adapter version; same fields as the synthetic contract.
        if (payload['synthetic'] is not False
                or payload['protocol_version'] not in LIVE_PROTOCOL_VERSIONS):
            raise ValueError('unsupported live input schema')
    elif payload['kind'] != 'synthetic_swap' or payload['synthetic'] is not True:
        raise ValueError('unsupported synthetic input schema')
    else:
        # Synthetic fixtures are test-only; the production path refuses them explicitly.
        refuse_synthetic('synthetic_swap input')
    endpoints = []
    for name in ('input', 'output'):
        data = payload[name]
        if (type(data) is not dict or not {'asset', 'raw_amount'} <= data.keys()
                or data.keys() - {'asset', 'raw_amount', 'decimals', 'decimal_provenance'}):
            raise ValueError('invalid synthetic endpoint schema')
        endpoints.append(Endpoint(data['asset'], data['raw_amount'],
                                  data.get('decimals'), data.get('decimal_provenance')))
    if type(payload['route']) is not list:
        raise ValueError('ordered route list required')
    return Action(identity, epoch, payload['protocol'], payload['protocol_version'],
                  payload['action_end'], payload['status'], payload.get('eligible'),
                  payload.get('route_proven'), tuple(payload['route']), endpoints[0], endpoints[1],
                  payload.get('trader'), payload.get('trader_method'),
                  payload.get('trader_unambiguous'))


def contributions(action: Action) -> tuple[Contribution, ...]:
    if type(action) is not Action:
        raise ValueError('Action required')
    if not action.eligible:
        return ()
    trader = (action.trader if action.trader_unambiguous is True
              and action.trader_method is not None else None)
    start = action.action_end // 300 * 300
    rows = []
    for token in dict.fromkeys((action.input.asset, action.output.asset)):
        incoming = action.input if token == action.input.asset else None
        outgoing = action.output if token == action.output.asset else None
        rows.append(Contribution(
            event_id=action.identity.event_id, token=token,
            role='both' if incoming and outgoing else 'input' if incoming else 'output',
            window_start=start, window_end=start + 300,
            routed_trade_id=action.identity.event_id, count_eligible=True,
            trader=trader, trader_eligible=trader is not None, unit='raw:' + token,
            input_raw=incoming.raw_amount if incoming else None,
            output_raw=outgoing.raw_amount if outgoing else None,
            input_scaled=incoming.scaled if incoming else None,
            output_scaled=outgoing.scaled if outgoing else None))
    return tuple(rows)


def semantic_digest(action: Action) -> str:
    if type(action) is not Action:
        raise ValueError('Action required')
    data = asdict(action)
    # Source/build provenance is lineage, not economic content.
    del data['epoch']
    encoded = json.dumps({'semantic_schema': 'synthetic-action-v1', **data},
                         ensure_ascii=True, sort_keys=True, separators=(',', ':'))
    return 'sha256:' + hashlib.sha256(encoded.encode('ascii')).hexdigest()
