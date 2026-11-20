"""Live source wiring: recorded or fetched TON Center v3 responses -> production pages."""
from __future__ import annotations

import json
from typing import NamedTuple

from ton_sentinel.contracts import SyntheticInputRefused
from ton_sentinel.normalize import normalize
from ton_sentinel.source.dedust_trace import recognize_dedust_trace
from ton_sentinel.source.toncenter_swaps import UnsupportedRecord, toncenter_swap_payload


class LiveBatch(NamedTuple):
    payloads: tuple
    unsupported: tuple
    unresolved: tuple


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def _unique(groups, list_key, id_key, what):
    """(record, group) pairs in input order; exact repeats collapse, conflicting ones raise."""
    seen, out = {}, []
    for group in groups:
        if type(group) is not dict or type(group.get(list_key)) is not list:
            raise ValueError(f'{what} response must contain a {list_key} array')
        for record in group[list_key]:
            if type(record) is not dict or type(record.get(id_key)) is not str:
                raise ValueError(f'{what} record requires {id_key}')
            key = record[id_key]
            if key in seen:
                if seen[key] != _canonical(record):
                    raise ValueError(f'conflicting {what} records share {id_key}')
                continue
            seen[key] = _canonical(record)
            out.append((record, group))
    return out


def live_swap_batch(action_responses, trace_responses, *, network,
                    native_aliases=frozenset()) -> LiveBatch:
    """Build the production ``live_swap`` payloads from indexer action and trace responses.

    Each response is a TON Center v3 document: ``{'actions': [...], 'metadata': {...}}`` from
    ``/api/v3/actions`` or ``{'traces': [...], 'metadata': {...}}`` from ``/api/v3/traces``, with an
    optional ``url`` of the request (only its path reaches a payload).
    """
    payloads, unsupported = [], []
    dedust_in = {}
    for record, group in _unique(action_responses, 'actions', 'action_id', 'action'):
        try:
            payload = toncenter_swap_payload(record, network=network,
                                             metadata=group.get('metadata') or {},
                                             native_aliases=native_aliases,
                                             source_url=group.get('url'))
        except UnsupportedRecord as exc:
            unsupported.append((str(exc), record['action_id']))
            continue
        payloads.append(payload)
        if payload['protocol'] == 'DeDust':
            dedust_in.setdefault(payload['trace_id'], set()).add(
                (payload['input']['asset'], payload['input']['raw_amount']))
    unresolved = []
    for trace, group in _unique(trace_responses, 'traces', 'trace_id', 'trace'):
        found = recognize_dedust_trace(trace, network=network, metadata=group.get('metadata') or {},
                                       covered=frozenset(dedust_in.get(trace['trace_id'], ())))
        payloads.extend(found.payloads)
        unresolved.extend(found.unresolved)
    ids = [(p['network'], p['trace_id'], p['action_id']) for p in payloads]
    if len(set(ids)) != len(ids):
        raise ValueError('conflicting live payloads share an action identity')
    payloads.sort(key=lambda p: (p['action_end'], p['trace_id'], p['action_id']))
    return LiveBatch(tuple(payloads), tuple(unsupported), tuple(unresolved))


class LivePages:
    """Page transport for ``ingest.ingest_range`` over one ``LiveBatch`` (no network I/O here).

    ``fetch_page`` serves the half-open window ``[start, end)``: eligible and pending records.
    """
    def __init__(self, batch: LiveBatch):
        if type(batch) is not LiveBatch:
            raise ValueError('LiveBatch required')
        for p in batch.payloads:
            if type(p) is not dict or p.get('kind') != 'live_swap' or p.get('synthetic') is not False:
                raise SyntheticInputRefused(
                    'non-live payload refused: the production path accepts only live_swap records')
        self._payloads = batch.payloads

    def _split(self, epoch, start, end):
        served, excluded = [], []
        for p in self._payloads:
            if not start <= p['action_end'] < end:
                continue
            action = normalize(p, epoch)
            if action.eligible or p['status'] == 'pending':
                served.append(p)
            else:
                excluded.append((p['trace_id'], p['action_id'], action.reasons))
        return served, excluded

    def fetch_page(self, epoch, start, end, offset, limit):
        if type(offset) is not int or type(limit) is not int or offset < 0 or limit < 1:
            raise ValueError('offset and limit must be a non-negative offset and positive limit')
        return self._split(epoch, start, end)[0][offset:offset + limit]

    def excluded(self, epoch, start, end):
        """(trace_id, action_id, reasons) of final ineligible records in ``[start, end)``."""
        return tuple(self._split(epoch, start, end)[1])
