"""Independent expected-state oracle for synthetic policy decision fixtures.

Deliberately imports no product contracts, identity, normalizer, or detector.
It models the documented fixture contract from raw JSON observations.
"""
from __future__ import annotations

import json
from collections import defaultdict


def _scaled(endpoint: dict) -> str | None:
    raw = endpoint['raw_amount']
    decimals = endpoint.get('decimals')
    if decimals is None:
        return None
    if decimals == 0:
        return raw
    digits = raw.rjust(decimals + 1, '0')
    whole, fraction = digits[:-decimals], digits[-decimals:]
    return whole + '.' + fraction


def _event_id(action: dict) -> str:
    key = [action['network'], action['trace_id'], action['action_id']]
    return 'ton-sentinel:event:v1:' + json.dumps(key, separators=(',', ':'))


def _reasons(action: dict) -> list[str]:
    reasons = []
    if action['protocol'] not in ('STON.fi', 'DeDust') or action['protocol_version'] != 'synthetic-v1':
        reasons.append('unsupported_protocol')
    if action['status'] != 'success':
        reasons.append('status_not_success')
    if action.get('eligible') is not True:
        reasons.append('eligibility_not_explicit')
    if action.get('route_proven') is not True:
        reasons.append('route_not_proven')
    return reasons


def _contributions(action: dict, event_id: str, eligible: bool) -> list[dict]:
    if not eligible:
        return []
    source = action['input']
    target = action['output']
    trader = (action.get('trader') if action.get('trader_unambiguous') is True
              and action.get('trader_method') is not None else None)
    start = action['action_end'] // 300 * 300
    result = []
    for asset in dict.fromkeys((source['asset'], target['asset'])):
        incoming = source if source['asset'] == asset else None
        outgoing = target if target['asset'] == asset else None
        result.append({
            'event_id': event_id,
            'token': asset,
            'role': 'both' if incoming and outgoing else 'input' if incoming else 'output',
            'window_start': start,
            'window_end': start + 300,
            'routed_trade_id': event_id,
            'count_eligible': True,
            'trader': trader,
            'trader_eligible': trader is not None,
            'unit': 'raw:' + asset,
            'input_raw': incoming['raw_amount'] if incoming else None,
            'output_raw': outgoing['raw_amount'] if outgoing else None,
            'input_scaled': _scaled(incoming) if incoming else None,
            'output_scaled': _scaled(outgoing) if outgoing else None,
        })
    return result


def expected_economic_features(actions: list[dict]) -> list[dict]:
    """Aggregate supported synthetic actions into raw-unit feature rows.

    Input is fixture source data rather than normalized product output. This
    makes event deduplication and endpoint aggregation independent of Spark.
    """
    by_event = {}
    for action in actions:
        if (action.get('protocol') not in ('STON.fi', 'DeDust')
                or action.get('protocol_version') != 'synthetic-v1'
                or action.get('status') != 'success'
                or action.get('eligible') is not True
                or action.get('route_proven') is not True):
            continue
        event = (action['network'], action['trace_id'], action['action_id'])
        if event in by_event and by_event[event] != action:
            raise ValueError('conflicting synthetic source actions share an event')
        by_event[event] = action

    aggregates = defaultdict(lambda: {
        'swaps': 0, 'input_raw': 0, 'output_raw': 0,
        'has_input': False, 'has_output': False,
        'known_traders': set(), 'unknown_count': 0,
    })
    for (network, _trace, _action_id), action in sorted(by_event.items()):
        start = action['action_end'] // 300 * 300
        trader = (action.get('trader') if action.get('trader_unambiguous') is True
                  and action.get('trader_method') is not None else None)
        endpoints = {}
        for side in ('input', 'output'):
            endpoint = action[side]
            row = endpoints.setdefault(endpoint['asset'], {'input_raw': 0, 'output_raw': 0})
            row[side + '_raw'] += int(endpoint['raw_amount'])
        for token, amounts in endpoints.items():
            key = (network, start, start + 300, token, 'synthetic:build')
            aggregate = aggregates[key]
            aggregate['swaps'] += 1
            aggregate['input_raw'] += amounts['input_raw']
            aggregate['output_raw'] += amounts['output_raw']
            aggregate['has_input'] |= 'input' in action and action['input']['asset'] == token
            aggregate['has_output'] |= 'output' in action and action['output']['asset'] == token
            if trader is None:
                aggregate['unknown_count'] += 1
            else:
                aggregate['known_traders'].add(trader)
    result = []
    for (network, start, end, token, generation), values in sorted(aggregates.items()):
        result.append({
            'network': network, 'window_start': start, 'window_end': end,
            'token': token, 'generation': generation, 'swaps': values['swaps'],
            'input_raw': values['input_raw'] if values['has_input'] else None,
            'output_raw': values['output_raw'] if values['has_output'] else None,
            'known_traders': len(values['known_traders']),
            'unknown_count': values['unknown_count'],
            'status': 'provisional', 'provisional': True,
        })
    return result


def _expected_action_window(policy: dict | None, start: int, end: int) -> dict:
    """Independent fixture-contract oracle; never claims source certification."""
    result = {
        'coverage_status': 'missing',
        'eligible_zero': False,
        'reason': 'mapping_unavailable',
        'source_coverage': 'unknown',
        'features': {'status': 'unsupported', 'computed': False, 'items': None},
        'alerts': {'status': 'unsupported', 'computed': False, 'items': None},
    }
    if type(start) is not int or type(end) is not int or start < 0 or start >= end:
        result['reason'] = 'invalid_window'
        return result
    if policy is None:
        return result
    if (type(policy) is not dict
            or set(policy) != {'schema_version', 'synthetic', 'mappings'}
            or type(policy.get('schema_version')) is not int
            or policy['schema_version'] != 1
            or policy.get('synthetic') is not True
            or type(policy.get('mappings')) is not list):
        result['reason'] = 'invalid_mapping'
        return result
    bounds = []
    for row in policy['mappings']:
        if (type(row) is not dict
                or set(row) != {'action_start', 'action_end', 'status'}
                or type(row.get('action_start')) is not int
                or type(row.get('action_end')) is not int
                or row['action_start'] < 0
                or row['action_start'] >= row['action_end']
                or row.get('status') != 'bounded_observed'):
            result['reason'] = 'invalid_mapping'
            return result
        bounds.append((row['action_start'], row['action_end']))
    bounds.sort(key=lambda pair: (pair[0], pair[1]))
    for index in range(1, len(bounds)):
        if bounds[index][0] < bounds[index - 1][1]:
            result['reason'] = 'invalid_mapping'
            return result
    position = start
    for lower, upper in bounds:
        if upper <= position:
            continue
        if lower > position:
            break
        position = upper
        if position >= end:
            result['coverage_status'] = 'bounded_observed'
            result['reason'] = 'explicit_mapping_covers_window'
            return result
    result['reason'] = 'mapping_gap'
    return result


def expected_state(truth: dict, decisions_input: list[dict]) -> dict:
    """Compute canonical selected rows, independent contributions and coverage."""
    if (truth.get('synthetic') is not True
            or type(truth.get('observations')) is not list
            or type(decisions_input) is not list):
        raise ValueError('explicit synthetic truth and decisions are required')
    observations = {}
    for item in truth['observations']:
        candidate = item['candidate_id']
        if type(candidate) is not str or not candidate or candidate in observations:
            raise ValueError('candidate IDs must be unique nonempty strings')
        observations[candidate] = item['action']
    decisions = {}
    for row in decisions_input:
        if type(row) is not dict:
            raise ValueError('decision must be an object')
        candidate = row.get('candidate_id')
        if (candidate not in observations or candidate in decisions
                or row.get('decision') not in ('accepted', 'rejected')
                or type(row.get('authority')) is not str or not row['authority'].strip()
                or type(row.get('reason')) is not str or not row['reason'].strip()):
            raise ValueError('invalid, duplicate, or unauthoritative policy decision')
        decisions[candidate] = row['decision']
    missing = sorted(observations.keys() - decisions.keys())
    accepted = [candidate for candidate, choice in decisions.items() if choice == 'accepted']
    rejected = [candidate for candidate, choice in decisions.items() if choice == 'rejected']

    selected_by_event = {}
    for candidate in accepted:
        action = observations[candidate]
        event = _event_id(action)
        if event in selected_by_event:
            raise ValueError('multiple accepted candidates for one event')
        selected_by_event[event] = (candidate, action)

    canonical = []
    for event, (candidate, action) in sorted(
            selected_by_event.items(), key=lambda item: item[1][0]):
        reasons = _reasons(action)
        canonical.append({
            'candidate_id': candidate,
            'event_id': event,
            'eligible': not reasons,
            'reasons': reasons,
            'endpoints': [action['input']['asset'], action['output']['asset']],
            'contributions': _contributions(action, event, not reasons),
        })
    result = {
        'canonical': canonical,
        'status': {
            'decision_coverage': 'complete' if not missing else 'incomplete',
            'accepted': len(accepted),
            'rejected': len(rejected),
            'missing': missing,
            'source_coverage': 'unknown',
        },
        'features': {'status': 'unsupported', 'computed': False, 'items': None},
        'alerts': {'status': 'unsupported', 'computed': False, 'items': None},
    }
    if 'coverage_policy' in truth or 'action_window' in truth:
        window = truth.get('action_window')
        if (type(window) is not list or len(window) != 2):
            result['coverage'] = _expected_action_window(None, 0, 0)
        else:
            result['coverage'] = _expected_action_window(
                truth.get('coverage_policy'), window[0], window[1])
    return result


_DETECTOR_DEFAULTS = {
    'baseline_seconds': 604800,
    'minimum_baseline_windows': 24,
    'score_threshold': 6.0,
    'swap_floor': 10,
    'trader_floor': 5,
    'attribution_floor': 0.90,
    'mad_multiplier': 1.4826,
}


def _reference_median(values: list[float]) -> float:
    """Median from order statistics, independent of production detector code."""
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[middle])
    return (float(ordered[middle - 1]) + float(ordered[middle])) / 2.0


def expected_detector_result(current: dict, history: list[dict], config: dict | None = None) -> dict:
    """Independent reference for provisional robust-MAD mechanics."""
    policy = dict(_DETECTOR_DEFAULTS)
    if config is not None:
        policy.update(config)
    target = dict(current)
    target.setdefault('eligible_zero', False)

    def eligible(row):
        return (row.get('provisional') is True and row.get('status') == 'provisional'
                and row.get('coverage_status') == 'bounded_observed'
                and (row.get('swaps', 0) > 0 or row.get('eligible_zero') is True))

    def attribution(row):
        return ((row['swaps'] - row['unknown_count']) / row['swaps']
                if row['swaps'] else 1.0)

    def lineage(rows):
        return [{'network': row['network'], 'token': row['token'],
                 'window_start': row['window_start'], 'window_end': row['window_end'],
                 'generation': row['generation']} for row in rows]

    def metric(values, observed):
        center = _reference_median(values)
        spread = _reference_median([abs(value - center) for value in values])
        return center, (observed - center) / max(1.0, policy['mad_multiplier'] * spread)

    same_asset = [row for row in history
                  if row['network'] == target['network'] and row['token'] == target['token']]
    cutoff = target['window_start'] - policy['baseline_seconds']
    baseline = sorted((row for row in same_asset
                       if cutoff <= row['window_start'] < target['window_start'] and eligible(row)),
                      key=lambda row: row['window_start'])
    full_lineage = lineage(baseline)
    empty = {'version': 'robust-mad-v1', 'status': 'missing', 'score': None,
             'observed_value': None, 'baseline_count': len(baseline),
             'baseline_keys': full_lineage, 'triggered': False, 'reasons': []}
    feature_key = {key: target[key] for key in
                   ('network', 'token', 'window_start', 'window_end', 'generation')}
    if not eligible(target):
        reason = ('missing_coverage' if target.get('coverage_status') in (None, 'missing', 'unsupported')
                  else 'ineligible_feature')
        missing = {**empty, 'reasons': [reason]}
        return {'version': 'robust-mad-v1', 'config': policy, 'feature_key': feature_key,
                'source_status': 'provisional', 'count': missing,
                'unique_traders': {**missing}}

    wallet_baseline = [row for row in baseline
                       if attribution(row) >= policy['attribution_floor']]
    amount = attribution(target)
    if len(baseline) < policy['minimum_baseline_windows']:
        wallet_status = ('insufficient_coverage' if amount < policy['attribution_floor'] else 'cold_start')
        wallet_empty = {**empty, 'baseline_count': len(wallet_baseline),
                        'baseline_keys': lineage(wallet_baseline), 'status': wallet_status,
                        'observed_value': target['known_traders'], 'attribution': amount,
                        'reasons': ['insufficient_prior_windows']}
        return {'version': 'robust-mad-v1', 'config': policy, 'feature_key': feature_key,
                'source_status': 'provisional',
                'count': {**empty, 'status': 'cold_start', 'observed_value': target['swaps'],
                          'reasons': ['insufficient_prior_windows']},
                'unique_traders': wallet_empty}

    center, count_score = metric([row['swaps'] for row in baseline], target['swaps'])
    count_reasons = []
    if target['swaps'] < policy['swap_floor']:
        count_reasons.append('below_swap_floor')
    if count_score < policy['score_threshold']:
        count_reasons.append('below_score_threshold')
    wallet_reasons = []
    if amount < policy['attribution_floor']:
        wallet_reasons.append('insufficient_trader_attribution')
    if target['known_traders'] < policy['trader_floor']:
        wallet_reasons.append('below_trader_floor')
    wallet_status = 'cold_start'
    if len(wallet_baseline) < policy['minimum_baseline_windows']:
        wallet_center, wallet_score = None, None
        wallet_status = 'cold_start'
        wallet_reasons.append('insufficient_prior_windows')
    else:
        wallet_center, wallet_score = metric([row['known_traders'] for row in wallet_baseline],
                                             target['known_traders'])
        if wallet_score < policy['score_threshold']:
            wallet_reasons.append('below_score_threshold')
    if amount < policy['attribution_floor']:
        wallet_status = 'insufficient_coverage'
    elif len(wallet_baseline) >= policy['minimum_baseline_windows']:
        wallet_status = 'scored'
    common = {'version': 'robust-mad-v1', 'baseline_count': len(baseline),
              'baseline_keys': full_lineage}
    count_result = {**common, 'status': 'scored', 'observed_value': target['swaps'],
                    'baseline_median': center, 'score': count_score,
                    'triggered': not count_reasons, 'reasons': count_reasons}
    wallet_result = {**common, 'baseline_count': len(wallet_baseline),
                     'baseline_keys': lineage(wallet_baseline), 'status': wallet_status,
                     'observed_value': target['known_traders'], 'baseline_median': wallet_center,
                     'score': wallet_score, 'attribution': amount,
                     'triggered': not wallet_reasons, 'reasons': wallet_reasons}
    return {'version': 'robust-mad-v1', 'config': policy, 'feature_key': feature_key,
            'source_status': 'provisional', 'count': count_result,
            'unique_traders': wallet_result}


# ------------------------------------------------------------------------------------------
# Independent oracle for the two v1 detectors over a pinned baseline input.
#
# Input is the stored record of a pinned baseline input (``baseline_inputs`` columns, cells
# run-length encoded) and the stored detector policy body (``detector_policies.body``).
# Written from the detector spec and the detector decisions;
# it imports no product module and uses exact
# rational arithmetic (fractions.Fraction) only.
from fractions import Fraction as _F

_ORACLE_DETECTORS = (
    ('swap_count_spike.v1', 'swap_count', 'swaps', 'swap_floor'),
    ('active_wallets_spike.v1', 'unique_traders', 'wallets', 'trader_floor'),
)


def _o_text(value):
    """Exact rational -> decimal text rounded half-even to 9 places, no trailing zeros."""
    if value is None:
        return None
    scaled = round(_F(value) * 10 ** 9)          # Fraction.__round__ is half-even
    sign = '-' if scaled < 0 else ''
    whole, frac = divmod(abs(scaled), 10 ** 9)
    text = sign + str(whole)
    if frac:
        text += '.' + str(frac).rjust(9, '0').rstrip('0')
    return '0' if text == '-0' else text


def _o_median(values):
    ordered = sorted(_F(v) for v in values)
    half = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[half]
    return (ordered[half - 1] + ordered[half]) / 2


def _o_expand(runs):
    out = []
    for run in runs:
        for start in range(run['start'], run['end'], 300):
            out.append(dict(run, start=start, end=start + 300))
    return out


def _o_ranges(starts):
    ranges = []
    for start in starts:
        if ranges and ranges[-1][1] == start:
            ranges[-1][1] = start + 300
        else:
            ranges.append([start, start + 300])
    return ranges


def _o_attr_ok(cell, floor):
    if cell['swaps'] == 0:
        return True
    return _F(cell['swaps'] - cell['unknown_count'], cell['swaps']) >= floor


def expected_detector_outcome(record: dict, policy: dict) -> list[dict]:
    """Both detector results for one pinned input, in policy detector order."""
    cells = _o_expand(record['cells'])
    current = record['current_cell']
    eligible = [c for c in cells if c['state'] in ('observed', 'zero')]
    threshold = _F(policy['score_threshold'])
    attr_floor = _F(policy['attribution_floor'])
    if current['state'] in ('missing', 'partial', 'pre_universe'):
        base_status = 'missing'
    elif current['state'] == 'provisional' or any(c['state'] == 'provisional' for c in cells):
        base_status = 'pending'
    elif len(eligible) < policy['min_prior_windows']:
        base_status = 'cold_start'
    else:
        base_status = 'ready'
    usable = current['state'] in ('observed', 'zero')
    results = []
    for detector, metric, unit, floor_name in _ORACLE_DETECTORS:
        wallet = metric == 'unique_traders'
        field = 'known_traders' if wallet else 'swaps'
        sample = [c for c in eligible if not wallet or _o_attr_ok(c, attr_floor)]
        observed = current[field] if usable else None
        attribution = None
        if wallet and usable:
            attribution = (_F(1) if current['swaps'] == 0 else
                           _F(current['swaps'] - current['unknown_count'], current['swaps']))
        median = mad = scale = score = None
        if base_status == 'missing':
            status, reasons = 'missing', ['current_' + current['state']]
        elif base_status == 'pending':
            status = 'pending'
            reasons = ['current_provisional' if current['state'] == 'provisional'
                       else 'baseline_provisional']
        elif wallet and attribution < attr_floor:
            status, reasons = 'insufficient_coverage', ['insufficient_trader_attribution']
        elif len(sample) < policy['min_prior_windows']:
            status, reasons = 'cold_start', ['insufficient_prior_windows']
        else:
            status, reasons = 'scored', []
            values = [c[field] for c in sample]
            median = _o_median(values)
            mad = _o_median([abs(_F(v) - median) for v in values])
            scale = max(_F(policy['scale_floor']), _F(policy['mad_multiplier']) * mad)
            score = (_F(observed) - median) / scale
            if observed < policy[floor_name]:
                reasons.append('below_trader_floor' if wallet else 'below_swap_floor')
            if _F(_o_text(score)) < threshold:
                reasons.append('below_score_threshold')
        triggered = status == 'scored' and not reasons
        result = {
            'detector': detector, 'metric': metric, 'unit': unit,
            'network': record['network'], 'token': record['token'],
            'direction': record['direction'], 'window_start': record['window_start'],
            'window_end': record['window_start'] + 300,
            'policy_id': record['policy_id'], 'input_digest': record['input_digest'],
            'dependency_epoch': record['dependency_epoch'],
            'universe_version': record['universe_version'],
            'certification': record['certification'], 'baseline_status': base_status,
            'current_state': current['state'], 'current_revision': current['revision'],
            'status': status, 'triggered': triggered, 'observed': observed,
            'attribution': _o_text(attribution),
            'sample_count': len(sample),
            'sample_windows': _o_ranges([c['start'] for c in sample]),
            'median': _o_text(median), 'mad': _o_text(mad), 'scale': _o_text(scale),
            'score': _o_text(score), 'threshold': policy['score_threshold'],
            'floor': policy[floor_name], 'min_prior_windows': policy['min_prior_windows'],
            'lookback_seconds': policy['lookback_seconds'], 'reasons': reasons,
        }
        head = (f"{detector} {record['network']}/{record['token']} "
                f"[{record['window_start']},{record['window_start'] + 300})")
        if status == 'scored':
            text = (f"{head}: observed {observed} {unit} vs baseline median {result['median']}, "
                    f"MAD {result['mad']}, scale {result['scale']} from {len(sample)} eligible "
                    f"prior windows (lookback {policy['lookback_seconds']}s); score "
                    f"{result['score']}, threshold {policy['score_threshold']}, floor "
                    f"{policy[floor_name]}")
            if wallet:
                text += f", attribution {result['attribution']} (floor {policy['attribution_floor']})"
            text += ': ACTIVE' if triggered else f": not triggered ({', '.join(reasons)})"
        else:
            text = (f"{head}: {status} ({', '.join(reasons)}); {len(sample)} eligible prior "
                    f"windows, {policy['min_prior_windows']} required")
        result['explanation'] = text + f"; input {record['input_digest']}"
        results.append(result)
    return results
