"""Fail-closed normalization for one bounded, read-only TON trace lookup."""
from dataclasses import dataclass
import json

from ton_sentinel.contracts import SourceEpoch


@dataclass(frozen=True)
class TraceLookup:
    """Evidence for one supplied trace ID; never certifies global coverage."""

    trace_id: str
    state: str
    action_keys: tuple[str, ...] | None
    global_coverage: bool = False


def lookup_trace(client, epoch, trace_id):
    """Return known-trace evidence, withholding keys unless classification is verified."""
    payload = client.fetch_trace(epoch, trace_id, include_actions=True)
    if type(payload) is not dict or type(payload.get("traces")) is not list:
        raise ValueError("trace lookup response lacks a traces array")
    traces = payload["traces"]
    if len(traces) != 1:
        raise ValueError("trace lookup returned no matching trace")
    trace = traces[0]
    if type(trace) is not dict or trace.get("trace_id") != trace_id:
        raise ValueError("trace lookup returned a different or malformed trace")
    info = trace.get("trace_info")
    if type(trace.get("is_incomplete")) is not bool or type(info) is not dict:
        raise ValueError("trace completeness and trace_info are required")
    if trace["is_incomplete"]:
        return TraceLookup(trace_id, "incomplete", None)
    trace_state = info.get("trace_state")
    if trace_state != "complete":
        raise ValueError("unsupported or incomplete trace_state")
    classification = info.get("classification_state")
    if classification == "unclassified":
        return TraceLookup(trace_id, "unclassified", None)
    if classification != "classified":
        raise ValueError("unsupported classification_state")
    actions = trace.get("actions")
    if type(actions) is not list:
        raise ValueError("classified trace must contain an actions array")
    keys = []
    for action in actions:
        if type(action) is not dict:
            raise ValueError("trace action must be an object")
        action_id = action.get("action_id")
        if type(action_id) is not str or not action_id:
            raise ValueError("classified trace action lacks a verified action_id")
        keys.append(action_id)
    return TraceLookup(trace_id, "classified_complete", tuple(keys))


def lookup_and_persist_trace(connection, client, range_id, trace_id, *, source_epoch=None):
    """Persist one bounded lookup only against the exact completed scan lineage."""
    row = connection.execute(
        "SELECT epoch,start_inclusive,end_exclusive,status FROM source_ranges "
        "WHERE range_id=%s FOR SHARE", (range_id,),
    ).fetchone()
    if row is None:
        raise ValueError("unknown scan range")
    epoch_payload, start, end, status = row
    if status not in ("complete", "accepted"):
        raise ValueError("trace lookup requires a complete scan range")
    if type(epoch_payload) is dict:
        epoch_payload = json.dumps(epoch_payload, separators=(",", ":"), sort_keys=True)
    lineage_epoch = SourceEpoch.from_json(epoch_payload)
    if source_epoch is not None and source_epoch != lineage_epoch:
        raise ValueError("source epoch mismatch with scan range")
    result = lookup_trace(client, lineage_epoch, trace_id)
    keys = None if result.action_keys is None else list(result.action_keys)
    connection.execute(
        "INSERT INTO trace_lookup_evidence "
        "(range_id,trace_id,epoch,start_inclusive,end_exclusive,state,action_keys,global_coverage) "
        "VALUES (%s,%s,%s::jsonb,%s,%s,%s,%s::jsonb,false)",
        (range_id, result.trace_id, lineage_epoch.to_json(), start, end, result.state,
         None if keys is None else json.dumps(keys, separators=(",", ":"))),
    )
    return result
