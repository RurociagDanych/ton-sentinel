"""Offline v1 identity/serialization profile, not a deployed-source adapter."""
import base64
import binascii
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import os
import re
from typing import ClassVar


_EVENT_PREFIX = "ton-sentinel:event:v1:"


def _network(value: str) -> None:
    if type(value) is not str or value not in ("mainnet", "testnet"):
        raise ValueError("network must be exactly mainnet or testnet")


def _source_id(value: str) -> None:
    # Check type and representation before decoding; never coerce source IDs.
    if type(value) is not str or len(value) != 44:
        raise ValueError("source ID must be canonical base64 of 32 bytes")
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError("invalid source ID encoding") from exc
    if len(raw) != 32 or base64.b64encode(raw).decode("ascii") != value:
        raise ValueError("source ID must be canonical base64 of 32 bytes")


def _fingerprint(value: str) -> None:
    if (type(value) is not str or not value.strip()
            or any(ord(c) < 32 or 127 <= ord(c) <= 159
                   or 0xD800 <= ord(c) <= 0xDFFF for c in value)):
        raise ValueError("fingerprint must be nonempty text without controls or surrogates")


def _unique_object(pairs: list) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError("nonstandard JSON constant")


def _load_json(text: str):
    if type(text) is not str:
        raise ValueError("serialized value must be text")
    try:
        return json.loads(text, object_pairs_hook=_unique_object,
                          parse_constant=_reject_constant)
    except (ValueError, RecursionError) as exc:
        raise ValueError("invalid serialized JSON") from exc


class _Versioned:
    _kind: ClassVar[str]

    def to_json(self) -> str:
        return json.dumps({"schema_version": 1, "kind": self._kind, **asdict(self)},
                          ensure_ascii=True, separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_json(cls, text: str):
        payload = _load_json(text)
        names = {field.name for field in fields(cls)}
        if (type(payload) is not dict
                or set(payload) != names | {"schema_version", "kind"}
                or type(payload["schema_version"]) is not int
                or payload["schema_version"] != 1
                or payload["kind"] != cls._kind):
            raise ValueError("unsupported serialized schema")
        return cls(**{name: payload[name] for name in names})


@dataclass(frozen=True)
class SourceEpoch(_Versioned):
    """Exact source/filter/build/action-policy provenance, not business identity."""
    _kind: ClassVar[str] = "source_epoch"
    network: str
    source_fingerprint: str
    filter_fingerprint: str
    build_fingerprint: str
    action_policy_fingerprint: str

    def __post_init__(self) -> None:
        _network(self.network)
        for name in ("source_fingerprint", "filter_fingerprint",
                     "build_fingerprint", "action_policy_fingerprint"):
            _fingerprint(getattr(self, name))


@dataclass(frozen=True)
class ActionIdentity(_Versioned):
    """One source action within a trace and network; no amount/symbol keying."""
    _kind: ClassVar[str] = "action_identity"
    network: str
    trace_id: str
    action_id: str

    def __post_init__(self) -> None:
        _network(self.network)
        _source_id(self.trace_id)
        _source_id(self.action_id)

    @property
    def event_id(self) -> str:
        return _EVENT_PREFIX + json.dumps(
            [self.network, self.trace_id, self.action_id], separators=(",", ":"))

    @classmethod
    def from_event_id(cls, text: str):
        if type(text) is not str or not text.startswith(_EVENT_PREFIX):
            raise ValueError("unsupported event ID version")
        parts = _load_json(text[len(_EVENT_PREFIX):])
        if type(parts) is not list or len(parts) != 3:
            raise ValueError("event ID must contain network, trace ID and action ID")
        return cls(*parts)


# Bounded offline synthetic economic profile. These limits are local contract
# choices, not claims about TON, SQL storage or Spark precision support.
MAX_RAW_DIGITS = 38
MAX_DECIMALS = 38
MAX_EVENT_TIME = 253402300799
# Registered live adapter versions -> the only protocol each may assert. Owned by the reviewed
# adapter ton_sentinel.source.toncenter_swaps; anything else stays unsupported.
LIVE_PROTOCOL_VERSIONS = {'toncenter-v3/stonfi': 'STON.fi', 'toncenter-v3/stonfi_v2': 'STON.fi',
                          'toncenter-v3/dedust': 'DeDust', 'toncenter-v3/dedust_v2': 'DeDust',
                          'toncenter-v3-trace/dedust': 'DeDust',
                          'toncenter-v3-trace/dedust_cpmm_v2': 'DeDust'}

# Only real data enters the pipeline. ``synthetic-v1`` fixtures
# are test-only: the production path refuses them unless the test suite (tests/conftest.py) or
# the deployment smoke harness (k3s_probe.py) sets this switch to exactly ``test-only``. No deploy
# manifest or operator script sets it (pinned by tests/unit/test_live_ingest.py).
SYNTHETIC_VERSION = 'synthetic-v1'
SYNTHETIC_FIXTURE_ENV = 'TON_SENTINEL_SYNTHETIC_FIXTURES'
_SYNTHETIC_FIXTURE_VALUE = 'test-only'


class SyntheticInputRefused(ValueError):
    """A synthetic-v1 fixture reached the production path (it accepts only live_swap records)."""
    pass


def synthetic_fixtures_allowed() -> bool:
    """True only inside the test suite or the probe harness (exact test-only switch)."""
    return os.environ.get(SYNTHETIC_FIXTURE_ENV) == _SYNTHETIC_FIXTURE_VALUE


def refuse_synthetic(what: str) -> None:
    """Raise ``SyntheticInputRefused`` for ``what`` unless test-only fixtures are enabled."""
    if not synthetic_fixtures_allowed():
        raise SyntheticInputRefused(
            f'{what} refused: synthetic-v1 fixtures are test-only; the production path accepts '
            'only live_swap records (toncenter_swaps adapter / dedust_trace recognizer)')


def _raw(value):
    if (type(value) is not str or not 1 <= len(value) <= MAX_RAW_DIGITS
            or any(c not in '0123456789' for c in value)
            or (len(value) > 1 and value[0] == '0')):
        raise ValueError('raw amount must be a canonical unsigned 38-digit string')


def _optional_bool(value):
    if value is not None and type(value) is not bool:
        raise ValueError('expected boolean or unavailable')


@dataclass(frozen=True)
class Endpoint(_Versioned):
    """Exact synthetic asset/unit and explicitly declared decimal provenance."""
    _kind: ClassVar[str] = 'synthetic_endpoint'
    asset: str
    raw_amount: str
    decimals: int | None
    decimal_provenance: str | None

    def __post_init__(self):
        _fingerprint(self.asset)
        _raw(self.raw_amount)
        if self.decimals is not None:
            if type(self.decimals) is not int or not 0 <= self.decimals <= MAX_DECIMALS:
                raise ValueError('decimals outside synthetic bound')
            _fingerprint(self.decimal_provenance)
        elif self.decimal_provenance is not None:
            raise ValueError('decimal provenance requires decimals')

    @property
    def scaled(self):
        # Decimal tuple construction is exact even under hostile ambient context.
        from decimal import Decimal
        if self.decimals is None:
            return None
        return format(Decimal((0, tuple(int(c) for c in self.raw_amount),
                               -self.decimals)), 'f')


@dataclass(frozen=True)
class Action(_Versioned):
    """Synthetic-v1 action, not a deployed protocol interpretation.

    Event time is integer UTC action-end seconds. Route is ordered asset evidence;
    only explicit route_proven permits endpoint grouping. Trader attribution is
    separately qualified and is never inferred from sender/router/recipient.
    """
    _kind: ClassVar[str] = 'synthetic_action'
    identity: ActionIdentity
    epoch: SourceEpoch
    protocol: str
    protocol_version: str
    action_end: int
    status: str
    source_eligible: bool | None
    route_proven: bool | None
    route: tuple[str, ...]
    input: Endpoint
    output: Endpoint
    trader: str | None
    trader_method: str | None
    trader_unambiguous: bool | None

    def __post_init__(self):
        if type(self.identity) is not ActionIdentity or type(self.epoch) is not SourceEpoch:
            raise ValueError('typed identity and epoch required')
        if self.protocol_version == SYNTHETIC_VERSION:
            refuse_synthetic('synthetic-v1 action')
        if self.identity.network != self.epoch.network:
            raise ValueError('epoch network mismatch')
        if type(self.input) is not Endpoint or type(self.output) is not Endpoint:
            raise ValueError('typed endpoints required')
        for value in (self.protocol, self.protocol_version, self.status):
            _fingerprint(value)
        if type(self.action_end) is not int or not 0 <= self.action_end <= MAX_EVENT_TIME:
            raise ValueError('action_end outside UTC second bound')
        for value in (self.source_eligible, self.route_proven, self.trader_unambiguous):
            _optional_bool(value)
        if type(self.route) is not tuple or not 2 <= len(self.route) <= 64:
            raise ValueError('route must contain 2..64 asset IDs')
        for asset in self.route:
            _fingerprint(asset)
        if self.route[0] != self.input.asset or self.route[-1] != self.output.asset:
            raise ValueError('route endpoints disagree')
        if self.trader is not None:
            _source_id(self.trader)
        if self.trader_method is not None:
            _fingerprint(self.trader_method)

    @property
    def reasons(self):
        return tuple(reason for condition, reason in (
            (self.protocol not in ('STON.fi', 'DeDust')
             or (self.protocol_version != 'synthetic-v1'
                 and LIVE_PROTOCOL_VERSIONS.get(self.protocol_version) != self.protocol),
             'unsupported_protocol'),
            (self.status != 'success', 'status_not_success'),
            (self.source_eligible is not True, 'eligibility_not_explicit'),
            (self.route_proven is not True, 'route_not_proven')) if condition)

    @property
    def eligible(self):
        return not self.reasons

    @classmethod
    def from_json(cls, text):
        data = _load_json(text)
        if type(data) is not dict:
            raise ValueError('action object required')
        for name, model in (('identity', ActionIdentity), ('epoch', SourceEpoch),
                            ('input', Endpoint), ('output', Endpoint)):
            value = data.get(name)
            if type(value) is not dict:
                raise ValueError('nested object required')
            data[name] = model.from_json(json.dumps(
                {'schema_version': 1, 'kind': model._kind, **value}))
        if type(data.get('route')) is not list:
            raise ValueError('route list required')
        data['route'] = tuple(data['route'])
        names = {f.name for f in fields(cls)}
        if (set(data) != names | {'schema_version', 'kind'}
                or type(data['schema_version']) is not int
                or data['schema_version'] != 1 or data['kind'] != cls._kind):
            raise ValueError('unsupported action schema')
        return cls(**{name: data[name] for name in names})


@dataclass(frozen=True)
class Contribution(_Versioned):
    """One endpoint asset per routed event; directions never netted or summed."""
    _kind: ClassVar[str] = 'synthetic_contribution'
    event_id: str
    token: str
    role: str
    window_start: int
    window_end: int
    routed_trade_id: str
    count_eligible: bool
    trader: str | None
    trader_eligible: bool
    unit: str
    input_raw: str | None
    output_raw: str | None
    input_scaled: str | None
    output_scaled: str | None

    def __post_init__(self):
        ActionIdentity.from_event_id(self.event_id)
        if self.routed_trade_id != self.event_id:
            raise ValueError('routed trade must use event identity')
        _fingerprint(self.token)
        if self.unit != 'raw:' + self.token or self.role not in ('input', 'output', 'both'):
            raise ValueError('invalid role or unit')
        if (type(self.window_start) is not int or type(self.window_end) is not int
                or not 0 <= self.window_start <= MAX_EVENT_TIME
                or self.window_start % 300 or self.window_end != self.window_start + 300):
            raise ValueError('invalid half-open five-minute window')
        if type(self.count_eligible) is not bool or type(self.trader_eligible) is not bool:
            raise ValueError('explicit eligibility required')
        if self.trader is not None:
            _source_id(self.trader)
        if self.trader_eligible != (self.trader is not None):
            raise ValueError('trader eligibility mismatch')
        for direction in ('input', 'output'):
            raw = getattr(self, direction + '_raw')
            scaled = getattr(self, direction + '_scaled')
            present = self.role in (direction, 'both')
            if present:
                _raw(raw)
            elif raw is not None or scaled is not None:
                raise ValueError('amount outside endpoint role')
            if scaled is not None:
                # Accept only exact scalings of this raw value within the profile.
                if type(scaled) is not str or not any(
                    Endpoint(self.token, raw, d, 'synthetic:validation').scaled == scaled
                    for d in range(MAX_DECIMALS + 1)):
                    raise ValueError('scaled amount is not exact')


@dataclass(frozen=True)
class Lease:
    """Database-issued fencing token for one transition owner."""
    owner: str
    fence: int
    expires_at: datetime

    def __post_init__(self):
        _fingerprint(self.owner)
        if type(self.fence) is not int or self.fence < 1:
            raise ValueError('fence must be a positive integer')
        if type(self.expires_at) is not datetime or self.expires_at.tzinfo is None:
            raise ValueError('lease expiry must be timezone-aware')


WINDOW_SECONDS = 300
CONTRACTS = ("source_monotonic_revision", "owner_resolution")
GRANT_ACTIONS = ("revise", "add", "retract")
CHANGE_KINDS = ("revision", "retraction", "quarantine", "cleared")


class RevisionRejected(ValueError):
    """Revision input is malformed or unprovable; nothing was admitted."""
    pass


def _plain_int(value, minimum=None) -> bool:
    return type(value) is int and (minimum is None or value >= minimum)


@dataclass(frozen=True)
class RevisionPolicy:
    """Trailing-baseline lookback: whole five-minute windows, default seven days."""
    baseline_lookback_seconds: int = 7 * 86400

    def __post_init__(self):
        value = self.baseline_lookback_seconds
        if not _plain_int(value, WINDOW_SECONDS) or value % WINDOW_SECONDS:
            raise ValueError("baseline lookback must be whole five-minute windows")


@dataclass(frozen=True)
class WindowKey:
    """One half-open five-minute UTC window of one token on one network."""
    network: str
    token: str
    window_start: int

    def __post_init__(self):
        _network(self.network)
        _fingerprint(self.token)
        if (not _plain_int(self.window_start, 0)
                or self.window_start % WINDOW_SECONDS):
            raise ValueError("window start must be a five-minute UTC boundary")

    @property
    def window_end(self) -> int:
        return self.window_start + WINDOW_SECONDS

    def dependents_span(self, policy: RevisionPolicy) -> tuple[int, int]:
        """Start range ``[from, until)`` of later windows whose trailing baseline holds this one."""
        return (self.window_end,
                self.window_start + policy.baseline_lookback_seconds + WINDOW_SECONDS)


@dataclass(frozen=True)
class Change:
    """One durable decision: an admitted revision/retraction or a quarantine disposition."""
    kind: str
    event_id: str
    reason: str
    accepted_generation: int | None
    old_windows: tuple[WindowKey, ...]
    new_windows: tuple[WindowKey, ...]

    def __post_init__(self):
        if self.kind not in CHANGE_KINDS:
            raise ValueError("unknown change kind")


@dataclass(frozen=True)
class Grant:
    event_id: str
    action: str
    digest: str | None
    expected_generation: int | None
    source_revision: int | None
    previous_source_revision: int | None


@dataclass(frozen=True)
class Authorization:
    """Audited authority to admit specific candidate digests; never inferred."""
    contract: str
    authority_id: str
    epoch: SourceEpoch
    grants: dict
    trace_sets: dict

    def __post_init__(self):
        object.__setattr__(self, "grants", dict(self.grants))
        object.__setattr__(self, "trace_sets", dict(self.trace_sets))

    def to_json(self) -> str:
        return json.dumps({
            "contract": self.contract, "authority_id": self.authority_id,
            "epoch": json.loads(self.epoch.to_json()),
            "grants": [asdict(g) for _, g in sorted(self.grants.items())],
            "trace_sets": {k: list(v) for k, v in sorted(self.trace_sets.items())},
        }, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


_GRANT_KEYS = {"event_id", "action", "digest", "expected_generation",
               "source_revision", "previous_source_revision"}


def _grant(raw, contract: str) -> Grant:
    if type(raw) is not dict or not _GRANT_KEYS >= raw.keys() or \
            not {"event_id", "action"} <= raw.keys():
        raise RevisionRejected("grant must be an object with known fields")
    action = raw["action"]
    if action not in GRANT_ACTIONS:
        raise RevisionRejected("unknown grant action")
    try:
        ActionIdentity.from_event_id(raw["event_id"])
    except ValueError as exc:
        raise RevisionRejected("grant event ID is invalid") from exc
    digest = raw.get("digest")
    if action == "retract":
        if digest is not None:
            raise RevisionRejected("retraction names no candidate digest")
    elif (type(digest) is not str or not digest.startswith("sha256:")
          or len(digest) != 71):
        raise RevisionRejected("grant requires the exact candidate digest")
    if action == "add":
        if raw.get("expected_generation") is not None:
            raise RevisionRejected("an addition has no accepted generation to expect")
    elif "expected_generation" not in raw or (
            raw["expected_generation"] is not None
            and not _plain_int(raw["expected_generation"], 1)):
        raise RevisionRejected("expected accepted generation is required")
    for name in ("source_revision", "previous_source_revision"):
        if raw.get(name) is not None and not _plain_int(raw[name], 1):
            raise RevisionRejected(f"{name} must be a positive integer")
    if (contract == "source_monotonic_revision" and action != "retract"
            and raw.get("source_revision") is None):
        raise RevisionRejected("monotonic authority requires a source revision")
    return Grant(raw["event_id"], action, digest, raw.get("expected_generation"),
                 raw.get("source_revision"), raw.get("previous_source_revision"))


def parse_authorization(raw) -> "Authorization | None":
    """Strictly validate an authorization; ``None`` means no authority was presented."""
    if raw is None:
        return None
    keys = {"contract", "authority_id", "epoch", "grants", "trace_sets"}
    if (type(raw) is not dict or not keys - {"trace_sets"} <= raw.keys()
            or raw.keys() - keys or raw["contract"] not in CONTRACTS):
        raise RevisionRejected("authorization shape or contract is invalid")
    authority = raw["authority_id"]
    if type(authority) is not str or not authority.strip():
        raise RevisionRejected("authority ID must be nonempty text")
    try:
        epoch = SourceEpoch.from_json(json.dumps(raw["epoch"]))
    except (ValueError, TypeError) as exc:
        raise RevisionRejected("authorization epoch is invalid") from exc
    grants = raw["grants"]
    if type(grants) is not list or not grants:
        raise RevisionRejected("at least one grant is required")
    parsed = {}
    for item in grants:
        grant = _grant(item, raw["contract"])
        if grant.event_id in parsed:
            raise RevisionRejected("duplicate grant for one event")
        parsed[grant.event_id] = grant
    trace_sets = {}
    for trace_id, members in (raw.get("trace_sets") or {}).items():
        if (type(trace_id) is not str or type(members) is not list
                or any(type(m) is not str for m in members)
                or len(set(members)) != len(members)):
            raise RevisionRejected("trace set must list unique action IDs")
        trace_sets[trace_id] = tuple(sorted(members))
    return Authorization(raw["contract"], authority.strip(), epoch, parsed, trace_sets)


# Exact qualified baseline grid and detector input contract.
# A grid cell is one (network, token, direction, five-minute window). Only ``observed`` and
# ``zero`` cells are baseline-eligible. A zero needs qualified action-window coverage and a
# token already admitted to the data-derived universe; an absent row without that evidence is
# ``missing`` or ``pre_universe``, never zero. Synthetic (fixture) coverage can license a
# bounded-observed zero but is never reported as certified.
GRID_STATES = ("observed", "zero", "partial", "provisional", "missing", "pre_universe")
ELIGIBLE_GRID_STATES = ("observed", "zero")
_MERGEABLE_GRID_STATES = ("zero", "missing", "pre_universe")
GRID_DIRECTION = "both"
QUALIFIED_MODES = ("bounded_observed", "certified")
BASELINE_STATUSES = ("ready", "cold_start", "pending", "missing")
CERTIFICATIONS = ("certified", "bounded_observed", "synthetic_bounded_observed", "none")
DETECTOR_IDS = ("swap_count_spike.v1", "active_wallets_spike.v1")
_DECIMAL_TEXT = re.compile(r"^(0|[1-9][0-9]{0,8})(\.[0-9]{1,9})?$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")


def _canonical_digest(body) -> str:
    text = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      allow_nan=False)
    return "sha256:" + hashlib.sha256(text.encode("ascii")).hexdigest()


def _decimal_text(name: str, value, *, positive: bool, at_most_one: bool = False) -> Decimal:
    if type(value) is not str or not _DECIMAL_TEXT.match(value):
        raise ValueError(f"{name} must be plain decimal text")
    number = Decimal(value)
    if (positive and number <= 0) or (at_most_one and number > 1):
        raise ValueError(f"{name} is outside its allowed range")
    return number


@dataclass(frozen=True)
class DetectorPolicy:
    """Fixed, versioned detector policy (spec 8.1/8.2 validation defaults).

    Thresholds are exact decimal text so the identity digest never depends on float
    formatting. Any changed value is a different policy with a different ``identity``;
    only registered identities (table ``detector_policies``) can be pinned.
    """
    version: str = "detector-policy.v1"
    window_seconds: int = WINDOW_SECONDS
    lookback_seconds: int = 7 * 86400
    min_prior_windows: int = 24
    score_threshold: str = "6"
    swap_floor: int = 10
    trader_floor: int = 5
    attribution_floor: str = "0.90"
    mad_multiplier: str = "1.4826"
    scale_floor: str = "1"
    direction: str = GRID_DIRECTION
    detectors: tuple = DETECTOR_IDS

    def __post_init__(self):
        _fingerprint(self.version)
        if type(self.window_seconds) is not int or self.window_seconds != WINDOW_SECONDS:
            raise ValueError("detector windows are exactly five minutes")
        if (not _plain_int(self.lookback_seconds, WINDOW_SECONDS)
                or self.lookback_seconds % WINDOW_SECONDS):
            raise ValueError("lookback must be whole five-minute windows")
        if (not _plain_int(self.min_prior_windows, 1)
                or self.min_prior_windows > self.lookback_seconds // WINDOW_SECONDS):
            raise ValueError("min_prior_windows must fit inside the lookback")
        for name in ("swap_floor", "trader_floor"):
            if not _plain_int(getattr(self, name), 1):
                raise ValueError(f"{name} must be a positive integer")
        _decimal_text("score_threshold", self.score_threshold, positive=False)
        _decimal_text("attribution_floor", self.attribution_floor, positive=True,
                      at_most_one=True)
        _decimal_text("mad_multiplier", self.mad_multiplier, positive=True)
        _decimal_text("scale_floor", self.scale_floor, positive=True)
        if self.direction != GRID_DIRECTION:
            raise ValueError("v1 detectors count both swap directions of a token")
        if type(self.detectors) is not tuple or self.detectors != DETECTOR_IDS:
            raise ValueError("detector policy names exactly the two approved detectors")

    def body(self) -> dict:
        return {f.name: (list(getattr(self, f.name)) if f.name == "detectors"
                         else getattr(self, f.name)) for f in fields(self)}

    def to_json(self) -> str:
        return json.dumps(self.body(), sort_keys=True, separators=(",", ":"), ensure_ascii=True)

    @property
    def identity(self) -> str:
        return _canonical_digest(self.body())


def baseline_span(policy: DetectorPolicy, window_start: int) -> tuple[int, int]:
    """Start range ``[from, until)`` of the strictly prior trailing baseline windows.

    The lookback is clipped at time zero (no window exists before it).
    """
    if type(policy) is not DetectorPolicy:
        raise ValueError("DetectorPolicy required")
    if not _plain_int(window_start, 0) or window_start % WINDOW_SECONDS:
        raise ValueError("scored window must be a five-minute UTC boundary")
    return max(0, window_start - policy.lookback_seconds), window_start


def grid_cell_state(*, feature_swaps, canonical: bool, qualified: bool, pending: bool,
                    touched: bool, in_universe: bool) -> str:
    """Classify one grid cell from its facts.

    ``feature_swaps``: the served row's swap count, ``None`` if there is no row.
    ``canonical``: the window's latest write is a canonical repair promotion.
    ``qualified``: whole-window qualified coverage under the active source epoch.
    ``pending``: a pending target dirty row exists for this token/window.
    ``touched``: a current canonical action names this token in this window.
    ``in_universe``: the token was admitted at or before this window.
    """
    for name, value in (("canonical", canonical), ("qualified", qualified),
                        ("pending", pending), ("touched", touched),
                        ("in_universe", in_universe)):
        if type(value) is not bool:
            raise ValueError(f"{name} must be boolean")
    if feature_swaps is not None and not _plain_int(feature_swaps, 0):
        raise ValueError("feature_swaps must be a nonnegative integer or None")
    if pending:
        return "provisional"
    if feature_swaps is not None:
        if not canonical:
            return "provisional"
        if not qualified:
            return "partial"
        return "zero" if feature_swaps == 0 else "observed"
    if not in_universe:
        return "pre_universe"
    if not qualified:
        return "missing"
    if canonical or not touched:
        return "zero"
    return "provisional"


_CELL_VALUE_FIELDS = ("swaps", "known_traders", "unknown_count")


@dataclass(frozen=True)
class GridCell:
    """One pinned baseline grid cell; values must agree with ``state``."""
    network: str
    token: str
    direction: str
    window_start: int
    state: str
    swaps: int | None
    known_traders: int | None
    unknown_count: int | None
    revision: str | None
    coverage: str | None
    synthetic: bool

    def __post_init__(self):
        _network(self.network)
        _fingerprint(self.token)
        if self.direction != GRID_DIRECTION:
            raise ValueError("grid direction must be 'both'")
        if not _plain_int(self.window_start, 0) or self.window_start % WINDOW_SECONDS:
            raise ValueError("grid window must be a five-minute UTC boundary")
        if self.state not in GRID_STATES:
            raise ValueError("unknown grid cell state")
        if type(self.synthetic) is not bool:
            raise ValueError("synthetic must be boolean")
        values = [getattr(self, name) for name in _CELL_VALUE_FIELDS]
        if all(value is None for value in values):
            has_values = False
        elif (all(_plain_int(value, 0) for value in values)
              and self.known_traders + self.unknown_count <= self.swaps):
            has_values = True
        else:
            raise ValueError("grid cell values must be all None or consistent counts")
        if self.revision is not None:
            _fingerprint(self.revision)
        if self.coverage is not None and self.coverage not in QUALIFIED_MODES:
            raise ValueError("grid coverage must be a qualified mode or None")
        if self.synthetic and self.coverage != "bounded_observed":
            raise ValueError("synthetic coverage is bounded-observed only, never certified")
        eligible = self.state in ELIGIBLE_GRID_STATES
        if eligible != (self.coverage is not None):
            raise ValueError("only observed/zero cells carry qualified coverage")
        ok = {
            "observed": has_values and self.swaps >= 1 and self.revision is not None,
            "zero": has_values and self.swaps == 0 and self.revision is not None,
            "partial": has_values and self.swaps >= 1 and self.revision is not None,
            "provisional": True,
            "missing": not has_values and self.revision is None,
            "pre_universe": not has_values and self.revision is None,
        }[self.state]
        if not ok:
            raise ValueError(f"grid cell values do not match state {self.state}")

    def run(self, end: int | None = None) -> dict:
        return {"network": self.network, "token": self.token, "direction": self.direction,
                "start": self.window_start,
                "end": self.window_start + WINDOW_SECONDS if end is None else end,
                "state": self.state, "swaps": self.swaps, "known_traders": self.known_traders,
                "unknown_count": self.unknown_count, "revision": self.revision,
                "coverage": self.coverage, "synthetic": self.synthetic}

    def _merge_key(self):
        return (self.state, self.swaps, self.known_traders, self.unknown_count, self.revision,
                self.coverage, self.synthetic)


_RUN_KEYS = frozenset({"network", "token", "direction", "start", "end", "state", "swaps",
                       "known_traders", "unknown_count", "revision", "coverage", "synthetic"})


def _cells_from_run(run) -> list:
    if type(run) is not dict or set(run) != _RUN_KEYS:
        raise ValueError("baseline cell run has unexpected fields")
    start, end = run["start"], run["end"]
    if (not _plain_int(start, 0) or not _plain_int(end, 0) or end <= start
            or start % WINDOW_SECONDS or end % WINDOW_SECONDS):
        raise ValueError("baseline cell run must span whole five-minute windows")
    if end - start > WINDOW_SECONDS and run["state"] not in _MERGEABLE_GRID_STATES:
        raise ValueError("only zero/missing/pre_universe cells are run-length encoded")
    return [GridCell(run["network"], run["token"], run["direction"], window, run["state"],
                     run["swaps"], run["known_traders"], run["unknown_count"], run["revision"],
                     run["coverage"], run["synthetic"])
            for window in range(start, end, WINDOW_SECONDS)]


def _runs(cells) -> list:
    runs = []
    for cell in cells:
        last = runs[-1] if runs else None
        if (last is not None and cell.state in _MERGEABLE_GRID_STATES
                and last[1] == cell.window_start and last[0]._merge_key() == cell._merge_key()):
            last[1] = cell.window_start + WINDOW_SECONDS
        else:
            runs.append([cell, cell.window_start + WINDOW_SECONDS])
    return [cell.run(end) for cell, end in runs]


@dataclass(frozen=True)
class BaselineInput:
    """Reproducible detector input for one scored (network, token, direction, window).

    ``cells`` is the exact strictly prior grid ``[max(0, W - lookback), W)``; ``current`` is
    the scored window's own cell. ``status``, ``eligible_count``, ``certification`` and
    ``digest`` are derived and re-checked on construction, so they cannot be forged.
    Build it with ``BaselineInput.create`` (or ``from_record`` for a stored pin).
    """
    network: str
    token: str
    direction: str
    window_start: int
    policy_id: str
    lookback_seconds: int
    min_prior_windows: int
    universe_version: int
    admitted_window_start: int | None
    dependency_epoch: int
    active_epoch: str | None
    current: GridCell
    cells: tuple
    status: str
    eligible_count: int
    certification: str
    digest: str

    def __post_init__(self):
        _network(self.network)
        _fingerprint(self.token)
        if self.direction != GRID_DIRECTION:
            raise ValueError("baseline direction must be 'both'")
        if not _plain_int(self.window_start, 0) or self.window_start % WINDOW_SECONDS:
            raise ValueError("scored window must be a five-minute UTC boundary")
        if type(self.policy_id) is not str or not _SHA256.match(self.policy_id):
            raise ValueError("policy_id must be a sha256 identity")
        if (not _plain_int(self.lookback_seconds, WINDOW_SECONDS)
                or self.lookback_seconds % WINDOW_SECONDS):
            raise ValueError("lookback must be whole five-minute windows")
        if not _plain_int(self.min_prior_windows, 1):
            raise ValueError("min_prior_windows must be positive")
        if not _plain_int(self.universe_version, 0) or not _plain_int(self.dependency_epoch, 0):
            raise ValueError("universe and dependency versions must be nonnegative integers")
        if self.admitted_window_start is not None and (
                not _plain_int(self.admitted_window_start, 0)
                or self.admitted_window_start % WINDOW_SECONDS):
            raise ValueError("admitted window must be a five-minute boundary or None")
        if self.active_epoch is not None and type(self.active_epoch) is not str:
            raise ValueError("active_epoch must be text or None")
        if type(self.current) is not GridCell or type(self.cells) is not tuple \
                or any(type(cell) is not GridCell for cell in self.cells):
            raise ValueError("baseline input needs GridCell values")
        scope = (self.network, self.token, self.direction)
        for cell in (self.current, *self.cells):
            if (cell.network, cell.token, cell.direction) != scope:
                raise ValueError("cross-scope baseline cell")
        if self.current.window_start != self.window_start:
            raise ValueError("current cell is not the scored window")
        start = max(0, self.window_start - self.lookback_seconds)
        if [cell.window_start for cell in self.cells] != list(
                range(start, self.window_start, WINDOW_SECONDS)):
            raise ValueError("incomplete baseline grid: cells must be exactly the strictly "
                             "prior lookback windows in order")
        derived = self._derived()
        if (self.status, self.eligible_count, self.certification) != derived:
            raise ValueError("baseline status/eligibility/certification do not match the cells")
        if self.digest != _canonical_digest(self._body()):
            raise ValueError("baseline input digest does not match its pins")

    def eligible_cells(self) -> tuple:
        return tuple(cell for cell in self.cells if cell.state in ELIGIBLE_GRID_STATES)

    def _derived(self):
        eligible = self.eligible_cells()
        if self.current.state in ("missing", "partial", "pre_universe"):
            status = "missing"
        elif self.current.state == "provisional" or any(
                cell.state == "provisional" for cell in self.cells):
            status = "pending"
        elif len(eligible) < self.min_prior_windows:
            status = "cold_start"
        else:
            status = "ready"
        considered = list(eligible) + ([self.current]
                                       if self.current.state in ELIGIBLE_GRID_STATES else [])
        if not considered:
            certification = "none"
        elif any(cell.synthetic for cell in considered):
            certification = "synthetic_bounded_observed"
        elif all(cell.coverage == "certified" for cell in considered):
            certification = "certified"
        else:
            certification = "bounded_observed"
        return status, len(eligible), certification

    def _header(self) -> dict:
        return {"network": self.network, "token": self.token, "direction": self.direction,
                "window_start": self.window_start, "policy_id": self.policy_id,
                "lookback_seconds": self.lookback_seconds,
                "min_prior_windows": self.min_prior_windows,
                "universe_version": self.universe_version,
                "admitted_window_start": self.admitted_window_start,
                "dependency_epoch": self.dependency_epoch, "active_epoch": self.active_epoch}

    def _body(self) -> dict:
        return {"header": self._header(), "cells": _runs(self.cells),
                "current": self.current.run()}

    @classmethod
    def create(cls, *, network, token, window_start, policy, universe_version,
               admitted_window_start, dependency_epoch, active_epoch, current,
               cells) -> "BaselineInput":
        if type(policy) is not DetectorPolicy:
            raise ValueError("DetectorPolicy required")
        draft = dict(network=network, token=token, direction=policy.direction,
                     window_start=window_start, policy_id=policy.identity,
                     lookback_seconds=policy.lookback_seconds,
                     min_prior_windows=policy.min_prior_windows,
                     universe_version=universe_version,
                     admitted_window_start=admitted_window_start,
                     dependency_epoch=dependency_epoch, active_epoch=active_epoch,
                     current=current, cells=tuple(cells))
        probe = object.__new__(cls)
        for name, value in draft.items():
            object.__setattr__(probe, name, value)
        if type(probe.current) is not GridCell or any(
                type(cell) is not GridCell for cell in probe.cells):
            raise ValueError("baseline input needs GridCell values")
        status, eligible, certification = probe._derived()
        return cls(**draft, status=status, eligible_count=eligible,
                   certification=certification, digest=_canonical_digest(probe._body()))

    def to_record(self) -> dict:
        """Column values of table ``baseline_inputs`` (cells run-length encoded)."""
        return {"input_digest": self.digest, **self._header(), "status": self.status,
                "eligible_count": self.eligible_count, "certification": self.certification,
                "current_cell": self.current.run(), "cells": _runs(self.cells)}

    @classmethod
    def from_record(cls, record) -> "BaselineInput":
        names = {"input_digest", "network", "token", "direction", "window_start", "policy_id",
                 "lookback_seconds", "min_prior_windows", "universe_version",
                 "admitted_window_start", "dependency_epoch", "active_epoch", "status",
                 "eligible_count", "certification", "current_cell", "cells"}
        if type(record) is not dict or set(record) != names or type(record["cells"]) is not list:
            raise ValueError("baseline input record has unexpected fields")
        current = _cells_from_run(record["current_cell"])
        if len(current) != 1:
            raise ValueError("current cell must be exactly one window")
        cells = tuple(cell for run in record["cells"] for cell in _cells_from_run(run))
        values = {name: record[name] for name in names
                  - {"input_digest", "current_cell", "cells"}}
        return cls(**values, current=current[0], cells=cells, digest=record["input_digest"])
