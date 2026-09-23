"""Immutable build identity and in-memory release/verification contracts.

No filesystem, publication, acceptance claim or locking implementation exists.
A _SUCCESS marker is never sufficient evidence of release acceptance.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from hashlib import sha256
from collections.abc import Mapping
from types import MappingProxyType

from spark.gold.contracts import (
    GOLD_MODELS,
    KEY_VERSION,
    MANIFEST_VERSION,
    SCHEMA_VERSION,
    SERIALIZATION_VERSION,
)
from spark.gold.hashing import canonical_json
from spark.gold.reference import absence_domain_fingerprint

# Identifiers for the approved decisions, not configurable alternative semantics.
APPROVED_ASSUMPTIONS = (
    "bounded-assignment-history-v1",
    "closed-month-overlap-v1",
    "current-business-unit-mapping-v1",
    "labelled-current-state-assumptions-v1",
    "payroll-actual-period-end-v1",
    "post-event-closing-headcount-v1",
)


def _text(value: str) -> None:
    if type(value) is not str or not value.strip():
        raise ValueError("Expected a nonempty explicit identifier")


def _digest(value: str) -> None:
    if (
        type(value) is not str
        or len(value) != 64
        or any(c not in "0123456789abcdef" for c in value)
    ):
        raise ValueError("Expected a lowercase SHA-256 hex digest")


def _utc(value: datetime) -> str:
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Audit timestamps must be timezone-aware")
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


@dataclass(frozen=True)
class RuntimeContract:
    """Only deterministic semantics; no paths, host names or execution knobs."""

    session_timezone: str = "UTC"
    ansi_enabled: bool = True
    case_sensitive: bool = True

    def __post_init__(self):
        if self.session_timezone != "UTC":
            raise ValueError("Gold requires UTC")
        if type(self.ansi_enabled) is not bool or type(self.case_sensitive) is not bool:
            raise ValueError("Runtime flags must be booleans")

    def to_dict(self) -> dict:
        return {
            "session_timezone": self.session_timezone,
            "ansi_enabled": self.ansi_enabled,
            "case_sensitive": self.case_sensitive,
        }


@dataclass(frozen=True)
class BuildSpec:
    """Full MVP input identity using per-dataset logical-content SHA-256 hashes.

    Input digests describe logical Silver content, independent of file layout.
    The source system namespace is material because analytical keys use it.
    New contract/policy versions can be represented for identity comparison;
    only the pinned approved versions can be used by a runnable release contract.
    """

    silver_batch_id: str
    source_namespace: str
    input_fingerprints: Mapping[str, str]
    code_revision: str
    reporting_start: date
    reporting_end: date
    source_cutoff: date
    assumption_policies: tuple[str, ...] = APPROVED_ASSUMPTIONS
    runtime: RuntimeContract = field(default_factory=RuntimeContract)
    schema_version: str = SCHEMA_VERSION
    serialization_version: str = SERIALIZATION_VERSION
    key_version: str = KEY_VERSION
    manifest_version: str = MANIFEST_VERSION
    absence_domain_digest: str = field(default_factory=absence_domain_fingerprint)

    def __post_init__(self):
        _digest(self.absence_domain_digest)
        for value in (
            self.silver_batch_id,
            self.source_namespace,
            self.code_revision,
            self.schema_version,
            self.serialization_version,
            self.key_version,
            self.manifest_version,
        ):
            _text(value)
        if not isinstance(self.runtime, RuntimeContract):
            raise ValueError("A typed runtime contract is required")
        for value in (self.reporting_start, self.reporting_end, self.source_cutoff):
            if type(value) is not date:
                raise ValueError("Reporting bounds/cutoff must be explicit dates")
        if not self.reporting_start <= self.reporting_end <= self.source_cutoff:
            raise ValueError(
                "Require reporting_start <= reporting_end <= source_cutoff"
            )
        if not isinstance(self.assumption_policies, (tuple, list)):
            raise ValueError("Assumption policies require a sequence of identifiers")
        policies = tuple(self.assumption_policies)
        if not policies or len(set(policies)) != len(policies):
            raise ValueError("Assumption identifiers must be nonempty and unique")
        for policy in policies:
            _text(policy)
        object.__setattr__(self, "assumption_policies", tuple(sorted(policies)))
        required = {
            source for model in GOLD_MODELS.values() for source in model.silver_sources
        }
        fingerprints = dict(self.input_fingerprints)
        if set(fingerprints) != required:
            raise ValueError(
                "Fingerprints must identify exactly the MVP Silver dependency set"
            )
        for digest in fingerprints.values():
            _digest(digest)
        object.__setattr__(self, "input_fingerprints", MappingProxyType(fingerprints))

    def to_dict(self) -> dict:
        return {
            "silver_batch_id": self.silver_batch_id,
            "source_namespace": self.source_namespace,
            "input_fingerprints": dict(self.input_fingerprints),
            "code_revision": self.code_revision,
            "reporting_start": self.reporting_start.isoformat(),
            "reporting_end": self.reporting_end.isoformat(),
            "source_cutoff": self.source_cutoff.isoformat(),
            "assumption_policies": list(self.assumption_policies),
            "runtime": self.runtime.to_dict(),
            "schema_version": self.schema_version,
            "serialization_version": self.serialization_version,
            "key_version": self.key_version,
            "manifest_version": self.manifest_version,
            "absence_domain_digest": self.absence_domain_digest,
        }

    @property
    def build_id(self) -> str:
        return sha256(
            canonical_json({"kind": "gold-build", "spec": self.to_dict()}).encode(
                "ascii"
            )
        ).hexdigest()

    def require_supported(self) -> None:
        if self.absence_domain_digest != absence_domain_fingerprint():
            raise ValueError("Governed absence domain differs from build identity")
        if (
            self.schema_version,
            self.serialization_version,
            self.key_version,
            self.manifest_version,
        ) != (
            SCHEMA_VERSION,
            SERIALIZATION_VERSION,
            KEY_VERSION,
            MANIFEST_VERSION,
        ) or self.assumption_policies != tuple(
            sorted(APPROVED_ASSUMPTIONS)
        ):
            raise ValueError(
                "Unsupported Gold contract or unapproved assumption policy"
            )


class ReleaseStatus(str, Enum):
    """Lifecycle vocabulary; successful promotion is not implemented yet."""

    BUILDING = "BUILDING"
    VALIDATED = "VALIDATED"
    ACCEPTED = "ACCEPTED"
    FAILED = "FAILED"


class RunMode(str, Enum):
    CREATE = "create"
    VERIFY_EXISTING = "verify-existing"


class ExistingOutputError(RuntimeError):
    """Existing/partial output needs explicit resolution; never append or repair."""


def require_output_policy(mode: RunMode, existing_status: ReleaseStatus | None) -> None:
    """Gate the exact requested build; None means no output of any kind exists.

    Partial output without a manifest is BUILDING. Different build IDs have
    separate immutable releases; callers must never substitute a latest build.
    Verification can inspect any existing state but cannot make it ACCEPTED.
    """
    if not isinstance(mode, RunMode) or (
        existing_status is not None and not isinstance(existing_status, ReleaseStatus)
    ):
        raise ValueError("Typed run mode/release status required")
    if mode is RunMode.VERIFY_EXISTING and existing_status is None:
        raise ExistingOutputError(
            "Missing output: verification cannot create or repair it"
        )
    if mode is RunMode.CREATE and existing_status is not None:
        raise ExistingOutputError(
            "Output exists: no append, overwrite or automatic deletion"
        )


@dataclass(frozen=True)
class ModelInventory:
    """Exact expected partition set, including empty output, plus optional evidence.

    Unpartitioned models use ((),). Partitioned models use tuples of ordered
    (field, integer value) pairs. () means an explicitly empty partition set.
    Row counts include reserved members; content digest describes logical rows.
    """

    model: str
    partitions: tuple[tuple[tuple[str, int], ...], ...]
    row_count: int | None = None
    content_fingerprint: str | None = None

    def __post_init__(self):
        if self.model not in GOLD_MODELS:
            raise ValueError("Unknown MVP model")
        contract = GOLD_MODELS[self.model]
        if not isinstance(self.partitions, (tuple, list)) or any(
            not isinstance(p, (tuple, list))
            or any(not isinstance(pair, (tuple, list)) for pair in p)
            for p in self.partitions
        ):
            raise ValueError("Partition inventory requires nested sequences")
        partitions = tuple(tuple(tuple(pair) for pair in p) for p in self.partitions)
        if len(set(partitions)) != len(partitions):
            raise ValueError("Duplicate partitions")
        if not contract.partition_fields and partitions != ((),):
            raise ValueError(
                "Unpartitioned models require exactly one unpartitioned entry"
            )
        for partition in partitions:
            if any(len(pair) != 2 for pair in partition):
                raise ValueError("Partition entries require a field and value")
            if tuple(pair[0] for pair in partition) != contract.partition_fields:
                raise ValueError("Partition fields differ from the approved model")
            if any(
                len(pair) != 2 or type(pair[1]) is not int or not 1 <= pair[1] <= 9999
                for pair in partition
            ):
                raise ValueError("Partition years must be valid integers")
        if self.row_count is not None and (
            type(self.row_count) is not int or self.row_count < 0
        ):
            raise ValueError("Invalid row count")
        if not partitions and self.row_count not in (None, 0):
            raise ValueError("An empty partition set cannot contain rows")
        if self.content_fingerprint is not None:
            _digest(self.content_fingerprint)
        object.__setattr__(self, "partitions", tuple(sorted(partitions)))

    def to_dict(self) -> dict:
        return {
            "model": self.model,
            "schema_contract_id": GOLD_MODELS[self.model].contract_id,
            "expected_partitions": [dict(p) for p in self.partitions],
            "row_count": self.row_count,
            "content_fingerprint": self.content_fingerprint,
        }


# Every gate is mandatory for eventual acceptance, not just _SUCCESS/readability.
VERIFICATION_CHECKS = (
    "exact_build",
    "model_partition_set",
    "schema",
    "content_hash",
    "grain",
    "foreign_key",
    "reconciliation",
    "physical_readback",
)


@dataclass(frozen=True)
class ReleaseManifest:
    """Unaccepted inventory or failure record, never caller-authorized acceptance.

    VALIDATED and ACCEPTED are reserved for a future evidence-backed promotion
    boundary. Diagnostic booleans/metrics are not proof that model validation,
    reconciliation and physical readback ran for this exact build.
    """

    spec: BuildSpec
    generated_at: datetime
    models: tuple[ModelInventory, ...]
    status: ReleaseStatus = ReleaseStatus.BUILDING
    verification_outcomes: Mapping[str, bool] = field(default_factory=dict)
    reconciliation_metrics: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self):
        if not isinstance(self.spec, BuildSpec):
            raise ValueError("Typed build specification required")
        self.spec.require_supported()
        _utc(self.generated_at)
        if not isinstance(self.status, ReleaseStatus):
            raise ValueError("Invalid release status")
        if self.status in (ReleaseStatus.VALIDATED, ReleaseStatus.ACCEPTED):
            raise ValueError(
                "Evidence-backed promotion is not implemented; "
                "manifest construction permits only BUILDING or FAILED"
            )
        models = tuple(self.models)
        if any(not isinstance(model, ModelInventory) for model in models):
            raise ValueError("Typed model inventories required")
        if len(models) != len(GOLD_MODELS) or {m.model for m in models} != set(
            GOLD_MODELS
        ):
            raise ValueError("Manifest must list exactly all ten MVP models")
        outcomes = dict(self.verification_outcomes)
        if set(outcomes) - set(VERIFICATION_CHECKS) or any(
            type(v) is not bool for v in outcomes.values()
        ):
            raise ValueError("Invalid verification outcomes")
        metrics = dict(self.reconciliation_metrics)
        for key, value in metrics.items():
            _text(key)
            _text(value)
        object.__setattr__(self, "models", tuple(sorted(models, key=lambda m: m.model)))
        object.__setattr__(self, "verification_outcomes", MappingProxyType(outcomes))
        object.__setattr__(self, "reconciliation_metrics", MappingProxyType(metrics))

    def to_dict(self) -> dict:
        return {
            "manifest_version": MANIFEST_VERSION,
            "build_id": self.spec.build_id,
            "spec": self.spec.to_dict(),
            "generated_at": _utc(self.generated_at),
            "status": self.status.value,
            "expected_models": [m.to_dict() for m in self.models],
            "verification_outcomes": dict(self.verification_outcomes),
            "reconciliation_metrics": dict(self.reconciliation_metrics),
        }

    def to_json(self) -> str:
        return canonical_json(self.to_dict())


@dataclass(frozen=True)
class VerificationRequest:
    """Read-only capability: no writer, repair callback or mutation options.

    A future verifier must fail missing/extra models or partitions, mismatched
    build, schema, content, grain, FK or reconciliation. Returning evidence must
    not mutate the input manifest or transition its acceptance state.
    """

    expected: ReleaseManifest

    def __post_init__(self):
        if not isinstance(self.expected, ReleaseManifest):
            raise ValueError("Verification requires an exact expected release")

    @property
    def allows_writes(self) -> bool:
        return False

    @property
    def required_checks(self) -> tuple[str, ...]:
        return VERIFICATION_CHECKS

    def check_inventory(self, actual: ReleaseManifest) -> None:
        if self.expected.spec.build_id != actual.spec.build_id:
            raise ExistingOutputError("Exact build selection required")
        if _utc(self.expected.generated_at) != _utc(actual.generated_at):
            raise ExistingOutputError("Stored audit timestamp must be reused")
        expected = [(m.model, m.partitions) for m in self.expected.models]
        observed = [(m.model, m.partitions) for m in actual.models]
        if expected != observed:
            raise ExistingOutputError(
                "Missing, extra or changed model/partition inventory"
            )
