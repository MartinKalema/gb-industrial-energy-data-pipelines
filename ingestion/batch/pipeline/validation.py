"""Deterministic validation and quarantine for bounded generated source files.

The module deliberately has no Airflow, Spark, Iceberg, or R2 dependency.  It
accepts already-downloaded JSON Lines files, validates them against the Phase 2
JSON Schemas, applies the cross-record rules which JSON Schema cannot express,
and writes deterministic accepted/quarantine artifacts for the next pipeline
step.

An accepted JSONL line is the original source payload.  Validation evidence
(the immutable source-revision identity and canonical payload SHA-256) is kept
in the report so a downstream loader can add lineage columns without mutating
the source-shaped record.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import quote

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource


UTC = timezone.utc
HALF_HOUR = timedelta(minutes=30)
DEFAULT_CONTRACTS_DIR = Path(__file__).resolve().parents[3] / "contracts"

DATASET_FILES: dict[str, str] = {
    "customer_master": "customer_master.jsonl",
    "industrial_site_master": "industrial_site_master.jsonl",
    "delivery_point_assignment": "delivery_point_assignment.jsonl",
    "revenue_meter_assignment": "revenue_meter_assignment.jsonl",
    "contract_terms": "contract_terms.jsonl",
    "commitment_schedule": "commitment_schedule.jsonl",
    "approved_excess_order": "approved_excess_order.jsonl",
    "revenue_meter_reading": "revenue_meter_reading.jsonl",
    "delivery_point_capacity_assessment": "delivery_point_capacity_assessment.jsonl",
}

# The identity is deliberately derived from the governed logical key and source
# revision, rather than trusting a source-supplied opaque revision ID.  This is
# what lets two differently named payloads for the same logical revision be
# identified as a conflict.
SOURCE_REVISION_KEY_FIELDS: dict[str, tuple[str, ...]] = {
    "customer_master": ("source_system_id", "customer_version_id", "source_revision"),
    "industrial_site_master": ("source_system_id", "site_version_id", "source_revision"),
    "delivery_point_assignment": (
        "source_system_id",
        "delivery_point_assignment_id",
        "source_revision",
    ),
    "revenue_meter_assignment": (
        "source_system_id",
        "meter_assignment_id",
        "source_revision",
    ),
    "contract_terms": ("source_system_id", "contract_terms_version_id", "source_revision"),
    "commitment_schedule": (
        "source_system_id",
        "delivery_point_natural_id",
        "interval_start_utc",
        "source_revision",
    ),
    "approved_excess_order": (
        "source_system_id",
        "order_interval_line_id",
        "source_revision",
    ),
    "revenue_meter_reading": (
        "source_system_id",
        "meter_natural_id",
        "register_natural_id",
        "reading_at_utc",
        "source_revision",
    ),
    "delivery_point_capacity_assessment": (
        "source_system_id",
        "delivery_point_natural_id",
        "interval_start_utc",
        "source_revision",
    ),
    # The public sidecar has no integer source revision.  Publication time is
    # therefore part of its immutable publication identity.
    "elexon_fuelhh": (
        "dataset",
        "settlement_date",
        "settlement_period",
        "fuel_type",
        "publish_time_utc",
    ),
}

OPAQUE_REVISION_ID_FIELDS: dict[str, str] = {
    "commitment_schedule": "source_commitment_revision_id",
    "revenue_meter_reading": "source_reading_revision_id",
    "delivery_point_capacity_assessment": "source_capacity_revision_id",
}

EPISODE_KEY_FIELDS: dict[str, tuple[str, ...]] = {
    "customer_master": ("source_system_id", "customer_version_id"),
    "industrial_site_master": ("source_system_id", "site_version_id"),
    "delivery_point_assignment": ("source_system_id", "delivery_point_assignment_id"),
    "revenue_meter_assignment": ("source_system_id", "meter_assignment_id"),
    "contract_terms": ("source_system_id", "contract_terms_version_id"),
    "commitment_schedule": (
        "source_system_id",
        "delivery_point_natural_id",
        "interval_start_utc",
    ),
    "approved_excess_order": ("source_system_id", "order_interval_line_id"),
    "revenue_meter_reading": (
        "source_system_id",
        "meter_natural_id",
        "register_natural_id",
        "reading_at_utc",
    ),
    "delivery_point_capacity_assessment": (
        "source_system_id",
        "delivery_point_natural_id",
        "interval_start_utc",
    ),
}

INTERVAL_DATASETS = {
    "commitment_schedule",
    "approved_excess_order",
    "delivery_point_capacity_assessment",
}

EFFECTIVE_DATASETS = {
    "customer_master",
    "industrial_site_master",
    "delivery_point_assignment",
    "revenue_meter_assignment",
    "contract_terms",
}


@dataclass(frozen=True, order=True)
class ValidationIssue:
    """One stable, machine-readable reason why a record was quarantined."""

    code: str
    message: str
    field: str | None = None
    related_identity: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.field is not None:
            result["field"] = self.field
        if self.related_identity is not None:
            result["related_identity"] = self.related_identity
        return result


@dataclass
class _Candidate:
    dataset: str
    line_number: int
    record: dict[str, Any] | None
    raw_text: str | None = None
    source_revision_identity: str | None = None
    payload_sha256: str | None = None
    issues: list[ValidationIssue] = field(default_factory=list)
    exact_replay_of: str | None = None

    def add_issue(
        self,
        code: str,
        message: str,
        *,
        field_name: str | None = None,
        related_identity: str | None = None,
    ) -> None:
        issue = ValidationIssue(code, message, field_name, related_identity)
        if issue not in self.issues:
            self.issues.append(issue)


@dataclass(frozen=True)
class ValidatedRecord:
    dataset: str
    line_number: int
    source_revision_identity: str
    payload_sha256: str
    record: dict[str, Any]

    def evidence_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "line_number": self.line_number,
            "source_revision_identity": self.source_revision_identity,
            "payload_sha256": self.payload_sha256,
        }


@dataclass(frozen=True)
class QuarantinedRecord:
    dataset: str
    line_number: int
    issues: tuple[ValidationIssue, ...]
    source_revision_identity: str | None = None
    payload_sha256: str | None = None
    record: dict[str, Any] | None = None
    raw_text: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "dataset": self.dataset,
            "line_number": self.line_number,
            "issues": [issue.to_dict() for issue in self.issues],
        }
        if self.source_revision_identity is not None:
            result["source_revision_identity"] = self.source_revision_identity
        if self.payload_sha256 is not None:
            result["payload_sha256"] = self.payload_sha256
        if self.record is not None:
            result["record"] = self.record
        if self.raw_text is not None:
            result["raw_text"] = self.raw_text
        return result


@dataclass(frozen=True)
class ReplayRecord:
    dataset: str
    line_number: int
    source_revision_identity: str
    payload_sha256: str
    accepted_line_number: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "line_number": self.line_number,
            "source_revision_identity": self.source_revision_identity,
            "payload_sha256": self.payload_sha256,
            "accepted_line_number": self.accepted_line_number,
        }


@dataclass(frozen=True)
class BundleValidationResult:
    """In-memory result returned by the pure validation API."""

    accepted: Mapping[str, tuple[ValidatedRecord, ...]]
    quarantined: Mapping[str, tuple[QuarantinedRecord, ...]]
    exact_replays: tuple[ReplayRecord, ...]
    bundle_issues: tuple[ValidationIssue, ...] = ()
    output_files: tuple[Mapping[str, Any], ...] = ()

    @property
    def accepted_count(self) -> int:
        return sum(len(rows) for rows in self.accepted.values())

    @property
    def quarantined_count(self) -> int:
        return sum(len(rows) for rows in self.quarantined.values())

    @property
    def is_clean(self) -> bool:
        return not self.bundle_issues and self.quarantined_count == 0

    def report_dict(self) -> dict[str, Any]:
        datasets = sorted(set(self.accepted) | set(self.quarantined))
        reason_counts: Counter[str] = Counter(
            issue.code
            for rows in self.quarantined.values()
            for row in rows
            for issue in row.issues
        )
        return {
            "report_schema_id": "bounded_bundle_validation_report",
            "report_schema_version": "1.0.0",
            "status": "accepted" if self.is_clean else "completed_with_quarantine",
            "accepted_record_count": self.accepted_count,
            "quarantined_record_count": self.quarantined_count,
            "exact_replay_count": len(self.exact_replays),
            "bundle_issues": [issue.to_dict() for issue in self.bundle_issues],
            "quarantine_reason_counts": dict(sorted(reason_counts.items())),
            "datasets": [
                {
                    "dataset": dataset,
                    "accepted_record_count": len(self.accepted.get(dataset, ())),
                    "quarantined_record_count": len(self.quarantined.get(dataset, ())),
                    "exact_replay_count": sum(
                        replay.dataset == dataset for replay in self.exact_replays
                    ),
                }
                for dataset in datasets
            ],
            "accepted_evidence": [
                row.evidence_dict()
                for dataset in datasets
                for row in self.accepted.get(dataset, ())
            ],
            "exact_replays": [item.to_dict() for item in self.exact_replays],
            "output_files": [dict(item) for item in self.output_files],
        }


def canonical_payload_bytes(record: Mapping[str, Any]) -> bytes:
    """Return the stable semantic encoding used for per-record idempotency."""

    return json.dumps(
        record,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def payload_sha256(record: Mapping[str, Any]) -> str:
    """Hash one canonicalized JSON payload, independent of source whitespace."""

    return hashlib.sha256(canonical_payload_bytes(record)).hexdigest()


def source_revision_key(dataset: str, record: Mapping[str, Any]) -> tuple[Any, ...]:
    """Return the governed logical-key-plus-revision tuple for a source record."""

    try:
        fields = SOURCE_REVISION_KEY_FIELDS[dataset]
    except KeyError as exc:
        raise ValueError(f"unsupported source dataset: {dataset}") from exc
    missing = [name for name in fields if name not in record]
    if missing:
        raise ValueError(
            f"cannot compute {dataset} source-revision identity; missing fields: "
            + ", ".join(missing)
        )
    return tuple(record[name] for name in fields)


def source_revision_identity(dataset: str, record: Mapping[str, Any]) -> str:
    """Return an inspectable, deterministic immutable source-revision ID."""

    fields = SOURCE_REVISION_KEY_FIELDS[dataset]
    values = source_revision_key(dataset, record)
    components = [dataset]
    components.extend(
        f"{name}={quote(str(value), safe='-._~')}"
        for name, value in zip(fields, values, strict=True)
    )
    return "::".join(components)


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamp must carry UTC offset Z or +00:00")
    return parsed.astimezone(UTC)


def _path_text(parts: Iterable[Any]) -> str:
    rendered = "$"
    for part in parts:
        rendered += f"[{part}]" if isinstance(part, int) else f".{part}"
    return rendered


def _load_validators(contracts_dir: Path) -> dict[str, Draft202012Validator]:
    contracts_dir = contracts_dir.expanduser().resolve()
    schemas: dict[str, dict[str, Any]] = {}
    for path in sorted(contracts_dir.glob("*.schema.json")):
        schema = json.loads(path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
        schemas[path.name] = schema

    common_name = "common.schema.json"
    if common_name not in schemas:
        raise FileNotFoundError(f"missing shared contract: {contracts_dir / common_name}")

    registry = Registry()
    for filename, schema in schemas.items():
        resource = Resource.from_contents(schema)
        registry = registry.with_resource(filename, resource)
        schema_id = schema.get("$id")
        if isinstance(schema_id, str):
            registry = registry.with_resource(schema_id, resource)

    validators: dict[str, Draft202012Validator] = {}
    for filename, schema in schemas.items():
        if filename == common_name:
            continue
        dataset = filename.removesuffix(".schema.json")
        validators[dataset] = Draft202012Validator(
            schema,
            registry=registry,
            format_checker=FormatChecker(),
        )
    return validators


def _schema_validate(
    candidates: Sequence[_Candidate], validators: Mapping[str, Draft202012Validator]
) -> None:
    for candidate in candidates:
        if candidate.record is None:
            continue
        try:
            candidate.payload_sha256 = payload_sha256(candidate.record)
        except (TypeError, ValueError) as exc:
            candidate.add_issue(
                "payload_not_canonical_json",
                f"payload cannot be hashed as canonical JSON: {exc}",
            )
        validator = validators.get(candidate.dataset)
        if validator is None:
            candidate.add_issue(
                "unsupported_dataset",
                f"no JSON Schema validator is registered for {candidate.dataset!r}",
            )
            continue
        for error in sorted(
            validator.iter_errors(candidate.record),
            key=lambda item: (tuple(str(part) for part in item.absolute_path), item.message),
        ):
            path = _path_text(error.absolute_path)
            candidate.add_issue(
                "schema_validation_failed",
                f"{path}: {error.message}",
                field_name=path,
            )

        try:
            candidate.source_revision_identity = source_revision_identity(
                candidate.dataset, candidate.record
            )
        except (TypeError, ValueError) as exc:
            if not candidate.issues:
                candidate.add_issue("identity_not_computable", str(exc))


def _detect_duplicate_revisions(candidates: Sequence[_Candidate]) -> None:
    groups: dict[str, list[_Candidate]] = defaultdict(list)
    for candidate in candidates:
        if not candidate.issues and candidate.source_revision_identity is not None:
            groups[candidate.source_revision_identity].append(candidate)

    for identity, rows in sorted(groups.items()):
        hashes = {row.payload_sha256 for row in rows}
        if len(hashes) > 1:
            for row in rows:
                row.add_issue(
                    "source_revision_conflict",
                    "the same logical source revision has more than one canonical payload",
                    related_identity=identity,
                )
            continue
        if len(rows) > 1:
            accepted = min(rows, key=lambda row: (row.dataset, row.line_number))
            for replay in rows:
                if replay is not accepted:
                    replay.exact_replay_of = identity


def _detect_opaque_id_reuse(candidates: Sequence[_Candidate]) -> None:
    for dataset, field_name in OPAQUE_REVISION_ID_FIELDS.items():
        groups: dict[str, list[_Candidate]] = defaultdict(list)
        for candidate in candidates:
            if (
                candidate.dataset == dataset
                and candidate.record is not None
                and not candidate.issues
                and candidate.exact_replay_of is None
                and field_name in candidate.record
            ):
                groups[str(candidate.record[field_name])].append(candidate)
        for opaque_id, rows in sorted(groups.items()):
            identities = {row.source_revision_identity for row in rows}
            if len(identities) <= 1:
                continue
            for row in rows:
                row.add_issue(
                    "opaque_revision_id_reused",
                    f"{field_name} {opaque_id!r} identifies more than one logical source revision",
                    field_name=field_name,
                )


def _local_business_rules(candidates: Sequence[_Candidate]) -> None:
    for candidate in candidates:
        if candidate.record is None or candidate.issues or candidate.exact_replay_of:
            continue
        row = candidate.record
        try:
            if candidate.dataset in INTERVAL_DATASETS:
                start = _parse_utc(row["interval_start_utc"])
                end = _parse_utc(row["interval_end_utc"])
                if end - start != HALF_HOUR:
                    candidate.add_issue(
                        "interval_not_30_minutes",
                        "interval_end_utc must be exactly 30 minutes after interval_start_utc",
                        field_name="interval_end_utc",
                    )
                if start.second or start.microsecond or start.minute not in (0, 30):
                    candidate.add_issue(
                        "interval_not_half_hour_aligned",
                        "interval_start_utc must be an exact UTC half-hour boundary",
                        field_name="interval_start_utc",
                    )

            if candidate.dataset in EFFECTIVE_DATASETS:
                effective_from = _parse_utc(row["effective_from_utc"])
                effective_to_text = row.get("effective_to_utc")
                if effective_to_text is not None:
                    effective_to = _parse_utc(effective_to_text)
                    if effective_from >= effective_to:
                        candidate.add_issue(
                            "invalid_effective_period",
                            "effective_from_utc must be earlier than effective_to_utc",
                            field_name="effective_to_utc",
                        )

            if candidate.dataset == "delivery_point_capacity_assessment":
                numeric_fields = (
                    "nameplate_ceiling_mwh_th",
                    "operational_restriction_mwh_th",
                    "deliverable_capacity_mwh_th",
                )
                if all(name in row for name in numeric_fields):
                    nameplate = Decimal(row[numeric_fields[0]])
                    restriction = Decimal(row[numeric_fields[1]])
                    observed = Decimal(row[numeric_fields[2]])
                    expected = max(nameplate - restriction, Decimal("0"))
                    if observed != expected:
                        candidate.add_issue(
                            "capacity_arithmetic_mismatch",
                            "deliverable_capacity_mwh_th must equal "
                            "max(nameplate_ceiling_mwh_th - "
                            "operational_restriction_mwh_th, 0)",
                            field_name="deliverable_capacity_mwh_th",
                        )
        except (InvalidOperation, KeyError, TypeError, ValueError) as exc:
            # This normally cannot occur after schema validation, but preserving
            # it as a quarantine reason makes the boundary fail closed if a
            # contract is accidentally weakened later.
            candidate.add_issue("business_rule_value_invalid", str(exc))


def _key(record: Mapping[str, Any], field_names: Sequence[str]) -> tuple[Any, ...]:
    return tuple(record[name] for name in field_names)


def _candidate_revision(candidate: _Candidate) -> int:
    assert candidate.record is not None
    return int(candidate.record.get("source_revision", 1))


def _current_episode_candidates(candidates: Sequence[_Candidate], dataset: str) -> list[_Candidate]:
    """Select the declared latest revision without falling back past an invalid latest row."""

    fields = EPISODE_KEY_FIELDS[dataset]
    groups: dict[tuple[Any, ...], list[_Candidate]] = defaultdict(list)
    for candidate in candidates:
        if (
            candidate.dataset == dataset
            and candidate.record is not None
            and candidate.exact_replay_of is None
            and candidate.source_revision_identity is not None
        ):
            groups[_key(candidate.record, fields)].append(candidate)

    current: list[_Candidate] = []
    for rows in groups.values():
        greatest_revision = max(_candidate_revision(item) for item in rows)
        greatest = [item for item in rows if _candidate_revision(item) == greatest_revision]
        # Conflicting greatest revisions have already been marked.  Never pick
        # an older revision merely because the latest one failed validation.
        current.extend(greatest)
    return current


def _is_active(candidate: _Candidate) -> bool:
    if candidate.record is None or candidate.issues:
        return False
    row = candidate.record
    if row.get("approval_state") != "approved":
        return False
    status_fields = {
        "customer_master": ("version_record_status", "active"),
        "industrial_site_master": ("version_record_status", "active"),
        "delivery_point_assignment": ("assignment_status", "active"),
        "revenue_meter_assignment": ("assignment_status", "active"),
        "contract_terms": ("terms_status", "active"),
        "commitment_schedule": ("schedule_record_status", "active"),
        "revenue_meter_reading": ("reading_status", "active"),
    }
    field_and_value = status_fields.get(candidate.dataset)
    return field_and_value is None or row.get(field_and_value[0]) == field_and_value[1]


def _effective_bounds(candidate: _Candidate) -> tuple[datetime, datetime | None]:
    assert candidate.record is not None
    start = _parse_utc(candidate.record["effective_from_utc"])
    end_text = candidate.record.get("effective_to_utc")
    return start, _parse_utc(end_text) if end_text is not None else None


def _overlaps(first: _Candidate, second: _Candidate) -> bool:
    first_start, first_end = _effective_bounds(first)
    second_start, second_end = _effective_bounds(second)
    return (first_end is None or second_start < first_end) and (
        second_end is None or first_start < second_end
    )


def _mark_overlaps(
    rows: Sequence[_Candidate],
    group_fields: Sequence[str],
    code: str,
    description: str,
) -> None:
    groups: dict[tuple[Any, ...], list[_Candidate]] = defaultdict(list)
    for candidate in rows:
        if _is_active(candidate):
            assert candidate.record is not None
            groups[_key(candidate.record, group_fields)].append(candidate)
    for group in groups.values():
        ordered = sorted(group, key=lambda item: (_effective_bounds(item)[0], item.line_number))
        for index, first in enumerate(ordered):
            for second in ordered[index + 1 :]:
                if _overlaps(first, second):
                    first.add_issue(
                        code,
                        description,
                        related_identity=second.source_revision_identity,
                    )
                    second.add_issue(
                        code,
                        description,
                        related_identity=first.source_revision_identity,
                    )


def _direct_reference_rules(candidates: Sequence[_Candidate]) -> None:
    def valid_rows() -> list[_Candidate]:
        return [
            item
            for item in candidates
            if item.record is not None
            and not item.issues
            and item.exact_replay_of is None
        ]

    parents = valid_rows()
    customers = {
        item.record["customer_natural_id"]
        for item in parents
        if item.dataset == "customer_master"
    }
    sites = {
        item.record["site_natural_id"]
        for item in parents
        if item.dataset == "industrial_site_master"
    }

    # Validate the parent assignment first. Children must not be allowed to use
    # a delivery-point ID supplied only by an assignment that is quarantined.
    for candidate in parents:
        assert candidate.record is not None
        row = candidate.record
        if candidate.dataset == "delivery_point_assignment":
            if row["customer_natural_id"] not in customers:
                candidate.add_issue(
                    "customer_reference_missing",
                    "delivery-point assignment references an unknown customer",
                    field_name="customer_natural_id",
                )
            if row["site_natural_id"] not in sites:
                candidate.add_issue(
                    "site_reference_missing",
                    "delivery-point assignment references an unknown site",
                    field_name="site_natural_id",
                )

    delivery_points = {
        item.record["delivery_point_natural_id"]
        for item in valid_rows()
        if item.dataset == "delivery_point_assignment"
    }
    for candidate in valid_rows():
        assert candidate.record is not None
        row = candidate.record
        if candidate.dataset in {"revenue_meter_assignment", "contract_terms"}:
            if row["delivery_point_natural_id"] not in delivery_points:
                candidate.add_issue(
                    "delivery_point_reference_missing",
                    f"{candidate.dataset} references an unknown delivery point",
                    field_name="delivery_point_natural_id",
                )
            if (
                candidate.dataset == "contract_terms"
                and row["customer_natural_id"] not in customers
            ):
                candidate.add_issue(
                    "customer_reference_missing",
                    "contract terms reference an unknown customer",
                    field_name="customer_natural_id",
                )


def _effective_overlap_rules(candidates: Sequence[_Candidate]) -> None:
    current = {
        dataset: _current_episode_candidates(candidates, dataset)
        for dataset in EFFECTIVE_DATASETS
    }
    _mark_overlaps(
        current["customer_master"],
        ("customer_natural_id",),
        "customer_effective_overlap",
        "current approved customer versions overlap in effective time",
    )
    _mark_overlaps(
        current["industrial_site_master"],
        ("site_natural_id",),
        "site_effective_overlap",
        "current approved site versions overlap in effective time",
    )
    _mark_overlaps(
        current["delivery_point_assignment"],
        ("delivery_point_natural_id",),
        "delivery_point_assignment_overlap",
        "current approved delivery-point assignments overlap in effective time",
    )
    _mark_overlaps(
        current["delivery_point_assignment"],
        ("site_natural_id",),
        "site_delivery_point_overlap",
        "Phase 2 permits only one active delivery point per site at an event time",
    )
    authoritative_meters = [
        item
        for item in current["revenue_meter_assignment"]
        if item.record is not None and item.record.get("assignment_role") == "authoritative_revenue"
    ]
    _mark_overlaps(
        authoritative_meters,
        ("meter_natural_id", "register_natural_id"),
        "meter_assignment_overlap",
        "one meter/register cannot be authoritative for multiple overlapping assignments",
    )
    _mark_overlaps(
        authoritative_meters,
        ("delivery_point_natural_id",),
        "delivery_point_meter_overlap",
        "one delivery point cannot have multiple authoritative revenue meters at an event time",
    )
    _mark_overlaps(
        current["contract_terms"],
        ("delivery_point_natural_id",),
        "contract_terms_overlap",
        "current approved contract terms overlap for one delivery point",
    )


def _covers_interval(candidate: _Candidate, start: datetime, end: datetime) -> bool:
    effective_from, effective_to = _effective_bounds(candidate)
    return effective_from <= start and (effective_to is None or end <= effective_to)


def _covers_event(candidate: _Candidate, event_at: datetime) -> bool:
    effective_from, effective_to = _effective_bounds(candidate)
    return effective_from <= event_at and (effective_to is None or event_at < effective_to)


def _active_current(candidates: Sequence[_Candidate], dataset: str) -> list[_Candidate]:
    return [
        item
        for item in _current_episode_candidates(candidates, dataset)
        if _is_active(item)
    ]


def _coverage_matches(
    rows: Sequence[_Candidate],
    *,
    start: datetime,
    end: datetime | None,
    predicates: Mapping[str, Any],
) -> list[_Candidate]:
    result: list[_Candidate] = []
    for candidate in rows:
        assert candidate.record is not None
        if any(
            candidate.record.get(field_name) != value
            for field_name, value in predicates.items()
        ):
            continue
        covered = (
            _covers_event(candidate, start)
            if end is None
            else _covers_interval(candidate, start, end)
        )
        if covered:
            result.append(candidate)
    return result


def _require_single_coverage(
    candidate: _Candidate,
    matches: Sequence[_Candidate],
    *,
    missing_code: str,
    ambiguous_code: str,
    subject: str,
) -> _Candidate | None:
    if len(matches) == 1:
        return matches[0]
    if not matches:
        candidate.add_issue(
            missing_code,
            f"no valid approved {subject} covers the complete event time",
        )
    else:
        candidate.add_issue(
            ambiguous_code,
            f"more than one valid approved {subject} covers the event time",
        )
    return None


def _assignment_context(
    candidate: _Candidate,
    *,
    start: datetime,
    end: datetime,
    delivery_point_id: str,
    delivery_assignments: Sequence[_Candidate],
    customer_versions: Sequence[_Candidate],
    site_versions: Sequence[_Candidate],
) -> _Candidate | None:
    assignments = _coverage_matches(
        delivery_assignments,
        start=start,
        end=end,
        predicates={"delivery_point_natural_id": delivery_point_id},
    )
    assignment = _require_single_coverage(
        candidate,
        assignments,
        missing_code="delivery_point_assignment_coverage_missing",
        ambiguous_code="delivery_point_assignment_coverage_ambiguous",
        subject="delivery-point assignment",
    )
    if assignment is None:
        return None
    assert assignment.record is not None

    customers = _coverage_matches(
        customer_versions,
        start=start,
        end=end,
        predicates={"customer_natural_id": assignment.record["customer_natural_id"]},
    )
    _require_single_coverage(
        candidate,
        customers,
        missing_code="customer_effective_coverage_missing",
        ambiguous_code="customer_effective_coverage_ambiguous",
        subject="active customer version",
    )
    sites = _coverage_matches(
        site_versions,
        start=start,
        end=end,
        predicates={"site_natural_id": assignment.record["site_natural_id"]},
    )
    _require_single_coverage(
        candidate,
        sites,
        missing_code="site_effective_coverage_missing",
        ambiguous_code="site_effective_coverage_ambiguous",
        subject="operational site version",
    )
    return assignment


def _interval_relationship_rules(candidates: Sequence[_Candidate]) -> None:
    customer_versions = [
        item
        for item in _active_current(candidates, "customer_master")
        if item.record is not None and item.record.get("lifecycle_status") == "active"
    ]
    site_versions = [
        item
        for item in _active_current(candidates, "industrial_site_master")
        if item.record is not None and item.record.get("operational_status") == "operational"
    ]
    delivery_assignments = _active_current(candidates, "delivery_point_assignment")
    contract_versions = _active_current(candidates, "contract_terms")
    meter_assignments = [
        item
        for item in _active_current(candidates, "revenue_meter_assignment")
        if item.record is not None and item.record.get("assignment_role") == "authoritative_revenue"
    ]
    commitments = _active_current(candidates, "commitment_schedule")

    eligible = [
        item
        for item in candidates
        if item.record is not None and not item.issues and item.exact_replay_of is None
    ]
    for candidate in eligible:
        assert candidate.record is not None
        row = candidate.record

        if candidate.dataset in INTERVAL_DATASETS:
            start = _parse_utc(row["interval_start_utc"])
            end = _parse_utc(row["interval_end_utc"])
            assignment = _assignment_context(
                candidate,
                start=start,
                end=end,
                delivery_point_id=row["delivery_point_natural_id"],
                delivery_assignments=delivery_assignments,
                customer_versions=customer_versions,
                site_versions=site_versions,
            )
            if assignment is not None and candidate.dataset in {
                "commitment_schedule",
                "approved_excess_order",
            }:
                assert assignment.record is not None
                if row["customer_natural_id"] != assignment.record["customer_natural_id"]:
                    candidate.add_issue(
                        "assignment_customer_mismatch",
                        "copied customer does not match the event-time delivery-point assignment",
                        field_name="customer_natural_id",
                    )

                contracts = _coverage_matches(
                    contract_versions,
                    start=start,
                    end=end,
                    predicates={"contract_natural_id": row["contract_natural_id"]},
                )
                contract = _require_single_coverage(
                    candidate,
                    contracts,
                    missing_code="contract_effective_coverage_missing",
                    ambiguous_code="contract_effective_coverage_ambiguous",
                    subject="contract-terms version",
                )
                if contract is not None:
                    assert contract.record is not None
                    if (
                        contract.record["delivery_point_natural_id"]
                        != row["delivery_point_natural_id"]
                    ):
                        candidate.add_issue(
                            "contract_delivery_point_mismatch",
                            "contract does not belong to the record's delivery point",
                            field_name="contract_natural_id",
                        )
                    if contract.record["customer_natural_id"] != row["customer_natural_id"]:
                        candidate.add_issue(
                            "contract_customer_mismatch",
                            "contract customer does not match the interval record customer",
                            field_name="contract_natural_id",
                        )

            if candidate.dataset == "approved_excess_order":
                schedule_matches = [
                    item
                    for item in commitments
                    if item.record is not None
                    and item.record["delivery_point_natural_id"] == row["delivery_point_natural_id"]
                    and item.record["interval_start_utc"] == row["interval_start_utc"]
                ]
                schedule = _require_single_coverage(
                    candidate,
                    schedule_matches,
                    missing_code="positive_commitment_missing",
                    ambiguous_code="commitment_current_version_ambiguous",
                    subject="current base commitment",
                )
                if schedule is not None:
                    assert schedule.record is not None
                    committed = Decimal(schedule.record.get("committed_mwh_th", "0"))
                    if schedule.record.get("obligation_status") != "committed" or committed <= 0:
                        candidate.add_issue(
                            "positive_commitment_missing",
                            "an excess allocation requires a positive current base commitment",
                        )

        elif candidate.dataset == "revenue_meter_reading":
            reading_at = _parse_utc(row["reading_at_utc"])
            meters = _coverage_matches(
                meter_assignments,
                start=reading_at,
                end=None,
                predicates={
                    "meter_natural_id": row["meter_natural_id"],
                    "register_natural_id": row["register_natural_id"],
                },
            )
            meter = _require_single_coverage(
                candidate,
                meters,
                missing_code="meter_assignment_coverage_missing",
                ambiguous_code="meter_assignment_coverage_ambiguous",
                subject="authoritative meter assignment",
            )
            if meter is not None:
                assert meter.record is not None
                for field_name in ("register_type", "native_unit"):
                    if row.get(field_name) != meter.record.get(field_name):
                        candidate.add_issue(
                            "meter_register_metadata_mismatch",
                            f"reading {field_name} does not match its effective meter assignment",
                            field_name=field_name,
                        )


def _build_result(
    candidates: Sequence[_Candidate], bundle_issues: Sequence[ValidationIssue]
) -> BundleValidationResult:
    accepted: dict[str, list[ValidatedRecord]] = defaultdict(list)
    quarantined: dict[str, list[QuarantinedRecord]] = defaultdict(list)
    replays: list[ReplayRecord] = []
    accepted_line_by_identity: dict[str, int] = {}

    for candidate in sorted(candidates, key=lambda item: (item.dataset, item.line_number)):
        if candidate.exact_replay_of is not None and not candidate.issues:
            identity = candidate.exact_replay_of
            replays.append(
                ReplayRecord(
                    dataset=candidate.dataset,
                    line_number=candidate.line_number,
                    source_revision_identity=identity,
                    payload_sha256=candidate.payload_sha256 or "",
                    accepted_line_number=accepted_line_by_identity.get(identity, 0),
                )
            )
            continue
        if candidate.issues or candidate.record is None:
            quarantined[candidate.dataset].append(
                QuarantinedRecord(
                    dataset=candidate.dataset,
                    line_number=candidate.line_number,
                    issues=tuple(sorted(candidate.issues)),
                    source_revision_identity=candidate.source_revision_identity,
                    payload_sha256=candidate.payload_sha256,
                    record=candidate.record,
                    raw_text=candidate.raw_text if candidate.record is None else None,
                )
            )
            continue
        assert candidate.source_revision_identity is not None
        assert candidate.payload_sha256 is not None
        accepted_line_by_identity[candidate.source_revision_identity] = candidate.line_number
        accepted[candidate.dataset].append(
            ValidatedRecord(
                dataset=candidate.dataset,
                line_number=candidate.line_number,
                source_revision_identity=candidate.source_revision_identity,
                payload_sha256=candidate.payload_sha256,
                record=candidate.record,
            )
        )

    datasets = sorted({item.dataset for item in candidates} | set(DATASET_FILES))
    return BundleValidationResult(
        accepted={name: tuple(accepted.get(name, ())) for name in datasets},
        quarantined={name: tuple(quarantined.get(name, ())) for name in datasets},
        exact_replays=tuple(replays),
        bundle_issues=tuple(sorted(bundle_issues)),
    )


def _validate_candidates(
    candidates: Sequence[_Candidate],
    *,
    contracts_dir: Path,
    bundle_issues: Sequence[ValidationIssue] = (),
) -> BundleValidationResult:
    validators = _load_validators(contracts_dir)
    _schema_validate(candidates, validators)
    _detect_duplicate_revisions(candidates)
    _detect_opaque_id_reuse(candidates)
    _local_business_rules(candidates)
    _direct_reference_rules(candidates)
    _effective_overlap_rules(candidates)
    # Overlap checks can quarantine a parent that looked valid during the
    # first pass, so propagate that invalidity to direct children before any
    # interval coverage rules select event-time context.
    _direct_reference_rules(candidates)
    _interval_relationship_rules(candidates)
    return _build_result(candidates, bundle_issues)


def validate_record_collections(
    records_by_dataset: Mapping[str, Iterable[Mapping[str, Any]]],
    *,
    contracts_dir: Path = DEFAULT_CONTRACTS_DIR,
    require_all_synthetic_datasets: bool = True,
) -> BundleValidationResult:
    """Validate in-memory records without performing file I/O.

    This is the preferred unit-test API.  Mapping iteration order does not
    affect identities, rules, or the serialized report.
    """

    candidates: list[_Candidate] = []
    bundle_issues: list[ValidationIssue] = []
    supplied = set(records_by_dataset)
    supported = set(SOURCE_REVISION_KEY_FIELDS)
    for unknown in sorted(supplied - supported):
        bundle_issues.append(
            ValidationIssue("unsupported_dataset", f"unsupported source dataset: {unknown}")
        )
    if require_all_synthetic_datasets:
        for missing in sorted(set(DATASET_FILES) - supplied):
            bundle_issues.append(
                ValidationIssue(
                    "dataset_missing",
                    f"required synthetic source dataset is missing: {missing}",
                )
            )

    for dataset in sorted(supplied & supported):
        for line_number, source_record in enumerate(records_by_dataset[dataset], start=1):
            if not isinstance(source_record, Mapping):
                candidates.append(
                    _Candidate(
                        dataset=dataset,
                        line_number=line_number,
                        record=None,
                        raw_text=repr(source_record),
                        issues=[
                            ValidationIssue(
                                "record_not_object", "a source record must be a JSON object"
                            )
                        ],
                    )
                )
                continue
            # Round-trip through a standard dict to detach the result from
            # caller mutation while retaining JSON-native scalar values.
            candidates.append(
                _Candidate(dataset=dataset, line_number=line_number, record=dict(source_record))
            )
    return _validate_candidates(
        candidates,
        contracts_dir=contracts_dir,
        bundle_issues=bundle_issues,
    )


def _reject_json_constant(token: str) -> None:
    raise ValueError(f"non-standard JSON constant {token!r}")


class _DuplicateJsonKey(ValueError):
    def __init__(self, key: str):
        self.key = key
        super().__init__(f"duplicate JSON object key {key!r}")


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateJsonKey(key)
        value[key] = item
    return value


def _read_jsonl_candidates(dataset: str, path: Path) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            raw_text = raw_line.rstrip("\r\n")
            if not raw_text.strip():
                candidates.append(
                    _Candidate(
                        dataset=dataset,
                        line_number=line_number,
                        record=None,
                        raw_text=raw_text,
                        issues=[
                            ValidationIssue(
                                "empty_jsonl_line", "JSONL lines must not be empty"
                            )
                        ],
                    )
                )
                continue
            try:
                value = json.loads(
                    raw_text,
                    parse_constant=_reject_json_constant,
                    object_pairs_hook=_reject_duplicate_json_keys,
                )
            except _DuplicateJsonKey as exc:
                candidates.append(
                    _Candidate(
                        dataset=dataset,
                        line_number=line_number,
                        record=None,
                        raw_text=raw_text,
                        issues=[
                            ValidationIssue(
                                "duplicate_json_key",
                                f"JSON object repeats key {exc.key!r}",
                                field=exc.key,
                            )
                        ],
                    )
                )
                continue
            except (json.JSONDecodeError, ValueError) as exc:
                if isinstance(exc, json.JSONDecodeError):
                    detail = f"invalid JSON at column {exc.colno}: {exc.msg}"
                else:
                    detail = f"invalid JSON: {exc}"
                candidates.append(
                    _Candidate(
                        dataset=dataset,
                        line_number=line_number,
                        record=None,
                        raw_text=raw_text,
                        issues=[
                            ValidationIssue(
                                "invalid_json",
                                detail,
                            )
                        ],
                    )
                )
                continue
            if not isinstance(value, dict):
                candidates.append(
                    _Candidate(
                        dataset=dataset,
                        line_number=line_number,
                        record=None,
                        raw_text=raw_text,
                        issues=[
                            ValidationIssue(
                                "record_not_object", "a source JSONL record must be an object"
                            )
                        ],
                    )
                )
                continue
            candidates.append(_Candidate(dataset=dataset, line_number=line_number, record=value))
    return candidates


def _jsonl_content(records: Iterable[Mapping[str, Any]]) -> bytes:
    return b"".join(canonical_payload_bytes(record) + b"\n" for record in records)


def _write_atomic(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    if temporary.exists():
        temporary.unlink()
    with temporary.open("wb") as handle:
        handle.write(content)
    temporary.replace(path)


def _write_result(
    result: BundleValidationResult, output_dir: Path, *, overwrite: bool
) -> BundleValidationResult:
    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists() and not output_dir.is_dir():
        raise ValueError(f"validation output path is not a directory: {output_dir}")
    accepted_dir = output_dir / "accepted"
    quarantine_dir = output_dir / "quarantine"
    targets = [
        *(accepted_dir / filename for filename in DATASET_FILES.values()),
        *(quarantine_dir / filename for filename in DATASET_FILES.values()),
        output_dir / "validation_report.json",
    ]
    existing = [path for path in targets if path.exists()]
    if existing and not overwrite:
        names = ", ".join(str(path.relative_to(output_dir)) for path in existing)
        raise FileExistsError(
            f"validation targets already exist ({names}); use a new run directory or overwrite=True"
        )

    output_files: list[dict[str, Any]] = []
    for dataset, filename in DATASET_FILES.items():
        accepted_content = _jsonl_content(
            item.record for item in result.accepted.get(dataset, ())
        )
        quarantine_content = _jsonl_content(
            item.to_dict() for item in result.quarantined.get(dataset, ())
        )
        for category, path, content, count in (
            (
                "accepted",
                accepted_dir / filename,
                accepted_content,
                len(result.accepted.get(dataset, ())),
            ),
            (
                "quarantine",
                quarantine_dir / filename,
                quarantine_content,
                len(result.quarantined.get(dataset, ())),
            ),
        ):
            _write_atomic(path, content)
            output_files.append(
                {
                    "category": category,
                    "dataset": dataset,
                    "file": str(path.relative_to(output_dir)),
                    "record_count": count,
                    "sha256": hashlib.sha256(content).hexdigest(),
                }
            )

    result_with_outputs = replace(result, output_files=tuple(output_files))
    report = result_with_outputs.report_dict()
    report_content = (
        json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        + "\n"
    ).encode("utf-8")
    _write_atomic(output_dir / "validation_report.json", report_content)
    return result_with_outputs


def validate_bundle(
    source_dir: Path,
    output_dir: Path,
    *,
    contracts_dir: Path = DEFAULT_CONTRACTS_DIR,
    dataset_files: Mapping[str, str] = DATASET_FILES,
    require_all_synthetic_datasets: bool = True,
    overwrite: bool = False,
) -> BundleValidationResult:
    """Validate a bounded directory of raw JSONL and write deterministic outputs.

    Missing files are bundle-level issues rather than exceptions.  A readable
    but invalid line becomes one quarantine record.  Filesystem failures and
    pre-existing output targets raise so orchestration can retry safely.
    """

    source_dir = source_dir.expanduser().resolve()
    if not source_dir.is_dir():
        raise NotADirectoryError(
            f"generated source-files directory does not exist: {source_dir}"
        )

    candidates: list[_Candidate] = []
    bundle_issues: list[ValidationIssue] = []
    for dataset, filename in sorted(dataset_files.items()):
        if dataset not in SOURCE_REVISION_KEY_FIELDS:
            bundle_issues.append(
                ValidationIssue("unsupported_dataset", f"unsupported source dataset: {dataset}")
            )
            continue
        path = source_dir / filename
        if not path.is_file():
            if require_all_synthetic_datasets or dataset in DATASET_FILES:
                bundle_issues.append(
                    ValidationIssue(
                        "dataset_file_missing",
                        f"required JSONL file is missing: {filename}",
                    )
                )
            continue
        candidates.extend(_read_jsonl_candidates(dataset, path))

    if require_all_synthetic_datasets:
        configured = set(dataset_files)
        for missing in sorted(set(DATASET_FILES) - configured):
            bundle_issues.append(
                ValidationIssue(
                    "dataset_missing",
                    f"required synthetic source dataset is not configured: {missing}",
                )
            )

    result = _validate_candidates(
        candidates,
        contracts_dir=contracts_dir,
        bundle_issues=bundle_issues,
    )
    return _write_result(result, output_dir, overwrite=overwrite)


__all__ = [
    "BundleValidationResult",
    "DATASET_FILES",
    "DEFAULT_CONTRACTS_DIR",
    "QuarantinedRecord",
    "ReplayRecord",
    "ValidatedRecord",
    "ValidationIssue",
    "canonical_payload_bytes",
    "payload_sha256",
    "source_revision_identity",
    "source_revision_key",
    "validate_bundle",
    "validate_record_collections",
]
