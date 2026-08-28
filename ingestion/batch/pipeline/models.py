"""Small serializable contracts shared by the bounded batch tasks."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping


UTC = timezone.utc
DEFAULT_MAX_BATCH_DAYS = 31
SAFE_RUN_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
SAFE_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


class PipelineError(RuntimeError):
    """Base error for a run that must fail without advancing the workflow."""


class ImmutableObjectConflict(PipelineError):
    """An immutable object key already exists with different content."""


def parse_iso_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise PipelineError(f"{field_name} must use YYYY-MM-DD") from exc


def parse_utc_timestamp(value: str, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise PipelineError(f"{field_name} must be an RFC 3339 UTC string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PipelineError(f"{field_name} must be an RFC 3339 UTC string") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise PipelineError(f"{field_name} must carry Z or +00:00")
    return parsed.astimezone(UTC)


def utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def require_environment(name: str, environment: Mapping[str, str]) -> str:
    value = environment.get(name, "").strip()
    if not value:
        raise PipelineError(f"required environment variable {name} is missing")
    return value


@dataclass(frozen=True)
class RunPlan:
    """The complete non-secret contract for one deterministic bounded run."""

    pipeline_run_id: str
    orchestrator_run_id: str
    start_date: str
    end_date: str
    seed: int
    generation_time_utc: str
    work_dir: str
    raw_bucket: str
    raw_prefix: str
    iceberg_catalog: str
    iceberg_schema: str
    trino_url: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "RunPlan":
        try:
            plan = cls(
                pipeline_run_id=str(value["pipeline_run_id"]),
                orchestrator_run_id=str(value["orchestrator_run_id"]),
                start_date=str(value["start_date"]),
                end_date=str(value["end_date"]),
                seed=int(value["seed"]),
                generation_time_utc=str(value["generation_time_utc"]),
                work_dir=str(value["work_dir"]),
                raw_bucket=str(value["raw_bucket"]),
                raw_prefix=str(value["raw_prefix"]),
                iceberg_catalog=str(value["iceberg_catalog"]),
                iceberg_schema=str(value["iceberg_schema"]),
                trino_url=str(value["trino_url"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise PipelineError("run plan is incomplete or malformed") from exc
        plan.validate()
        return plan

    def validate(self, *, max_batch_days: int = DEFAULT_MAX_BATCH_DAYS) -> None:
        start = parse_iso_date(self.start_date, "start_date")
        end = parse_iso_date(self.end_date, "end_date")
        day_count = (end - start).days + 1
        if day_count < 1:
            raise PipelineError("end_date must be on or after start_date")
        if day_count > max_batch_days:
            raise PipelineError(
                f"requested {day_count} local dates exceeds the {max_batch_days}-day bound"
            )
        if self.seed < 0:
            raise PipelineError("seed must be a non-negative integer")
        parse_utc_timestamp(self.generation_time_utc, "generation_time_utc")
        if not SAFE_RUN_IDENTIFIER.fullmatch(self.pipeline_run_id):
            raise PipelineError("pipeline_run_id contains unsupported characters")
        for field_name, identifier in (
            ("iceberg_catalog", self.iceberg_catalog),
            ("iceberg_schema", self.iceberg_schema),
        ):
            if not SAFE_SQL_IDENTIFIER.fullmatch(identifier):
                raise PipelineError(f"{field_name} contains unsupported characters")
        if not self.raw_bucket or "/" in self.raw_bucket:
            raise PipelineError("raw_bucket must be one bucket name")
        if (
            not self.raw_prefix
            or self.raw_prefix.startswith("/")
            or any(part in {"", ".", ".."} for part in self.raw_prefix.split("/"))
        ):
            raise PipelineError("raw_prefix must be a relative object prefix")
        if not self.trino_url.startswith(("http://", "https://")):
            raise PipelineError("trino_url must be HTTP or HTTPS")
        work_path = Path(self.work_dir)
        if not work_path.is_absolute():
            raise PipelineError("work_dir must be absolute")


def build_run_plan(
    *,
    start_date: str,
    end_date: str,
    seed: int,
    generation_time_utc: str,
    orchestrator_run_id: str,
    environment: Mapping[str, str] | None = None,
) -> RunPlan:
    """Create and validate a deterministic run plan without copying secrets."""

    environment = environment or os.environ
    start = parse_iso_date(start_date, "start_date")
    end = parse_iso_date(end_date, "end_date")
    generated_at = utc_text(
        parse_utc_timestamp(generation_time_utc, "generation_time_utc")
    )
    identity_payload = {
        "end_date": end.isoformat(),
        "generation_time_utc": generated_at,
        "seed": int(seed),
        "start_date": start.isoformat(),
    }
    digest = hashlib.sha256(
        json.dumps(identity_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:16]
    pipeline_run_id = f"batch-{start:%Y%m%d}-{end:%Y%m%d}-{digest}"
    work_root = Path(environment.get("PIPELINE_WORK_ROOT", "tmp/airflow-batch"))
    if not work_root.is_absolute():
        repository_root = Path(__file__).resolve().parents[3]
        work_root = repository_root / work_root
    trino_host = environment.get("TRINO_HOST", "trino")
    trino_port = environment.get("TRINO_PORT", "8080")
    plan = RunPlan(
        pipeline_run_id=pipeline_run_id,
        orchestrator_run_id=orchestrator_run_id,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        seed=int(seed),
        generation_time_utc=generated_at,
        work_dir=str((work_root / pipeline_run_id).resolve()),
        raw_bucket=require_environment("R2_RAW_BUCKET", environment),
        raw_prefix=environment.get("R2_PIPELINE_PREFIX", "industrial-energy").strip("/"),
        iceberg_catalog=environment.get("ICEBERG_CATALOG", "r2"),
        iceberg_schema=environment.get(
            "ICEBERG_VALIDATED_SCHEMA", "industrial_energy_validated"
        ),
        trino_url=environment.get(
            "TRINO_URL", f"http://{trino_host}:{trino_port}"
        ).rstrip("/"),
    )
    max_days = int(environment.get("PIPELINE_MAX_BATCH_DAYS", DEFAULT_MAX_BATCH_DAYS))
    plan.validate(max_batch_days=max_days)
    return plan
