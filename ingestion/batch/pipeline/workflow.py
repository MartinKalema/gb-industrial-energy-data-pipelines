"""Airflow-facing functions for the finite Phase 2 batch workflow.

Every public function accepts and returns small JSON-serializable dictionaries.
Records travel through the shared work volume and R2, never through Airflow
XCom. The functions are also callable without Airflow for integration tests.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

from ingestion.batch.synthetic.generate import DATASET_FILES, write_bundle

from .models import PipelineError, RunPlan, build_run_plan, parse_utc_timestamp
from .storage import ObjectStore, R2ObjectStore, content_sha256


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
CONTRACTS_DIR = REPOSITORY_ROOT / "contracts"


def _canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(
        (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
    )
    temporary.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError(f"cannot read JSON artifact {path}") from exc
    if not isinstance(value, dict):
        raise PipelineError(f"expected a JSON object in {path}")
    return value


def _validator(schema_name: str) -> Draft202012Validator:
    common = _read_json(CONTRACTS_DIR / "common.schema.json")
    schema = _read_json(CONTRACTS_DIR / f"{schema_name}.schema.json")
    registry = Registry().with_resource(
        "common.schema.json", Resource.from_contents(common)
    )
    return Draft202012Validator(
        schema, registry=registry, format_checker=FormatChecker()
    )


def _require_mapping(value: Mapping[str, Any] | Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PipelineError(f"{name} must be a mapping")
    return value


def _object_store(store: ObjectStore | None) -> ObjectStore:
    return store or R2ObjectStore.from_environment()


def plan_run(
    *,
    start_date: str,
    end_date: str,
    seed: int,
    generation_time_utc: str,
    orchestrator_run_id: str,
    environment: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Plan one run, persist the plan locally, and return only non-secret data."""

    plan = build_run_plan(
        start_date=start_date,
        end_date=end_date,
        seed=seed,
        generation_time_utc=generation_time_utc,
        orchestrator_run_id=orchestrator_run_id,
        environment=environment,
    )
    work_dir = Path(plan.work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(work_dir / "run-plan.json", plan.to_dict())
    return plan.to_dict()


def plan_run_from_airflow() -> dict[str, Any]:
    """Build a run plan from validated Airflow Params and current run context."""

    try:
        from airflow.sdk import get_current_context
    except ImportError as exc:  # pragma: no cover - only imported in Airflow image
        raise PipelineError("Airflow is required for plan_run_from_airflow") from exc
    context = get_current_context()
    params = context["params"]
    return plan_run(
        start_date=str(params["start_date"]),
        end_date=str(params["end_date"]),
        seed=int(params["seed"]),
        generation_time_utc=str(params["generation_time_utc"]),
        orchestrator_run_id=str(context["run_id"]),
    )


def generate_source_bundle(plan_value: Mapping[str, Any]) -> dict[str, Any]:
    """Generate the nine source files into the shared, run-scoped work folder."""

    plan = RunPlan.from_mapping(_require_mapping(plan_value, "plan"))
    source_dir = Path(plan.work_dir) / "generated"
    generated_at = parse_utc_timestamp(
        plan.generation_time_utc, "generation_time_utc"
    )
    manifest = write_bundle(
        start_date=datetime.fromisoformat(plan.start_date).date(),
        end_date=datetime.fromisoformat(plan.end_date).date(),
        seed=plan.seed,
        generation_time_utc=generated_at,
        output_dir=source_dir,
        overwrite=True,
    )
    if len(manifest["datasets"]) != len(DATASET_FILES):
        raise PipelineError("generator did not produce all nine accepted datasets")
    for item in manifest["datasets"]:
        content = (source_dir / item["file"]).read_bytes()
        if content_sha256(content) != item["sha256"]:
            raise PipelineError(f"generated hash mismatch for {item['file']}")
        if len(content.splitlines()) != item["record_count"]:
            raise PipelineError(f"generated row-count mismatch for {item['file']}")
    return {
        "pipeline_run_id": plan.pipeline_run_id,
        "source_dir": str(source_dir),
        "manifest_path": str(source_dir / "manifest.json"),
        "dataset_count": len(manifest["datasets"]),
        "total_record_count": manifest["total_record_count"],
    }


def _raw_object_key(plan: RunPlan, item: Mapping[str, Any]) -> str:
    return (
        f"{plan.raw_prefix}/raw/synthetic/{item['dataset']}/"
        f"start_date={plan.start_date}/end_date={plan.end_date}/"
        f"schema_version={item['source_schema_version']}/"
        f"sha256={item['sha256']}/{item['file']}"
    )


def _evidence_envelope(
    plan: RunPlan, item: Mapping[str, Any], object_info: Mapping[str, Any]
) -> dict[str, Any]:
    identity_digest = hashlib.sha256(
        f"{plan.pipeline_run_id}:{item['dataset']}:{item['sha256']}".encode()
    ).hexdigest()[:24]
    return {
        "envelope_schema_id": "raw_evidence_envelope",
        "envelope_schema_version": "1.0.0",
        "evidence_envelope_id": f"EVIDENCE-{identity_digest}",
        "source_dataset": item["dataset"],
        "source_system_id": f"synthetic.{item['dataset']}",
        "record_schema_id": item["source_schema_id"],
        "record_schema_version": item["source_schema_version"],
        "ingestion_method": "synthetic_generation",
        "ingested_at_utc": object_info["last_modified_utc"],
        "payload_sha256": item["sha256"],
        "raw_object_uri": object_info["uri"],
        "raw_record_locator": f"jsonl:1-{item['record_count']}",
        "content_type": "application/x-ndjson",
        "content_length_bytes": object_info["content_length"],
        "record_count": item["record_count"],
        "generator_run_id": plan.pipeline_run_id,
        "generator_seed": plan.seed,
    }


def land_raw_bundle(
    plan_value: Mapping[str, Any],
    generation_result_value: Mapping[str, Any],
    *,
    store: ObjectStore | None = None,
) -> dict[str, Any]:
    """Write source bytes and lineage envelopes to immutable R2 object keys."""

    plan = RunPlan.from_mapping(_require_mapping(plan_value, "plan"))
    generation_result = _require_mapping(generation_result_value, "generation_result")
    if generation_result.get("pipeline_run_id") != plan.pipeline_run_id:
        raise PipelineError("generation result belongs to another pipeline run")
    source_dir = Path(str(generation_result["source_dir"])).resolve()
    expected_source_dir = (Path(plan.work_dir) / "generated").resolve()
    if source_dir != expected_source_dir:
        raise PipelineError("generation result points outside the run work directory")
    manifest_path = Path(str(generation_result["manifest_path"])).resolve()
    if manifest_path != source_dir / "manifest.json":
        raise PipelineError("generation manifest points outside the source bundle")
    manifest = _read_json(manifest_path)
    storage = _object_store(store)
    envelope_validator = _validator("raw_evidence_envelope")
    raw_artifacts: list[dict[str, Any]] = []
    envelope_artifacts: list[dict[str, Any]] = []

    for item_value in manifest.get("datasets", []):
        item = _require_mapping(item_value, "manifest dataset")
        dataset = str(item["dataset"])
        if dataset not in DATASET_FILES or DATASET_FILES[dataset] != item["file"]:
            raise PipelineError(f"manifest contains unexpected dataset {dataset}")
        content = (source_dir / str(item["file"])).read_bytes()
        if content_sha256(content) != item["sha256"]:
            raise PipelineError(f"raw source changed after generation: {item['file']}")
        object_info = storage.put_immutable(
            bucket=plan.raw_bucket,
            key=_raw_object_key(plan, item),
            content=content,
            content_type="application/x-ndjson",
            metadata={
                "dataset": dataset,
                "schema-version": str(item["source_schema_version"]),
                "pipeline-run-id": plan.pipeline_run_id,
            },
        ).to_dict()
        raw_artifacts.append({"dataset": dataset, **object_info})

        envelope = _evidence_envelope(plan, item, object_info)
        errors = list(envelope_validator.iter_errors(envelope))
        if errors:
            raise PipelineError(
                f"generated evidence envelope failed at {errors[0].json_path}: "
                f"{errors[0].message}"
            )
        envelope_content = _canonical_json_bytes(envelope)
        envelope_key = (
            f"{plan.raw_prefix}/raw/_evidence/{plan.pipeline_run_id}/"
            f"{dataset}.envelope.json"
        )
        envelope_info = storage.put_immutable(
            bucket=plan.raw_bucket,
            key=envelope_key,
            content=envelope_content,
            content_type="application/json",
            metadata={
                "dataset": dataset,
                "pipeline-run-id": plan.pipeline_run_id,
                "raw-sha256": str(item["sha256"]),
            },
        ).to_dict()
        envelope_artifacts.append(
            {
                "dataset": dataset,
                "evidence_envelope_id": envelope["evidence_envelope_id"],
                **envelope_info,
            }
        )

    manifest_content = manifest_path.read_bytes()
    manifest_info = storage.put_immutable(
        bucket=plan.raw_bucket,
        key=(
            f"{plan.raw_prefix}/raw/_manifests/{plan.pipeline_run_id}/"
            "source-manifest.json"
        ),
        content=manifest_content,
        content_type="application/json",
        metadata={"pipeline-run-id": plan.pipeline_run_id},
    ).to_dict()
    result = {
        "pipeline_run_id": plan.pipeline_run_id,
        "raw_artifacts": raw_artifacts,
        "evidence_envelopes": envelope_artifacts,
        "raw_manifest": manifest_info,
        "raw_record_count": sum(int(item["record_count"]) for item in manifest["datasets"]),
    }
    _write_json_atomic(Path(plan.work_dir) / "raw-landing-result.json", result)
    return result


def validate_raw_bundle(
    plan_value: Mapping[str, Any],
    raw_result_value: Mapping[str, Any],
    *,
    store: ObjectStore | None = None,
) -> dict[str, Any]:
    """Download raw evidence, validate it, and publish accepted/quarantine artifacts."""

    # Imported lazily so DAG parsing does not perform validation setup.
    from .validation import validate_bundle

    plan = RunPlan.from_mapping(_require_mapping(plan_value, "plan"))
    raw_result = _require_mapping(raw_result_value, "raw_result")
    if raw_result.get("pipeline_run_id") != plan.pipeline_run_id:
        raise PipelineError("raw result belongs to another pipeline run")
    storage = _object_store(store)
    input_dir = Path(plan.work_dir) / "validation" / "input"
    output_dir = Path(plan.work_dir) / "validation" / "output"
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    for artifact_value in raw_result.get("raw_artifacts", []):
        artifact = _require_mapping(artifact_value, "raw artifact")
        dataset = str(artifact["dataset"])
        content = storage.get_bytes(bucket=plan.raw_bucket, key=str(artifact["key"]))
        if content_sha256(content) != artifact["sha256"]:
            raise PipelineError(f"R2 round-trip hash mismatch for {dataset}")
        target = input_dir / DATASET_FILES[dataset]
        target.write_bytes(content)

    validation = validate_bundle(
        source_dir=input_dir,
        output_dir=output_dir,
        contracts_dir=CONTRACTS_DIR,
        overwrite=True,
    )
    report = validation.report_dict()
    if report["bundle_issues"]:
        issue = report["bundle_issues"][0]
        raise PipelineError(
            "bundle validation could not complete: "
            f"{issue['code']}: {issue['message']}"
        )

    accepted_artifacts: list[dict[str, Any]] = []
    quarantine_artifacts: list[dict[str, Any]] = []
    file_reports = {
        (item["category"], item["dataset"]): item for item in report["output_files"]
    }
    for dataset in DATASET_FILES:
        dataset_report = next(
            item for item in report["datasets"] if item["dataset"] == dataset
        )
        accepted_file_report = file_reports[("accepted", dataset)]
        quarantine_file_report = file_reports[("quarantine", dataset)]
        accepted_path = output_dir / accepted_file_report["file"]
        quarantine_path = output_dir / quarantine_file_report["file"]
        accepted_content = accepted_path.read_bytes()
        quarantine_content = quarantine_path.read_bytes()
        if content_sha256(accepted_content) != accepted_file_report["sha256"]:
            raise PipelineError(f"accepted output hash mismatch for {dataset}")
        if content_sha256(quarantine_content) != quarantine_file_report["sha256"]:
            raise PipelineError(f"quarantine output hash mismatch for {dataset}")
        accepted_info = storage.put_immutable(
            bucket=plan.raw_bucket,
            key=(
                f"{plan.raw_prefix}/validated-staging/{plan.pipeline_run_id}/"
                f"{dataset}.accepted.jsonl"
            ),
            content=accepted_content,
            content_type="application/x-ndjson",
            metadata={"dataset": dataset, "pipeline-run-id": plan.pipeline_run_id},
        ).to_dict()
        quarantine_info = storage.put_immutable(
            bucket=plan.raw_bucket,
            key=(
                f"{plan.raw_prefix}/quarantine/{plan.pipeline_run_id}/"
                f"{dataset}.quarantine.jsonl"
            ),
            content=quarantine_content,
            content_type="application/x-ndjson",
            metadata={"dataset": dataset, "pipeline-run-id": plan.pipeline_run_id},
        ).to_dict()
        accepted_artifacts.append(
            {
                "dataset": dataset,
                "record_count": dataset_report["accepted_record_count"],
                "local_path": str(accepted_path),
                **accepted_info,
            }
        )
        quarantine_artifacts.append(
            {
                "dataset": dataset,
                "record_count": dataset_report["quarantined_record_count"],
                "local_path": str(quarantine_path),
                **quarantine_info,
            }
        )

    report_path = output_dir / "validation_report.json"
    report_content = report_path.read_bytes()
    report_info = storage.put_immutable(
        bucket=plan.raw_bucket,
        key=(
            f"{plan.raw_prefix}/quality/{plan.pipeline_run_id}/"
            "validation-report.json"
        ),
        content=report_content,
        content_type="application/json",
        metadata={"pipeline-run-id": plan.pipeline_run_id},
    ).to_dict()
    result = {
        "pipeline_run_id": plan.pipeline_run_id,
        "accepted_artifacts": accepted_artifacts,
        "quarantine_artifacts": quarantine_artifacts,
        "validation_report": report_info,
        "accepted_record_count": report["accepted_record_count"],
        "quarantined_record_count": report["quarantined_record_count"],
        "duplicate_record_count": report["exact_replay_count"],
        "validation_output_dir": str(output_dir),
    }
    _write_json_atomic(Path(plan.work_dir) / "validation-result.json", result)
    return result


def load_validated_bundle(
    plan_value: Mapping[str, Any],
    raw_result_value: Mapping[str, Any],
    validation_result_value: Mapping[str, Any],
) -> dict[str, Any]:
    """Load accepted revisions into typed Iceberg tables through finite Trino SQL."""

    from .trino_loader import AcceptedRecord, IcebergLoaderConfig, TrinoIcebergLoader
    from .validation import payload_sha256

    plan = RunPlan.from_mapping(_require_mapping(plan_value, "plan"))
    raw_result = _require_mapping(raw_result_value, "raw_result")
    validation_result = _require_mapping(validation_result_value, "validation_result")
    if raw_result.get("pipeline_run_id") != plan.pipeline_run_id:
        raise PipelineError("raw result belongs to another pipeline run")
    if validation_result.get("pipeline_run_id") != plan.pipeline_run_id:
        raise PipelineError("validation result belongs to another pipeline run")
    config = IcebergLoaderConfig(
        trino_endpoint=plan.trino_url,
        catalog=plan.iceberg_catalog,
        iceberg_schema=plan.iceberg_schema,
        trino_user=os.environ.get("TRINO_USER", "airflow"),
        query_timeout_seconds=float(
            os.environ.get("TRINO_QUERY_TIMEOUT_SECONDS", "300")
        ),
        chunk_size=int(os.environ.get("TRINO_INSERT_BATCH_SIZE", "200")),
        conflict_detail_limit=10,
    )
    loader = TrinoIcebergLoader(config=config, contracts_dir=CONTRACTS_DIR)

    raw_by_dataset = {
        str(item["dataset"]): item for item in raw_result["raw_artifacts"]
    }
    envelope_by_dataset = {
        str(item["dataset"]): item for item in raw_result["evidence_envelopes"]
    }
    accepted_by_dataset = {
        str(item["dataset"]): item for item in validation_result["accepted_artifacts"]
    }
    validation_output_dir = Path(
        str(validation_result["validation_output_dir"])
    ).resolve()
    expected_output_dir = (Path(plan.work_dir) / "validation" / "output").resolve()
    if validation_output_dir != expected_output_dir:
        raise PipelineError("validation result points outside the run work directory")
    report = _read_json(validation_output_dir / "validation_report.json")
    evidence_by_dataset: dict[str, list[Mapping[str, Any]]] = {
        dataset: [] for dataset in DATASET_FILES
    }
    for item in report["accepted_evidence"]:
        evidence_by_dataset[str(item["dataset"])].append(item)

    dataset_results: list[dict[str, Any]] = []
    for dataset in DATASET_FILES:
        artifact = accepted_by_dataset[dataset]
        accepted_path = Path(str(artifact["local_path"])).resolve()
        if accepted_path.parent != validation_output_dir / "accepted":
            raise PipelineError(f"accepted path is outside validation output for {dataset}")
        accepted_content = accepted_path.read_bytes()
        if content_sha256(accepted_content) != artifact["sha256"]:
            raise PipelineError(f"accepted artifact changed before load for {dataset}")
        payloads = [
            json.loads(line)
            for line in accepted_content.decode("utf-8").splitlines()
            if line.strip()
        ]
        evidence_rows = evidence_by_dataset[dataset]
        if len(payloads) != len(evidence_rows):
            raise PipelineError(f"accepted lineage count mismatch for {dataset}")
        raw_artifact = raw_by_dataset[dataset]
        envelope = envelope_by_dataset[dataset]
        accepted_records: list[AcceptedRecord] = []
        for payload, evidence in zip(payloads, evidence_rows, strict=True):
            if payload_sha256(payload) != evidence["payload_sha256"]:
                raise PipelineError(f"accepted record hash mismatch for {dataset}")
            accepted_records.append(
                AcceptedRecord(
                    payload=payload,
                    pipeline_run_id=plan.pipeline_run_id,
                    evidence_envelope_id=str(envelope["evidence_envelope_id"]),
                    ingested_at_utc=str(raw_artifact["last_modified_utc"]),
                    raw_object_uri=str(raw_artifact["uri"]),
                    raw_object_sha256=str(raw_artifact["sha256"]),
                    raw_record_locator=f"line:{int(evidence['line_number'])}",
                )
            )
        dataset_results.append(
            loader.load_records(dataset, accepted_records).to_dict()
        )

    result = {
        "pipeline_run_id": plan.pipeline_run_id,
        "inserted_count": sum(item["inserted_records"] for item in dataset_results),
        "reused_count": sum(
            item["skipped_exact_replays"] for item in dataset_results
        ),
        "conflict_count": sum(item["conflict_records"] for item in dataset_results),
        "table_count": len(dataset_results),
        "datasets": dataset_results,
    }
    _write_json_atomic(Path(plan.work_dir) / "iceberg-load-result.json", result)
    return result


def reconcile_run(
    plan_value: Mapping[str, Any],
    raw_result_value: Mapping[str, Any],
    validation_result_value: Mapping[str, Any],
    load_result_value: Mapping[str, Any],
    *,
    store: ObjectStore | None = None,
) -> dict[str, Any]:
    """Prove that raw, validated, quarantined, and Iceberg counts reconcile."""

    plan = RunPlan.from_mapping(_require_mapping(plan_value, "plan"))
    raw_result = _require_mapping(raw_result_value, "raw_result")
    validation_result = _require_mapping(validation_result_value, "validation_result")
    load_result = _require_mapping(load_result_value, "load_result")
    for name, value in (
        ("raw", raw_result),
        ("validation", validation_result),
        ("load", load_result),
    ):
        if value.get("pipeline_run_id") != plan.pipeline_run_id:
            raise PipelineError(f"{name} result belongs to another pipeline run")

    raw_count = int(raw_result["raw_record_count"])
    accepted = int(validation_result["accepted_record_count"])
    quarantined = int(validation_result["quarantined_record_count"])
    duplicates = int(validation_result["duplicate_record_count"])
    if raw_count != accepted + quarantined + duplicates:
        raise PipelineError(
            "validation counts do not reconcile: "
            f"raw={raw_count} accepted={accepted} quarantined={quarantined} "
            f"duplicates={duplicates}"
        )
    loader_conflicts = int(load_result.get("conflict_count", 0))
    loaded_or_reused = int(load_result["inserted_count"]) + int(
        load_result["reused_count"]
    )
    if loader_conflicts:
        raise PipelineError(f"Iceberg loader found {loader_conflicts} immutable conflicts")
    if loaded_or_reused != accepted:
        raise PipelineError(
            f"Iceberg count mismatch: accepted={accepted} inserted_or_reused={loaded_or_reused}"
        )
    summary = {
        "pipeline_run_id": plan.pipeline_run_id,
        "orchestrator_run_id": plan.orchestrator_run_id,
        "status": "succeeded_with_quarantine" if quarantined else "succeeded",
        "raw_record_count": raw_count,
        "accepted_record_count": accepted,
        "quarantined_record_count": quarantined,
        "duplicate_record_count": duplicates,
        "iceberg_inserted_count": int(load_result["inserted_count"]),
        "iceberg_reused_count": int(load_result["reused_count"]),
        "iceberg_table_count": int(load_result["table_count"]),
    }
    content = _canonical_json_bytes(summary)
    storage = _object_store(store)
    attempt_digest = hashlib.sha256(plan.orchestrator_run_id.encode("utf-8")).hexdigest()[:16]
    artifact = storage.put_immutable(
        bucket=plan.raw_bucket,
        key=(
            f"{plan.raw_prefix}/quality/{plan.pipeline_run_id}/"
            f"reconciliation/attempt={attempt_digest}.summary.json"
        ),
        content=content,
        content_type="application/json",
        metadata={"pipeline-run-id": plan.pipeline_run_id},
    ).to_dict()
    result = {**summary, "reconciliation_artifact": artifact}
    _write_json_atomic(Path(plan.work_dir) / "reconciliation-result.json", result)
    return result


def publish_batch_run_coverage(
    plan_value: Mapping[str, Any],
    raw_result_value: Mapping[str, Any],
    reconciliation_result_value: Mapping[str, Any],
    *,
    statement_runner: Any | None = None,
) -> dict[str, Any]:
    """Publish one successful-run coverage declaration to the Iceberg control table.

    The returned dictionary contains only the run identity, relation, canonical
    hash, and create/reuse disposition.  Row content remains in Iceberg rather
    than travelling through Airflow XCom.
    """

    from .coverage_control import (
        BatchRunCoverage,
        BatchRunCoveragePublisher,
        CoveragePublisherConfig,
    )

    plan = RunPlan.from_mapping(_require_mapping(plan_value, "plan"))
    raw_result = _require_mapping(raw_result_value, "raw_result")
    reconciliation_result = _require_mapping(
        reconciliation_result_value, "reconciliation_result"
    )
    coverage = BatchRunCoverage.from_workflow(
        plan,
        raw_result,
        reconciliation_result,
    )
    publisher = BatchRunCoveragePublisher(
        CoveragePublisherConfig(
            trino_endpoint=plan.trino_url,
            catalog=plan.iceberg_catalog,
            trino_user=os.environ.get("TRINO_USER", "airflow"),
            query_timeout_seconds=float(
                os.environ.get("TRINO_QUERY_TIMEOUT_SECONDS", "300")
            ),
        ),
        statement_runner=statement_runner,
    )
    result = publisher.publish(coverage).to_dict()
    _write_json_atomic(Path(plan.work_dir) / "coverage-publication-result.json", result)
    return result
