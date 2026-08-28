from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

from ingestion.batch.pipeline import trino_loader
from ingestion.batch.pipeline.workflow import (
    generate_source_bundle,
    land_raw_bundle,
    load_validated_bundle,
    plan_run,
    reconcile_run,
    validate_raw_bundle,
)
from tests.pipeline.fakes import FakeObjectStore


def _raw_and_validated(tmp_path: Path):
    plan = plan_run(
        start_date="2026-08-27",
        end_date="2026-08-27",
        seed=20260828,
        generation_time_utc="2026-08-28T12:00:00Z",
        orchestrator_run_id="manual__workflow-test",
        environment={
            "R2_RAW_BUCKET": "raw-test",
            "PIPELINE_WORK_ROOT": str(tmp_path / "work"),
            "TRINO_URL": "http://trino:8080",
        },
    )
    store = FakeObjectStore(tmp_path / "fake-r2")
    generated = generate_source_bundle(plan)
    raw = land_raw_bundle(plan, generated, store=store)
    validated = validate_raw_bundle(plan, raw, store=store)
    return plan, raw, validated, store


def test_validation_publishes_accepted_quarantine_and_report_to_r2(
    tmp_path: Path,
) -> None:
    plan, raw, validated, store = _raw_and_validated(tmp_path)

    assert validated["accepted_record_count"] == raw["raw_record_count"] == 313
    assert validated["quarantined_record_count"] == 0
    assert validated["duplicate_record_count"] == 0
    assert len(validated["accepted_artifacts"]) == 9
    assert len(validated["quarantine_artifacts"]) == 9
    assert len(json.dumps(raw, separators=(",", ":")).encode()) < 16 * 1024
    assert len(json.dumps(validated, separators=(",", ":")).encode()) < 16 * 1024
    assert all(item["uri"].startswith("r2://raw-test/") for item in validated["accepted_artifacts"])
    assert all(item["content_length"] == 0 for item in validated["quarantine_artifacts"])
    report = store.get_bytes(
        bucket=plan["raw_bucket"], key=validated["validation_report"]["key"]
    )
    assert b'"report_schema_id": "bounded_bundle_validation_report"' in report


@dataclass
class _FakeLoadResult:
    dataset: str
    count: int

    def to_dict(self):
        return {
            "dataset": self.dataset,
            "table": f'r2.industrial_energy_validated."{self.dataset}"',
            "dry_run": False,
            "database_checks_performed": True,
            "input_records": self.count,
            "planned_new_records": self.count,
            "inserted_records": self.count,
            "skipped_exact_replays": 0,
            "conflict_records": 0,
            "conflict_identities": 0,
            "chunks_processed": 1 if self.count else 0,
            "conflicts": [],
            "conflict_details_truncated": False,
            "warnings": [],
            "succeeded_without_conflicts": True,
        }


def test_load_uses_original_raw_lineage_and_reconciles(
    tmp_path: Path, monkeypatch
) -> None:
    plan, raw, validated, store = _raw_and_validated(tmp_path)
    captured: dict[str, list[trino_loader.AcceptedRecord]] = {}

    class FakeLoader:
        def __init__(self, *args, **kwargs):
            pass

        def load_records(self, dataset, records):
            captured[dataset] = list(records)
            return _FakeLoadResult(dataset, len(captured[dataset]))

    monkeypatch.setattr(trino_loader, "TrinoIcebergLoader", FakeLoader)

    loaded = load_validated_bundle(plan, raw, validated)
    reconciled = reconcile_run(plan, raw, validated, loaded, store=store)
    replay_plan = {**plan, "orchestrator_run_id": "manual__workflow-replay"}
    replay_reconciled = reconcile_run(
        replay_plan, raw, validated, loaded, store=store
    )

    assert loaded["inserted_count"] == validated["accepted_record_count"]
    assert loaded["reused_count"] == 0
    assert loaded["conflict_count"] == 0
    assert loaded["table_count"] == 9
    first = captured["customer_master"][0]
    customer_raw = next(
        item for item in raw["raw_artifacts"] if item["dataset"] == "customer_master"
    )
    assert first.raw_object_uri == customer_raw["uri"]
    assert first.raw_object_sha256 == customer_raw["sha256"]
    assert first.evidence_envelope_id.startswith("EVIDENCE-")
    assert first.raw_record_locator == "line:1"
    assert reconciled["status"] == "succeeded"
    assert reconciled["raw_record_count"] == 313
    assert replay_reconciled["pipeline_run_id"] == reconciled["pipeline_run_id"]
    assert (
        replay_reconciled["reconciliation_artifact"]["key"]
        != reconciled["reconciliation_artifact"]["key"]
    )
