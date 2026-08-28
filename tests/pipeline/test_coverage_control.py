"""Focused contract and replay tests for successful-run coverage publication."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from ingestion.batch.pipeline.coverage_control import (
    COVERAGE_COLUMNS,
    BatchRunCoverage,
    BatchRunCoveragePublisher,
    CoveragePayloadConflict,
    CoveragePublicationError,
    CoveragePublisherConfig,
)
from ingestion.batch.pipeline.models import RunPlan, build_run_plan
from ingestion.batch.pipeline.trino_loader import QueryResult
from ingestion.batch.pipeline.workflow import publish_batch_run_coverage


class MemoryCoverageRunner:
    """Minimal one-row identity index driven by the publisher's emitted SQL."""

    def __init__(self) -> None:
        self.payload_by_run: dict[str, str] = {}
        self.statements: list[str] = []
        self.merge_calls = 0

    def execute(self, sql: str) -> QueryResult:
        self.statements.append(sql)
        if sql.startswith(("CREATE SCHEMA", "CREATE TABLE")):
            return QueryResult()
        if sql.startswith("DESCRIBE"):
            return QueryResult(
                columns=("Column", "Type"),
                rows=tuple(
                    (column.name, column.sql_type) for column in COVERAGE_COLUMNS
                ),
            )
        if sql.startswith("SELECT coverage_payload_sha256"):
            match = re.search(r"WHERE pipeline_run_id = '([^']+)'", sql)
            assert match, "test runner could not locate the pipeline run identity"
            pipeline_run_id = match.group(1)
            if pipeline_run_id not in self.payload_by_run:
                return QueryResult(columns=("coverage_payload_sha256",))
            return QueryResult(
                columns=("coverage_payload_sha256",),
                rows=((self.payload_by_run[pipeline_run_id],),),
            )
        if sql.startswith("MERGE INTO"):
            identity = re.search(r"USING \(VALUES\s+\('([^']+)'", sql)
            hashes = re.findall(r"'([a-f0-9]{64})'", sql)
            assert identity and hashes, "test runner could not parse coverage MERGE"
            pipeline_run_id = identity.group(1)
            payload_sha256 = hashes[-1]
            self.merge_calls += 1
            inserted = 0
            if pipeline_run_id not in self.payload_by_run:
                self.payload_by_run[pipeline_run_id] = payload_sha256
                inserted = 1
            return QueryResult(update_type="MERGE", update_count=inserted)
        raise AssertionError(f"unexpected coverage SQL: {sql[:120]}")


def _plan(tmp_path: Path, orchestrator_run_id: str) -> RunPlan:
    return build_run_plan(
        start_date="2026-08-26",
        end_date="2026-08-26",
        seed=20260828,
        generation_time_utc="2026-08-28T12:00:00Z",
        orchestrator_run_id=orchestrator_run_id,
        environment={
            "R2_RAW_BUCKET": "raw-test",
            "PIPELINE_WORK_ROOT": str(tmp_path / "work"),
            "TRINO_URL": "http://trino:8080",
        },
    )


def _raw_result(plan: RunPlan, *, manifest_sha256: str = "a" * 64) -> dict[str, Any]:
    return {
        "pipeline_run_id": plan.pipeline_run_id,
        "raw_manifest": {
            "uri": f"r2://raw-test/raw/_manifests/{manifest_sha256}/manifest.json",
            "sha256": manifest_sha256,
            "last_modified_utc": "2026-08-28T12:01:00Z",
        },
    }


def _reconciliation_result(
    plan: RunPlan,
    *,
    inserted: int,
    reused: int,
    artifact_sha256: str,
) -> dict[str, Any]:
    return {
        "pipeline_run_id": plan.pipeline_run_id,
        "status": "succeeded",
        "raw_record_count": 313,
        "accepted_record_count": 313,
        "quarantined_record_count": 0,
        "duplicate_record_count": 0,
        "iceberg_inserted_count": inserted,
        "iceberg_reused_count": reused,
        "iceberg_table_count": 9,
        "reconciliation_artifact": {
            "uri": (
                "r2://raw-test/quality/reconciliation/"
                f"{plan.orchestrator_run_id}.summary.json"
            ),
            "sha256": artifact_sha256,
            "last_modified_utc": "2026-08-28T12:05:00Z",
        },
    }


def _publisher(runner: MemoryCoverageRunner) -> BatchRunCoveragePublisher:
    return BatchRunCoveragePublisher(
        CoveragePublisherConfig(trino_endpoint="http://trino:8080"),
        statement_runner=runner,
    )


def test_table_contract_is_the_accepted_r2_control_relation() -> None:
    runner = MemoryCoverageRunner()
    publisher = _publisher(runner)

    assert publisher.qualified_table == (
        '"r2"."industrial_energy_control"."batch_run_coverage"'
    )
    assert '"start_date_local_inclusive" DATE' in publisher.create_table_sql
    assert '"generated_at_utc" TIMESTAMP(6) WITH TIME ZONE' in (
        publisher.create_table_sql
    )
    assert '"iceberg_reconciled_record_count" BIGINT' in publisher.create_table_sql
    assert '"coverage_payload_sha256" VARCHAR' in publisher.create_table_sql
    assert "format_version = 2" in publisher.create_table_sql


def test_exact_replay_reuses_canonical_row_and_preserves_first_attempt(
    tmp_path: Path,
) -> None:
    first_plan = _plan(tmp_path, "manual__first-attempt")
    replay_plan = _plan(tmp_path, "manual__replay-attempt")
    first = BatchRunCoverage.from_workflow(
        first_plan,
        _raw_result(first_plan),
        _reconciliation_result(
            first_plan,
            inserted=313,
            reused=0,
            artifact_sha256="b" * 64,
        ),
    )
    replay = BatchRunCoverage.from_workflow(
        replay_plan,
        _raw_result(replay_plan),
        _reconciliation_result(
            replay_plan,
            inserted=0,
            reused=313,
            artifact_sha256="c" * 64,
        ),
    )
    runner = MemoryCoverageRunner()
    publisher = _publisher(runner)

    created = publisher.publish(first)
    reused = publisher.publish(replay)

    assert first.pipeline_run_id == replay.pipeline_run_id
    assert first.first_orchestrator_run_id != replay.first_orchestrator_run_id
    assert first.reconciliation_artifact_uri != replay.reconciliation_artifact_uri
    assert first.coverage_payload_sha256 == replay.coverage_payload_sha256
    assert created.disposition == "created"
    assert reused.disposition == "reused"
    assert runner.merge_calls == 1
    assert len(runner.payload_by_run) == 1


def test_same_run_id_with_changed_stable_manifest_identity_is_rejected(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path, "manual__conflict")
    reconciliation = _reconciliation_result(
        plan,
        inserted=313,
        reused=0,
        artifact_sha256="b" * 64,
    )
    original = BatchRunCoverage.from_workflow(
        plan, _raw_result(plan, manifest_sha256="a" * 64), reconciliation
    )
    changed = BatchRunCoverage.from_workflow(
        plan, _raw_result(plan, manifest_sha256="d" * 64), reconciliation
    )
    runner = MemoryCoverageRunner()
    publisher = _publisher(runner)
    publisher.publish(original)

    with pytest.raises(CoveragePayloadConflict, match="different canonical coverage"):
        publisher.publish(changed)

    assert runner.merge_calls == 1
    assert runner.payload_by_run[plan.pipeline_run_id] == (
        original.coverage_payload_sha256
    )


def test_workflow_api_requires_success_and_returns_only_small_summary(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path, "manual__workflow-api")
    raw = _raw_result(plan)
    reconciliation = _reconciliation_result(
        plan,
        inserted=313,
        reused=0,
        artifact_sha256="b" * 64,
    )
    runner = MemoryCoverageRunner()

    result = publish_batch_run_coverage(
        plan.to_dict(), raw, reconciliation, statement_runner=runner
    )

    assert result == {
        "pipeline_run_id": plan.pipeline_run_id,
        "table": '"r2"."industrial_energy_control"."batch_run_coverage"',
        "coverage_payload_sha256": runner.payload_by_run[plan.pipeline_run_id],
        "disposition": "created",
    }
    assert len(json.dumps(result, separators=(",", ":")).encode()) < 512
    persisted = json.loads(
        (Path(plan.work_dir) / "coverage-publication-result.json").read_text()
    )
    assert persisted == result

    with pytest.raises(CoveragePublicationError, match="only after successful"):
        BatchRunCoverage.from_workflow(
            plan,
            raw,
            {**reconciliation, "status": "failed"},
        )
