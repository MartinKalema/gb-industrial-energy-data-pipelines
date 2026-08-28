"""Focused behavior tests for bounded bundle validation and quarantine."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import date, datetime, timezone
from pathlib import Path

from ingestion.batch.pipeline.validation import (
    DATASET_FILES,
    validate_bundle,
    validate_record_collections,
)
from ingestion.batch.synthetic.generate import build_bundle, write_bundle


GENERATION_TIME = datetime(2026, 12, 31, 12, 0, tzinfo=timezone.utc)
LOCAL_DATE = date(2026, 3, 29)


def source_records() -> dict[str, list[dict]]:
    records, _ = build_bundle(
        LOCAL_DATE,
        LOCAL_DATE,
        20260828,
        GENERATION_TIME,
    )
    return records


def test_clean_synthetic_bundle_writes_accepted_files_and_report(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    output_dir = tmp_path / "validated"
    manifest = write_bundle(
        start_date=LOCAL_DATE,
        end_date=LOCAL_DATE,
        seed=20260828,
        generation_time_utc=GENERATION_TIME,
        output_dir=raw_dir,
    )

    result = validate_bundle(raw_dir, output_dir)

    assert result.is_clean
    assert result.accepted_count == manifest["total_record_count"]
    assert result.quarantined_count == 0
    assert len(result.output_files) == 2 * len(DATASET_FILES)

    report_path = output_dir / "validation_report.json"
    assert report_path.is_file()
    written_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert written_report == result.report_dict()
    assert written_report["report_schema_id"] == "bounded_bundle_validation_report"
    assert written_report["report_schema_version"] == "1.0.0"
    assert written_report["status"] == "accepted"
    assert written_report["output_files"]
    assert all(len(item["sha256"]) == 64 for item in written_report["output_files"])

    for dataset, filename in DATASET_FILES.items():
        assert (output_dir / "accepted" / filename).is_file(), dataset
        assert (output_dir / "quarantine" / filename).read_bytes() == b"", dataset


def test_exact_replay_is_skipped_idempotently() -> None:
    records = source_records()
    original_count = sum(len(rows) for rows in records.values())
    replay = deepcopy(records["customer_master"][0])
    records["customer_master"].append(replay)

    result = validate_record_collections(records)

    assert result.is_clean
    assert result.accepted_count == original_count
    assert result.quarantined_count == 0
    assert len(result.exact_replays) == 1
    assert result.exact_replays[0].payload_sha256
    assert result.exact_replays[0].accepted_line_number > 0


def test_conflicting_payloads_for_same_revision_quarantine_both() -> None:
    records = source_records()
    original_count = sum(len(rows) for rows in records.values())
    conflicting = deepcopy(records["customer_master"][0])
    conflicting["display_name"] = "Conflicting but structurally valid name"
    records["customer_master"].append(conflicting)

    result = validate_record_collections(records)

    customer_quarantine = result.quarantined["customer_master"]
    assert result.accepted_count == original_count - 1
    assert result.quarantined_count == 2
    assert len(customer_quarantine) == 2
    assert {
        issue.code
        for quarantined in customer_quarantine
        for issue in quarantined.issues
    } == {"source_revision_conflict"}
    assert len({row.source_revision_identity for row in customer_quarantine}) == 1
    assert len({row.payload_sha256 for row in customer_quarantine}) == 2


def test_broken_interval_customer_reference_is_quarantined() -> None:
    records = source_records()
    original_count = sum(len(rows) for rows in records.values())
    broken = records["commitment_schedule"][0]
    broken["customer_natural_id"] = "CUST-999"

    result = validate_record_collections(records)

    assert result.accepted_count == original_count - 1
    assert result.quarantined_count == 1
    quarantined = result.quarantined["commitment_schedule"][0]
    issue_codes = {issue.code for issue in quarantined.issues}
    assert "assignment_customer_mismatch" in issue_codes
    assert "contract_customer_mismatch" in issue_codes
    assert quarantined.payload_sha256
    assert quarantined.source_revision_identity


def test_duplicate_json_key_is_quarantined_without_canonicalizing_it_away(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "raw"
    source_dir.mkdir()
    record = source_records()["customer_master"][0]
    ambiguous = json.dumps(record, sort_keys=True).replace(
        '"source_revision": 1',
        '"source_revision": 1, "source_revision": 1',
        1,
    )
    (source_dir / "customer.jsonl").write_text(ambiguous + "\n", encoding="utf-8")

    result = validate_bundle(
        source_dir,
        tmp_path / "validated",
        dataset_files={"customer_master": "customer.jsonl"},
        require_all_synthetic_datasets=False,
    )

    quarantined = result.quarantined["customer_master"][0]
    assert result.accepted_count == 0
    assert {issue.code for issue in quarantined.issues} == {"duplicate_json_key"}
    assert quarantined.raw_text == ambiguous
    assert quarantined.record is None


def test_children_of_a_quarantined_delivery_point_assignment_are_quarantined() -> None:
    records = source_records()
    records["delivery_point_assignment"][0]["customer_natural_id"] = "CUST-999"

    result = validate_record_collections(records)

    assignment = result.quarantined["delivery_point_assignment"][0]
    assert "customer_reference_missing" in {
        issue.code for issue in assignment.issues
    }
    meter = next(
        row
        for row in result.quarantined["revenue_meter_assignment"]
        if row.record and row.record["delivery_point_natural_id"] == "DP-001"
    )
    contract_rows = [
        row
        for row in result.quarantined["contract_terms"]
        if row.record and row.record["delivery_point_natural_id"] == "DP-001"
    ]
    assert "delivery_point_reference_missing" in {
        issue.code for issue in meter.issues
    }
    assert contract_rows
    assert all(
        "delivery_point_reference_missing" in {issue.code for issue in row.issues}
        for row in contract_rows
    )
