"""Executable checks for the accepted Phase 2 source-record contracts."""

from __future__ import annotations

import copy
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

from ingestion.batch.synthetic.generate import DATASET_FILES, build_bundle


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CONTRACTS_DIR = REPOSITORY_ROOT / "contracts"
GENERATION_TIME = datetime(2026, 12, 31, 12, 0, tzinfo=timezone.utc)


def load_schema(schema_name: str) -> dict:
    return json.loads((CONTRACTS_DIR / f"{schema_name}.schema.json").read_text())


COMMON_SCHEMA = load_schema("common")
SCHEMA_REGISTRY = Registry().with_resource(
    "common.schema.json", Resource.from_contents(COMMON_SCHEMA)
)


def validator_for(schema_name: str) -> Draft202012Validator:
    return Draft202012Validator(
        load_schema(schema_name),
        registry=SCHEMA_REGISTRY,
        format_checker=FormatChecker(),
    )


@pytest.fixture(scope="module")
def generated_records() -> dict[str, list[dict]]:
    records, _ = build_bundle(
        date(2026, 8, 27),
        date(2026, 8, 27),
        20260828,
        GENERATION_TIME,
    )
    return records


def test_every_schema_is_valid_draft_2020_12() -> None:
    schema_paths = sorted(CONTRACTS_DIR.glob("*.schema.json"))
    assert len(schema_paths) == 12
    for path in schema_paths:
        Draft202012Validator.check_schema(json.loads(path.read_text()))


def test_all_generated_rows_satisfy_their_contract(generated_records: dict) -> None:
    assert set(generated_records) == set(DATASET_FILES)
    for dataset, rows in generated_records.items():
        contract = validator_for(dataset)
        for row_number, row in enumerate(rows, start=1):
            errors = sorted(contract.iter_errors(row), key=lambda item: list(item.path))
            assert not errors, (
                f"{dataset} row {row_number} failed at "
                f"{errors[0].json_path}: {errors[0].message}"
            )


def test_closed_contract_rejects_an_unexpected_field(generated_records: dict) -> None:
    record = copy.deepcopy(generated_records["customer_master"][0])
    record["uncontracted_field"] = "must not pass silently"
    errors = list(validator_for("customer_master").iter_errors(record))
    assert any("Additional properties are not allowed" in error.message for error in errors)


@pytest.mark.parametrize(
    ("dataset", "mutation"),
    [
        ("commitment_schedule", {"quantity_unit": "MWh_e"}),
        ("commitment_schedule", {"committed_mwh_th": 5.0}),
        ("revenue_meter_reading", {"reading_at_utc": "2026-08-27T00:17:00Z"}),
        ("revenue_meter_reading", {"native_unit": "MWh_e"}),
        ("approved_excess_order", {"order_state": "authorized_later"}),
    ],
)
def test_invalid_units_types_times_and_codes_are_rejected(
    generated_records: dict, dataset: str, mutation: dict
) -> None:
    record = copy.deepcopy(generated_records[dataset][0])
    record.update(mutation)
    assert not validator_for(dataset).is_valid(record)


def test_correction_requires_its_revision_lineage(generated_records: dict) -> None:
    correction = next(
        row
        for row in generated_records["revenue_meter_reading"]
        if row["revision_type"] == "reconciliation"
    )
    invalid = copy.deepcopy(correction)
    invalid.pop("supersedes_source_revision")
    assert not validator_for("revenue_meter_reading").is_valid(invalid)


def test_final_capacity_requires_finalization_time(generated_records: dict) -> None:
    final_record = next(
        row
        for row in generated_records["delivery_point_capacity_assessment"]
        if row["assessment_status"] == "final"
    )
    invalid = copy.deepcopy(final_record)
    invalid.pop("finalized_at_utc")
    assert not validator_for("delivery_point_capacity_assessment").is_valid(invalid)


def test_elexon_sidecar_record_has_its_own_separate_contract() -> None:
    record = {
        "source_schema_id": "elexon_fuelhh",
        "source_schema_version": "1.0.0",
        "dataset": "FUELHH",
        "publish_time_utc": "2026-08-27T12:05:00Z",
        "start_time_utc": "2026-08-27T11:30:00Z",
        "settlement_date": "2026-08-27",
        "settlement_period": 24,
        "fuel_type": "WIND",
        "generation_mw": "8123.456000",
        "power_unit": "MW",
    }
    contract = validator_for("elexon_fuelhh")
    assert contract.is_valid(record)

    invalid = {**record, "power_unit": "MWh_th"}
    assert not contract.is_valid(invalid)


def test_raw_envelope_keeps_ingestion_lineage_outside_source_payload() -> None:
    envelope = {
        "envelope_schema_id": "raw_evidence_envelope",
        "envelope_schema_version": "1.0.0",
        "evidence_envelope_id": "RUN-001:customer-master:001",
        "source_dataset": "customer_master",
        "source_system_id": "synthetic.customer_master",
        "record_schema_id": "customer_master",
        "record_schema_version": "1.0.0",
        "ingestion_method": "synthetic_generation",
        "ingested_at_utc": "2026-08-28T12:00:00Z",
        "payload_sha256": "a" * 64,
        "raw_object_uri": "r2://industrial-energy-raw/synthetic/customer_master.jsonl",
        "raw_record_locator": "line:1",
        "content_type": "application/x-ndjson",
        "content_length_bytes": 512,
        "record_count": 1,
        "generator_run_id": "RUN-001",
        "generator_seed": 20260828,
    }
    contract = validator_for("raw_evidence_envelope")
    assert contract.is_valid(envelope)

    mismatched = {**envelope, "record_schema_id": "industrial_site_master"}
    assert not contract.is_valid(mismatched)
