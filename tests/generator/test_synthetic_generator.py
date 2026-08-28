"""Behavioral tests for deterministic fictional source evidence."""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from ingestion.batch.synthetic.generate import (
    DATASET_FILES,
    build_bundle,
    intervals_for_local_dates,
    write_bundle,
)


GENERATION_TIME = datetime(2026, 12, 31, 12, 0, tzinfo=timezone.utc)


def bundle(
    start_date: date = date(2026, 8, 27),
    end_date: date = date(2026, 8, 27),
    seed: int = 20260828,
) -> tuple[dict[str, list[dict]], dict]:
    return build_bundle(start_date, end_date, seed, GENERATION_TIME)


@pytest.mark.parametrize(
    ("local_date", "expected_count"),
    [
        (date(2026, 3, 29), 46),
        (date(2026, 8, 27), 48),
        (date(2026, 10, 25), 50),
    ],
)
def test_london_operational_days_follow_real_dst(
    local_date: date, expected_count: int
) -> None:
    intervals = intervals_for_local_dates(local_date, local_date)
    assert len(intervals) == expected_count
    assert len({item.start_utc for item in intervals}) == expected_count
    assert all((item.end_utc - item.start_utc).total_seconds() == 1800 for item in intervals)


def test_identical_inputs_produce_byte_identical_artifacts(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    arguments = {
        "start_date": date(2026, 10, 25),
        "end_date": date(2026, 10, 25),
        "seed": 77,
        "generation_time_utc": GENERATION_TIME,
    }
    write_bundle(output_dir=first, **arguments)
    write_bundle(output_dir=second, **arguments)

    for filename in [*DATASET_FILES.values(), "manifest.json"]:
        assert (first / filename).read_bytes() == (second / filename).read_bytes()


def test_different_seed_changes_measurements_without_changing_event_keys() -> None:
    first, _ = bundle(seed=11)
    second, _ = bundle(seed=12)

    def reading_keys(rows: list[dict]) -> list[tuple[str, str, str, int]]:
        return [
            (
                row["meter_natural_id"],
                row["register_natural_id"],
                row["reading_at_utc"],
                row["source_revision"],
            )
            for row in rows
        ]

    first_readings = first["revenue_meter_reading"]
    second_readings = second["revenue_meter_reading"]
    assert reading_keys(first_readings) == reading_keys(second_readings)
    assert [row["cumulative_value"] for row in first_readings] != [
        row["cumulative_value"] for row in second_readings
    ]
    assert {dataset: len(rows) for dataset, rows in first.items()} == {
        dataset: len(rows) for dataset, rows in second.items()
    }


def test_manifest_counts_and_hashes_every_jsonl_file(tmp_path: Path) -> None:
    manifest = write_bundle(
        start_date=date(2026, 8, 27),
        end_date=date(2026, 8, 27),
        seed=42,
        generation_time_utc=GENERATION_TIME,
        output_dir=tmp_path,
    )
    assert manifest["coverage"]["utc_half_hour_interval_count"] == 48
    assert manifest["generated_at_utc"] == "2026-12-31T12:00:00Z"
    assert len(manifest["datasets"]) == 9

    for item in manifest["datasets"]:
        content = (tmp_path / item["file"]).read_bytes()
        assert hashlib.sha256(content).hexdigest() == item["sha256"]
        assert len(content.splitlines()) == item["record_count"]
        assert item["source_schema_id"] == item["dataset"]
        assert item["source_schema_version"] == "1.0.0"
        assert item["synthetic_data"] is True


def test_generated_rows_are_explicitly_synthetic_and_source_shaped() -> None:
    records, _ = bundle()
    for dataset, rows in records.items():
        assert rows
        for row in rows:
            assert row["synthetic_data"] is True
            assert row["source_schema_id"] == dataset
            assert row["source_schema_version"] == "1.0.0"
            assert "raw_object_uri" not in row
            assert "ingested_at_utc" not in row


def test_cross_source_identifiers_are_complete_and_consistent() -> None:
    records, _ = bundle()
    customers = {row["customer_natural_id"] for row in records["customer_master"]}
    sites = {row["site_natural_id"] for row in records["industrial_site_master"]}
    assignments = {
        row["delivery_point_natural_id"]: (
            row["site_natural_id"],
            row["customer_natural_id"],
        )
        for row in records["delivery_point_assignment"]
    }
    contracts = {
        (row["contract_natural_id"], row["delivery_point_natural_id"], row["customer_natural_id"])
        for row in records["contract_terms"]
    }
    meter_assignments = {
        (row["meter_natural_id"], row["register_natural_id"]): row[
            "delivery_point_natural_id"
        ]
        for row in records["revenue_meter_assignment"]
    }

    assert all(site in sites and customer in customers for site, customer in assignments.values())
    for dataset in ("commitment_schedule", "approved_excess_order"):
        for row in records[dataset]:
            point = row["delivery_point_natural_id"]
            assert assignments[point][1] == row["customer_natural_id"]
            assert (
                row["contract_natural_id"], point, row["customer_natural_id"]
            ) in contracts
    for row in records["delivery_point_capacity_assessment"]:
        assert row["delivery_point_natural_id"] in assignments
    for row in records["revenue_meter_reading"]:
        assert (row["meter_natural_id"], row["register_natural_id"]) in meter_assignments


def test_missing_commitment_is_not_fabricated_as_no_commitment() -> None:
    records, manifest = bundle()
    scenarios = {item["scenario_id"]: item for item in manifest["scenario_catalog"]}
    explicit = scenarios["explicit_no_commitment_approved_maintenance"]
    missing = scenarios["missing_commitment_is_unknown"]

    explicit_rows = [
        row
        for row in records["commitment_schedule"]
        if row["delivery_point_natural_id"] == explicit["delivery_point_natural_id"]
        and row["interval_start_utc"] == explicit["interval_start_utc"]
    ]
    assert max(explicit_rows, key=lambda row: row["source_revision"])[
        "obligation_status"
    ] == "no_commitment"
    assert max(explicit_rows, key=lambda row: row["source_revision"])[
        "committed_mwh_th"
    ] == "0.000000"

    missing_rows = [
        row
        for row in records["commitment_schedule"]
        if row["delivery_point_natural_id"] == missing["delivery_point_natural_id"]
        and row["interval_start_utc"] == missing["interval_start_utc"]
    ]
    assert missing_rows == []


def test_approved_extra_increases_billing_cap_but_not_sla_commitment() -> None:
    _, manifest = bundle()
    scenario = next(
        item
        for item in manifest["scenario_catalog"]
        if item["scenario_id"] == "approved_excess_order"
    )
    commitment = Decimal(scenario["committed_mwh_th"])
    approved_extra = Decimal(scenario["approved_extra_mwh_th"])
    delivered = Decimal(scenario["delivered_mwh_th"])

    billable = min(delivered, commitment + approved_extra)
    unbilled = max(delivered - commitment - approved_extra, Decimal("0"))
    assert billable == Decimal("5.400000")
    assert unbilled == Decimal("0.200000")
    assert min(delivered, commitment) == commitment


def test_shared_boundary_correction_recalculates_both_adjacent_intervals() -> None:
    records, manifest = bundle()
    scenario = next(
        item
        for item in manifest["scenario_catalog"]
        if item["scenario_id"] == "shared_boundary_meter_correction"
    )
    readings = records["revenue_meter_reading"]
    boundary_revisions = sorted(
        (
            row
            for row in readings
            if row["meter_natural_id"] == scenario["meter_natural_id"]
            and row["reading_at_utc"] == scenario["reading_at_utc"]
        ),
        key=lambda row: row["source_revision"],
    )
    assert [row["source_revision"] for row in boundary_revisions] == [1, 2]

    meter_rows = [
        row
        for row in readings
        if row["meter_natural_id"] == scenario["meter_natural_id"]
    ]
    event_times = sorted({row["reading_at_utc"] for row in meter_rows})
    event_index = event_times.index(scenario["reading_at_utc"])
    opening = next(
        row
        for row in meter_rows
        if row["reading_at_utc"] == event_times[event_index - 1]
    )
    closing = next(
        row
        for row in meter_rows
        if row["reading_at_utc"] == event_times[event_index + 1]
    )
    original_boundary = Decimal(boundary_revisions[0]["cumulative_value"])
    corrected_boundary = Decimal(boundary_revisions[1]["cumulative_value"])
    opening_value = Decimal(opening["cumulative_value"])
    closing_value = Decimal(closing["cumulative_value"])
    original_deliveries = (
        original_boundary - opening_value,
        closing_value - original_boundary,
    )
    corrected_deliveries = (
        corrected_boundary - opening_value,
        closing_value - corrected_boundary,
    )
    assert original_deliveries == (Decimal("4.700000"), Decimal("5.300000"))
    assert corrected_deliveries == (Decimal("4.900000"), Decimal("5.100000"))
    assert sum(original_deliveries) == sum(corrected_deliveries) == Decimal("10.000000")

    commitment = Decimal("5.000000")
    rate = Decimal("50.000000")
    penalty_rate = Decimal("100.000000")

    def metrics(deliveries: tuple[Decimal, Decimal]) -> tuple[Decimal, Decimal, Decimal]:
        credited = sum(min(value, commitment) for value in deliveries)
        shortfall = sum(max(commitment - value, Decimal("0")) for value in deliveries)
        sla = Decimal("100") * credited / (commitment * 2)
        gross = credited * rate
        net = gross - shortfall * penalty_rate
        return sla, gross, net

    assert metrics(original_deliveries) == (
        Decimal("97.00"),
        Decimal("485.000000000000"),
        Decimal("455.000000000000"),
    )
    assert metrics(corrected_deliveries) == (
        Decimal("99.00"),
        Decimal("495.000000000000"),
        Decimal("485.000000000000"),
    )


def test_capacity_states_preserve_provisional_zero_missing_and_correction() -> None:
    records, manifest = bundle()
    scenarios = {item["scenario_id"]: item for item in manifest["scenario_catalog"]}
    rows = records["delivery_point_capacity_assessment"]

    corrected = scenarios["capacity_provisional_final_correction"]
    corrected_rows = sorted(
        (
            row
            for row in rows
            if row["delivery_point_natural_id"] == corrected["delivery_point_natural_id"]
            and row["interval_start_utc"] == corrected["interval_start_utc"]
        ),
        key=lambda row: row["source_revision"],
    )
    assert [row["assessment_status"] for row in corrected_rows] == [
        "provisional",
        "final",
        "final",
    ]
    assert [row["deliverable_capacity_mwh_th"] for row in corrected_rows] == [
        "5.500000",
        "5.500000",
        "4.500000",
    ]

    provisional = scenarios["capacity_provisional_only"]
    provisional_rows = [
        row
        for row in rows
        if row["delivery_point_natural_id"] == provisional["delivery_point_natural_id"]
        and row["interval_start_utc"] == provisional["interval_start_utc"]
    ]
    assert [row["assessment_status"] for row in provisional_rows] == ["provisional"]

    final_zero = scenarios["capacity_final_zero"]
    zero_rows = [
        row
        for row in rows
        if row["delivery_point_natural_id"] == final_zero["delivery_point_natural_id"]
        and row["interval_start_utc"] == final_zero["interval_start_utc"]
    ]
    assert [row["deliverable_capacity_mwh_th"] for row in zero_rows] == ["0.000000"]

    missing = scenarios["capacity_missing_is_unknown"]
    assert not any(
        row["delivery_point_natural_id"] == missing["delivery_point_natural_id"]
        and row["interval_start_utc"] == missing["interval_start_utc"]
        for row in rows
    )


def test_existing_outputs_are_not_overwritten_without_explicit_permission(
    tmp_path: Path,
) -> None:
    arguments = {
        "start_date": date(2026, 8, 27),
        "end_date": date(2026, 8, 27),
        "seed": 1,
        "generation_time_utc": GENERATION_TIME,
        "output_dir": tmp_path,
    }
    write_bundle(**arguments)
    with pytest.raises(FileExistsError):
        write_bundle(**arguments)

    unrelated = tmp_path / "notes.txt"
    unrelated.write_text("leave me alone")
    write_bundle(overwrite=True, **arguments)
    assert unrelated.read_text() == "leave me alone"


def test_generation_time_cannot_precede_the_generated_source_evidence() -> None:
    with pytest.raises(ValueError, match="cannot precede source evidence"):
        build_bundle(
            date(2026, 8, 27),
            date(2026, 8, 27),
            1,
            datetime(2026, 8, 27, 0, 0, tzinfo=timezone.utc),
        )


def test_negative_seed_is_rejected_to_match_the_raw_envelope_contract() -> None:
    with pytest.raises(ValueError, match="seed must be a non-negative integer"):
        build_bundle(
            date(2026, 8, 27),
            date(2026, 8, 27),
            -1,
            GENERATION_TIME,
        )
