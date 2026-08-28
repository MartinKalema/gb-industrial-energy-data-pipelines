#!/usr/bin/env python3
"""Generate deterministic synthetic source records for the Phase 2 lakehouse slice.

The generator intentionally uses only the Python standard library.  It emits
source-shaped JSONL evidence, not warehouse facts or already-resolved current
views.  Every immutable source revision is retained in the output.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import tempfile
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo


GENERATOR_NAME = "industrial-energy-lakehouse-synthetic-source-generator"
GENERATOR_VERSION = "1.1.0"
SCHEMA_VERSION = "1.0.0"
LONDON = ZoneInfo("Europe/London")
UTC = timezone.utc
HALF_HOUR = timedelta(minutes=30)
SIX_PLACES = Decimal("0.000001")
DECIMAL_PATTERN = re.compile(r"^-?\d+\.\d{6}$")

# The synthetic source represents one continuous fictional timeline.  These
# anchors deliberately match the original reference evidence, so fixing range
# composition does not change that known-good source bundle.
SYNTHETIC_TIMELINE_START_LOCAL_DATE = date(2026, 8, 26)
SYNTHETIC_TIMELINE_START_UTC = datetime(2026, 8, 25, 23, 0, tzinfo=UTC)
SYNTHETIC_HISTORY_BOUNDARY_UTC = datetime(2026, 8, 26, 11, 0, tzinfo=UTC)
SYNTHETIC_PROJECT_SEED = 20260828

DATASET_FILES = {
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

SOURCE_SCHEMA_IDS = {dataset: dataset for dataset in DATASET_FILES}

DATASET_GRAINS = {
    "customer_master": "one published revision of one effective customer version",
    "industrial_site_master": "one published revision of one effective industrial-site version",
    "delivery_point_assignment": "one published revision of one effective delivery-point assignment",
    "revenue_meter_assignment": "one published revision of one effective revenue-meter assignment",
    "contract_terms": "one published revision of one effective commercial-terms episode",
    "commitment_schedule": "one published revision of one delivery-point half-hour commitment",
    "approved_excess_order": "one published revision of one approved excess-order interval allocation",
    "revenue_meter_reading": "one published revision of one cumulative revenue-meter boundary reading",
    "delivery_point_capacity_assessment": "one published revision of one delivery-point half-hour capacity assessment",
}

DECIMAL_FIELDS = {
    "maximum_plausible_30_min_change",
    "energy_rate_gbp_per_mwh_th",
    "sla_penalty_rate_gbp_per_mwh_th",
    "committed_mwh_th",
    "approved_extra_mwh_th",
    "cumulative_value",
    "nameplate_ceiling_mwh_th",
    "operational_restriction_mwh_th",
    "deliverable_capacity_mwh_th",
}


@dataclass(frozen=True)
class Interval:
    """One real UTC half-hour attributed to a Europe/London local date."""

    index: int
    start_utc: datetime
    end_utc: datetime
    local_date: date
    local_period_number: int


@dataclass(frozen=True)
class Entity:
    customer_id: str
    site_id: str
    delivery_point_id: str
    meter_id: str
    register_id: str
    contract_id: str
    base_commitment: Decimal
    nameplate_ceiling: Decimal
    maximum_meter_delta: Decimal


ENTITIES = (
    Entity(
        customer_id="CUST-001",
        site_id="SITE-001",
        delivery_point_id="DP-001",
        meter_id="RM-001",
        register_id="ENERGY-01",
        contract_id="CONTRACT-001",
        base_commitment=Decimal("5.000000"),
        nameplate_ceiling=Decimal("7.000000"),
        maximum_meter_delta=Decimal("8.000000"),
    ),
    Entity(
        customer_id="CUST-002",
        site_id="SITE-002",
        delivery_point_id="DP-002",
        meter_id="RM-002",
        register_id="ENERGY-01",
        contract_id="CONTRACT-002",
        base_commitment=Decimal("4.000000"),
        nameplate_ceiling=Decimal("6.000000"),
        maximum_meter_delta=Decimal("7.000000"),
    ),
)


def decimal_text(value: Decimal | str | int) -> str:
    """Return an exact, non-scientific decimal string with six fractional digits."""

    return format(Decimal(value).quantize(SIX_PLACES, rounding=ROUND_HALF_UP), "f")


def utc_text(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def time_token(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def stable_int(seed: int, label: str, modulo: int) -> int:
    digest = hashlib.sha256(f"{seed}:{label}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % modulo


def synthetic_timeline_index(value: datetime) -> int:
    """Return the stable half-hour position of a UTC boundary in the timeline."""

    if value.tzinfo is None:
        raise ValueError("timeline timestamps must be timezone-aware")
    difference = value.astimezone(UTC) - SYNTHETIC_TIMELINE_START_UTC
    half_hours, remainder = divmod(int(difference.total_seconds()), 1800)
    if remainder:
        raise ValueError("timeline timestamps must fall on a UTC half-hour boundary")
    return half_hours


def synthetic_interval_at(start_utc: datetime) -> Interval:
    """Build the range-independent interval metadata for one UTC half-hour."""

    start_utc = start_utc.astimezone(UTC)
    local_date = start_utc.astimezone(LONDON).date()
    local_midnight_utc = datetime.combine(local_date, time.min, tzinfo=LONDON).astimezone(
        UTC
    )
    local_period_number = synthetic_timeline_index(start_utc) - synthetic_timeline_index(
        local_midnight_utc
    ) + 1
    return Interval(
        index=synthetic_timeline_index(start_utc),
        start_utc=start_utc,
        end_utc=start_utc + HALF_HOUR,
        local_date=local_date,
        local_period_number=local_period_number,
    )


def intervals_for_local_dates(start_date: date, end_date: date) -> list[Interval]:
    """Expand inclusive London dates into real, unambiguous UTC half-hours."""

    if end_date < start_date:
        raise ValueError("end date must be on or after start date")

    start_local = datetime.combine(start_date, time.min, tzinfo=LONDON)
    end_local_exclusive = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=LONDON)
    cursor = start_local.astimezone(UTC)
    end_utc = end_local_exclusive.astimezone(UTC)
    local_periods: Counter[date] = Counter()
    result: list[Interval] = []

    while cursor < end_utc:
        local_day = cursor.astimezone(LONDON).date()
        local_periods[local_day] += 1
        result.append(
            Interval(
                index=len(result),
                start_utc=cursor,
                end_utc=cursor + HALF_HOUR,
                local_date=local_day,
                local_period_number=local_periods[local_day],
            )
        )
        cursor += HALF_HOUR

    if not result:
        raise ValueError("date range must contain at least one interval")
    return result


def revision_metadata(
    *,
    dataset: str,
    immutable_id: str,
    source_revision: int,
    revision_type: str,
    published_at: datetime,
    approved_at: datetime | None = None,
    supersedes_source_revision: int | None = None,
    correction_reason_code: str | None = None,
    correction_at: datetime | None = None,
    approval_reference: str | None = None,
) -> dict[str, Any]:
    if source_revision < 1:
        raise ValueError("source_revision must be positive")
    approved_at = approved_at or published_at - timedelta(minutes=5)
    metadata: dict[str, Any] = {
        "synthetic_data": True,
        "source_system_id": f"synthetic.{dataset}",
        "source_schema_id": SOURCE_SCHEMA_IDS[dataset],
        "source_schema_version": SCHEMA_VERSION,
        "source_revision": source_revision,
        "revision_type": revision_type,
        "published_at_utc": utc_text(published_at),
        "approval_state": "approved",
        "approved_at_utc": utc_text(approved_at),
        "approved_by": "SYNTHETIC-AUTO-APPROVER",
    }
    if supersedes_source_revision is not None:
        metadata["supersedes_source_revision"] = supersedes_source_revision
    if correction_reason_code is not None:
        if correction_at is None:
            raise ValueError("a correction reason requires a correction timestamp")
        if dataset == "contract_terms":
            metadata["changed_at_utc"] = utc_text(correction_at)
            metadata["change_reason_code"] = correction_reason_code
        else:
            metadata["corrected_at_utc"] = utc_text(correction_at)
            metadata["correction_reason_code"] = correction_reason_code
    if dataset == "commitment_schedule" and approval_reference is not None:
        metadata["approval_reference"] = approval_reference
    return metadata


def source_revision_id(prefix: str, logical_parts: Iterable[str], revision: int) -> str:
    return f"{prefix}-{'-'.join(logical_parts)}-R{revision:03d}"


def customer_records(window_start: datetime, history_boundary: datetime) -> list[dict[str, Any]]:
    effective_start = window_start - timedelta(days=365)
    v1_publication = window_start - timedelta(days=400)
    correction_publication = window_start - timedelta(days=20)
    v2_publication = history_boundary - timedelta(days=2)

    def record(
        *,
        customer_id: str,
        version_id: str,
        revision: int,
        legal_name: str,
        display_name: str,
        industry: str,
        effective_from: datetime,
        effective_to: datetime | None,
        revision_type: str,
        published_at: datetime,
        supersedes: int | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        immutable_id = source_revision_id("CUSTREV", (version_id,), revision)
        return {
            "customer_natural_id": customer_id,
            "customer_version_id": version_id,
            "legal_name": legal_name,
            "display_name": display_name,
            "industry_sector_code": industry,
            "country_code": "GB",
            "lifecycle_status": "active",
            "tenant_authorization_scope_id": f"TENANT-{customer_id}",
            "effective_from_utc": utc_text(effective_from),
            "effective_to_utc": utc_text(effective_to),
            "version_record_status": "active",
            **revision_metadata(
                dataset="customer_master",
                immutable_id=immutable_id,
                source_revision=revision,
                revision_type=revision_type,
                published_at=published_at,
                supersedes_source_revision=supersedes,
                correction_reason_code=reason,
                correction_at=published_at - timedelta(minutes=15) if reason else None,
                approval_reference=f"SYN-APP-{immutable_id}",
            ),
        }

    return [
        record(
            customer_id="CUST-001",
            version_id="CUST-001-V1",
            revision=1,
            legal_name="Northstar Advanced Ceramcis Ltd",
            display_name="Northstar Ceramics",
            industry="ceramics",
            effective_from=effective_start,
            effective_to=history_boundary,
            revision_type="original",
            published_at=v1_publication,
        ),
        record(
            customer_id="CUST-001",
            version_id="CUST-001-V1",
            revision=2,
            legal_name="Northstar Advanced Ceramics Ltd",
            display_name="Northstar Ceramics",
            industry="ceramics",
            effective_from=effective_start,
            effective_to=history_boundary,
            revision_type="correction",
            published_at=correction_publication,
            supersedes=1,
            reason="descriptive_data_correction",
        ),
        record(
            customer_id="CUST-001",
            version_id="CUST-001-V2",
            revision=1,
            legal_name="Northstar Thermal Ceramics Ltd",
            display_name="Northstar Thermal Ceramics",
            industry="ceramics",
            effective_from=history_boundary,
            effective_to=None,
            revision_type="business_change",
            published_at=v2_publication,
        ),
        record(
            customer_id="CUST-002",
            version_id="CUST-002-V1",
            revision=1,
            legal_name="Riverside Sustainable Foods Ltd",
            display_name="Riverside Foods",
            industry="food_and_beverage",
            effective_from=effective_start,
            effective_to=None,
            revision_type="original",
            published_at=v1_publication,
        ),
    ]


def site_records(window_start: datetime, history_boundary: datetime) -> list[dict[str, Any]]:
    effective_start = window_start - timedelta(days=365)
    v1_publication = window_start - timedelta(days=400)
    correction_publication = window_start - timedelta(days=18)
    v2_publication = history_boundary - timedelta(days=2)

    def record(
        *,
        site_id: str,
        version_id: str,
        revision: int,
        site_name: str,
        locality: str,
        postal_area: str,
        region: str,
        latitude: str,
        longitude: str,
        effective_from: datetime,
        effective_to: datetime | None,
        revision_type: str,
        published_at: datetime,
        supersedes: int | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        immutable_id = source_revision_id("SITEREV", (version_id,), revision)
        return {
            "site_natural_id": site_id,
            "site_version_id": version_id,
            "site_name": site_name,
            "locality": locality,
            "postal_area": postal_area,
            "country_code": "GB",
            "region_code": region,
            "iana_timezone": "Europe/London",
            "operational_status": "operational",
            "latitude": float(Decimal(latitude)),
            "longitude": float(Decimal(longitude)),
            "effective_from_utc": utc_text(effective_from),
            "effective_to_utc": utc_text(effective_to),
            "version_record_status": "active",
            **revision_metadata(
                dataset="industrial_site_master",
                immutable_id=immutable_id,
                source_revision=revision,
                revision_type=revision_type,
                published_at=published_at,
                supersedes_source_revision=supersedes,
                correction_reason_code=reason,
                correction_at=published_at - timedelta(minutes=15) if reason else None,
                approval_reference=f"SYN-APP-{immutable_id}",
            ),
        }

    return [
        record(
            site_id="SITE-001",
            version_id="SITE-001-V1",
            revision=1,
            site_name="Northstar Sheffield Works",
            locality="Sheffeld",
            postal_area="S9",
            region="GB-ENG",
            latitude="53.385000",
            longitude="-1.420000",
            effective_from=effective_start,
            effective_to=history_boundary,
            revision_type="original",
            published_at=v1_publication,
        ),
        record(
            site_id="SITE-001",
            version_id="SITE-001-V1",
            revision=2,
            site_name="Northstar Sheffield Works",
            locality="Sheffield",
            postal_area="S9",
            region="GB-ENG",
            latitude="53.385000",
            longitude="-1.420000",
            effective_from=effective_start,
            effective_to=history_boundary,
            revision_type="correction",
            published_at=correction_publication,
            supersedes=1,
            reason="descriptive_data_correction",
        ),
        record(
            site_id="SITE-001",
            version_id="SITE-001-V2",
            revision=1,
            site_name="Northstar Sheffield Thermal Works",
            locality="Sheffield",
            postal_area="S9",
            region="GB-ENG",
            latitude="53.385000",
            longitude="-1.420000",
            effective_from=history_boundary,
            effective_to=None,
            revision_type="business_change",
            published_at=v2_publication,
        ),
        record(
            site_id="SITE-002",
            version_id="SITE-002-V1",
            revision=1,
            site_name="Riverside Hull Food Works",
            locality="Hull",
            postal_area="HU3",
            region="GB-ENG",
            latitude="53.745000",
            longitude="-0.355000",
            effective_from=effective_start,
            effective_to=None,
            revision_type="original",
            published_at=v1_publication,
        ),
    ]


def delivery_point_assignment_records(window_start: datetime) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for number, entity in enumerate(ENTITIES, start=1):
        assignment_id = f"DPA-{number:03d}"
        immutable_id = source_revision_id("DPAREV", (assignment_id,), 1)
        records.append(
            {
                "delivery_point_assignment_id": assignment_id,
                "delivery_point_natural_id": entity.delivery_point_id,
                "delivery_point_name": f"Synthetic Steam Delivery Point {number}",
                "site_natural_id": entity.site_id,
                "customer_natural_id": entity.customer_id,
                "service_type": "industrial_steam",
                "effective_from_utc": utc_text(window_start - timedelta(days=365)),
                "effective_to_utc": None,
                "assignment_status": "active",
                **revision_metadata(
                    dataset="delivery_point_assignment",
                    immutable_id=immutable_id,
                    source_revision=1,
                    revision_type="original",
                    published_at=window_start - timedelta(days=400),
                    approval_reference=f"SYN-APP-{immutable_id}",
                ),
            }
        )
    return records


def revenue_meter_assignment_records(window_start: datetime) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for number, entity in enumerate(ENTITIES, start=1):
        assignment_id = f"MA-{number:03d}"
        immutable_id = source_revision_id("MAREV", (assignment_id,), 1)
        records.append(
            {
                "meter_assignment_id": assignment_id,
                "meter_natural_id": entity.meter_id,
                "register_natural_id": entity.register_id,
                "delivery_point_natural_id": entity.delivery_point_id,
                "assignment_role": "authoritative_revenue",
                "register_type": "thermal_energy",
                "native_unit": "MWh_th",
                "calibration_id": f"CAL-{entity.meter_id}-001",
                "maximum_plausible_30_min_change": decimal_text(entity.maximum_meter_delta),
                "effective_from_utc": utc_text(window_start - timedelta(days=365)),
                "effective_to_utc": None,
                "assignment_status": "active",
                **revision_metadata(
                    dataset="revenue_meter_assignment",
                    immutable_id=immutable_id,
                    source_revision=1,
                    revision_type="original",
                    published_at=window_start - timedelta(days=400),
                    approval_reference=f"SYN-APP-{immutable_id}",
                ),
            }
        )
    return records


def contract_records(window_start: datetime, history_boundary: datetime) -> list[dict[str, Any]]:
    effective_start = window_start - timedelta(days=365)

    def record(
        *,
        entity: Entity,
        version_id: str,
        revision: int,
        effective_from: datetime,
        effective_to: datetime | None,
        energy_rate: str,
        penalty_rate: str,
        revision_type: str,
        published_at: datetime,
        supersedes: int | None = None,
        reason: str | None = None,
        agreement_id: str | None = None,
    ) -> dict[str, Any]:
        immutable_id = source_revision_id("TERMSREV", (version_id,), revision)
        return {
            "contract_natural_id": entity.contract_id,
            "contract_terms_version_id": version_id,
            "delivery_point_natural_id": entity.delivery_point_id,
            "customer_natural_id": entity.customer_id,
            "effective_from_utc": utc_text(effective_from),
            "effective_to_utc": utc_text(effective_to),
            "energy_rate_gbp_per_mwh_th": decimal_text(energy_rate),
            "sla_penalty_rate_gbp_per_mwh_th": decimal_text(penalty_rate),
            "currency_code": "GBP",
            "rate_unit": "GBP/MWh_th",
            "terms_status": "active",
            **({"agreement_reference": agreement_id} if agreement_id else {}),
            **revision_metadata(
                dataset="contract_terms",
                immutable_id=immutable_id,
                source_revision=revision,
                revision_type=revision_type,
                published_at=published_at,
                supersedes_source_revision=supersedes,
                correction_reason_code=reason,
                correction_at=published_at - timedelta(minutes=30) if reason else None,
                approval_reference=f"SYN-APP-{immutable_id}",
            ),
        }

    first, second = ENTITIES
    return [
        record(
            entity=first,
            version_id="CONTRACT-001-T1",
            revision=1,
            effective_from=effective_start,
            effective_to=history_boundary,
            energy_rate="49.500000",
            penalty_rate="100.000000",
            revision_type="original",
            published_at=window_start - timedelta(days=400),
        ),
        record(
            entity=first,
            version_id="CONTRACT-001-T1",
            revision=2,
            effective_from=effective_start,
            effective_to=history_boundary,
            energy_rate="50.000000",
            penalty_rate="100.000000",
            revision_type="correction",
            published_at=window_start - timedelta(days=10),
            supersedes=1,
            reason="source_error",
            agreement_id="SYN-CORRECTION-001",
        ),
        record(
            entity=first,
            version_id="CONTRACT-001-T2",
            revision=1,
            effective_from=history_boundary,
            effective_to=None,
            energy_rate="57.500000",
            penalty_rate="125.000000",
            revision_type="amendment",
            published_at=history_boundary - timedelta(days=2),
            agreement_id="SYN-AMENDMENT-001",
        ),
        record(
            entity=second,
            version_id="CONTRACT-002-T1",
            revision=1,
            effective_from=effective_start,
            effective_to=None,
            energy_rate="52.000000",
            penalty_rate="110.000000",
            revision_type="original",
            published_at=window_start - timedelta(days=400),
        ),
    ]


def delivery_quantity(entity: Entity, interval: Interval, seed: int) -> Decimal:
    """Return a delivery determined only by entity, event time, and source seed."""

    timeline_index = synthetic_timeline_index(interval.start_utc)
    jitter = Decimal(
        stable_int(seed, f"{entity.delivery_point_id}:{timeline_index}", 5)
    ) / Decimal("100")
    quantity = entity.base_commitment + jitter

    if entity.delivery_point_id == "DP-001":
        scenario_quantities = {
            3: Decimal("4.700000"),  # explicit shortfall
            4: Decimal("5.600000"),  # approved extra, plus unbilled excess
            5: Decimal("5.100000"),  # second line of a multi-interval order
            6: Decimal("0.000000"),  # approved-maintenance no commitment
            7: Decimal("4.900000"),  # commitment record deliberately absent
            9: Decimal("4.900000"),  # left side of shared-boundary correction
            10: Decimal("5.100000"),  # right side of shared-boundary correction
            11: Decimal("5.200000"),  # retroactive commitment correction
        }
        quantity = scenario_quantities.get(interval.local_period_number, quantity)
    return quantity


def delivery_quantities(intervals: list[Interval], seed: int) -> dict[str, list[Decimal]]:
    return {
        entity.delivery_point_id: [
            delivery_quantity(entity, interval, seed) for interval in intervals
        ]
        for entity in ENTITIES
    }


def commitment_records(intervals: list[Interval]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    def make_record(
        entity: Entity,
        interval: Interval,
        revision: int,
        status: str,
        quantity: Decimal,
        revision_type: str,
        published_at: datetime,
        *,
        supersedes: int | None = None,
        reason: str | None = None,
        approval_reference: str | None = None,
    ) -> dict[str, Any]:
        token = time_token(interval.start_utc)
        immutable_id = source_revision_id(
            "COMMREV", (entity.delivery_point_id, token), revision
        )
        return {
            "source_commitment_revision_id": immutable_id,
            "delivery_point_natural_id": entity.delivery_point_id,
            "customer_natural_id": entity.customer_id,
            "contract_natural_id": entity.contract_id,
            "interval_start_utc": utc_text(interval.start_utc),
            "interval_end_utc": utc_text(interval.end_utc),
            "obligation_status": status,
            "committed_mwh_th": decimal_text(quantity),
            "quantity_unit": "MWh_th",
            "commitment_reason_code": (
                "normal_operation" if status == "committed" else "approved_maintenance"
            ),
            "schedule_record_status": "active",
            **revision_metadata(
                dataset="commitment_schedule",
                immutable_id=immutable_id,
                source_revision=revision,
                revision_type=revision_type,
                published_at=published_at,
                supersedes_source_revision=supersedes,
                correction_reason_code=reason,
                correction_at=published_at - timedelta(minutes=15) if reason else None,
                approval_reference=approval_reference or f"SYN-APP-{immutable_id}",
            ),
        }

    for entity in ENTITIES:
        for interval in intervals:
            if (
                entity.delivery_point_id == "DP-001"
                and interval.local_period_number == 7
            ):
                continue  # absence is intentionally unknown, not zero

            initial_publication = interval.start_utc - timedelta(days=2)
            if (
                entity.delivery_point_id == "DP-001"
                and interval.local_period_number == 6
            ):
                records.append(
                    make_record(
                        entity,
                        interval,
                        1,
                        "committed",
                        entity.base_commitment,
                        "original",
                        initial_publication,
                    )
                )
                records.append(
                    make_record(
                        entity,
                        interval,
                        2,
                        "no_commitment",
                        Decimal("0"),
                        "change",
                        interval.start_utc - timedelta(days=1),
                        supersedes=1,
                        approval_reference="SYN-MAINT-001",
                    )
                )
                continue

            records.append(
                make_record(
                    entity,
                    interval,
                    1,
                    "committed",
                    entity.base_commitment,
                    "original",
                    initial_publication,
                )
            )
            if (
                entity.delivery_point_id == "DP-001"
                and interval.local_period_number == 11
            ):
                records.append(
                    make_record(
                        entity,
                        interval,
                        2,
                        "committed",
                        Decimal("5.500000"),
                        "retroactive_change",
                        interval.end_utc + timedelta(hours=6),
                        supersedes=1,
                        reason="approved_retroactive_change",
                        approval_reference="SYN-RETRO-COMMIT-001",
                    )
                )
    return records


def excess_order_records(intervals: list[Interval]) -> list[dict[str, Any]]:
    first = ENTITIES[0]
    records: list[dict[str, Any]] = []

    def order_id(sequence: int, local_date: date) -> str:
        base = f"EXCESS-ORDER-{sequence:03d}"
        if local_date == SYNTHETIC_TIMELINE_START_LOCAL_DATE:
            return base
        return f"{base}-{local_date:%Y%m%d}"

    def make_record(
        *,
        order_id: str,
        line_id: str,
        interval: Interval,
        revision: int,
        order_state: str,
        quantity: Decimal,
        revision_type: str,
        published_at: datetime,
        supersedes: int | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        immutable_id = source_revision_id("ORDERREV", (line_id,), revision)
        return {
            "excess_order_natural_id": order_id,
            "order_interval_line_id": line_id,
            "delivery_point_natural_id": first.delivery_point_id,
            "customer_natural_id": first.customer_id,
            "contract_natural_id": first.contract_id,
            "interval_start_utc": utc_text(interval.start_utc),
            "interval_end_utc": utc_text(interval.end_utc),
            "approved_extra_mwh_th": decimal_text(quantity),
            "quantity_unit": "MWh_th",
            "order_state": order_state,
            "requested_at_utc": utc_text(interval.start_utc - timedelta(days=3)),
            **revision_metadata(
                dataset="approved_excess_order",
                immutable_id=immutable_id,
                source_revision=revision,
                revision_type=revision_type,
                published_at=published_at,
                approved_at=published_at - timedelta(hours=1),
                supersedes_source_revision=supersedes,
                correction_reason_code=reason,
                correction_at=published_at - timedelta(minutes=15) if reason else None,
                approval_reference=f"SYN-APP-{immutable_id}",
            ),
        }

    intervals_by_date: dict[date, dict[int, Interval]] = {}
    for interval in intervals:
        intervals_by_date.setdefault(interval.local_date, {})[
            interval.local_period_number
        ] = interval

    for local_date, local_intervals in sorted(intervals_by_date.items()):
        approved_order_id = order_id(1, local_date)
        for line_number, (period_number, quantity) in enumerate(
            ((4, Decimal("0.400000")), (5, Decimal("0.200000"))), start=1
        ):
            interval = local_intervals[period_number]
            records.append(
                make_record(
                    order_id=approved_order_id,
                    line_id=f"{approved_order_id}-L{line_number:02d}",
                    interval=interval,
                    revision=1,
                    order_state="approved",
                    quantity=quantity,
                    revision_type="original",
                    published_at=interval.start_utc - timedelta(days=1),
                )
            )

        canceled_order_id = order_id(2, local_date)
        canceled_interval = local_intervals[13]
        records.append(
            make_record(
                order_id=canceled_order_id,
                line_id=f"{canceled_order_id}-L01",
                interval=canceled_interval,
                revision=1,
                order_state="approved",
                quantity=Decimal("0.300000"),
                revision_type="original",
                published_at=canceled_interval.start_utc - timedelta(days=2),
            )
        )
        records.append(
            make_record(
                order_id=canceled_order_id,
                line_id=f"{canceled_order_id}-L01",
                interval=canceled_interval,
                revision=2,
                order_state="cancelled",
                quantity=Decimal("0"),
                revision_type="cancellation",
                published_at=canceled_interval.start_utc - timedelta(days=1),
                supersedes=1,
                reason="customer_cancellation",
            )
        )
    return records


def meter_value_at(
    boundary_utc: datetime, entity_number: int, entity: Entity, seed: int
) -> Decimal:
    """Return the continuous cumulative register value at one UTC boundary."""

    boundary_utc = boundary_utc.astimezone(UTC)
    if boundary_utc < SYNTHETIC_TIMELINE_START_UTC:
        raise ValueError(
            "cumulative meter evidence is unavailable before the synthetic timeline starts"
        )
    value = (
        Decimal("2000.000000") if entity_number == 1 else Decimal("3500.000000")
    ) + Decimal(stable_int(seed, f"start:{entity.meter_id}", 50))
    cursor = SYNTHETIC_TIMELINE_START_UTC
    while cursor < boundary_utc:
        value += delivery_quantity(entity, synthetic_interval_at(cursor), seed)
        cursor += HALF_HOUR
    return value


def meter_reading_records(
    intervals: list[Interval], deliveries: dict[str, list[Decimal]], seed: int
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    for entity_number, entity in enumerate(ENTITIES, start=1):
        starting_value = meter_value_at(
            intervals[0].start_utc, entity_number, entity, seed
        )
        cumulative_values = [starting_value]
        for quantity in deliveries[entity.delivery_point_id]:
            cumulative_values.append(cumulative_values[-1] + quantity)

        boundaries = [intervals[0].start_utc] + [item.end_utc for item in intervals]
        for boundary, current_value in zip(boundaries, cumulative_values, strict=True):
            revisions = [(1, current_value, "initial")]
            preceding_interval = synthetic_interval_at(boundary - HALF_HOUR)
            if (
                entity.delivery_point_id == "DP-001"
                and preceding_interval.local_period_number == 9
            ):
                revisions = [
                    (1, current_value - Decimal("0.200000"), "initial"),
                    (2, current_value, "correction"),
                ]

            for revision, value, revision_type in revisions:
                token = time_token(boundary)
                immutable_id = source_revision_id(
                    "READREV", (entity.meter_id, entity.register_id, token), revision
                )
                published_at = boundary + timedelta(minutes=5)
                supersedes = None
                reason = None
                correction_at = None
                if revision == 2:
                    published_at = boundary + timedelta(days=1, hours=6)
                    supersedes = 1
                    reason = "APPROVED_METER_RECONCILIATION"
                    correction_at = published_at - timedelta(minutes=30)

                records.append(
                    {
                        "source_reading_revision_id": immutable_id,
                        "meter_natural_id": entity.meter_id,
                        "register_natural_id": entity.register_id,
                        "reading_at_utc": utc_text(boundary),
                        "cumulative_value": decimal_text(value),
                        "native_unit": "MWh_th",
                        "register_type": "thermal_energy",
                        "reading_method": "actual",
                        "reading_status": "active",
                        **revision_metadata(
                            dataset="revenue_meter_reading",
                            immutable_id=immutable_id,
                            source_revision=revision,
                            revision_type=(
                                "original" if revision_type == "initial" else "reconciliation"
                            ),
                            published_at=published_at,
                            approved_at=published_at - timedelta(minutes=5),
                            supersedes_source_revision=supersedes,
                            correction_reason_code=(
                                "meter_data_reconciliation" if reason else None
                            ),
                            correction_at=correction_at,
                            approval_reference=f"SYN-APP-{immutable_id}",
                        ),
                    }
                )
    return records


def capacity_records(intervals: list[Interval]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    def make_record(
        *,
        entity: Entity,
        interval: Interval,
        revision: int,
        assessment_status: str,
        restriction: Decimal,
        reason_code: str,
        revision_type: str,
        published_at: datetime,
        supersedes: int | None = None,
        correction_reason: str | None = None,
    ) -> dict[str, Any]:
        token = time_token(interval.start_utc)
        immutable_id = source_revision_id(
            "CAPREV", (entity.delivery_point_id, token), revision
        )
        capacity = max(entity.nameplate_ceiling - restriction, Decimal("0"))
        return {
            "source_capacity_revision_id": immutable_id,
            "delivery_point_natural_id": entity.delivery_point_id,
            "interval_start_utc": utc_text(interval.start_utc),
            "interval_end_utc": utc_text(interval.end_utc),
            "assessment_status": assessment_status,
            "nameplate_ceiling_mwh_th": decimal_text(entity.nameplate_ceiling),
            "operational_restriction_mwh_th": decimal_text(restriction),
            "deliverable_capacity_mwh_th": decimal_text(capacity),
            "quantity_unit": "MWh_th",
            "assessment_method": "nameplate_minus_operational_restriction_floor_zero",
            "assessment_method_version": "1.0.0",
            "capacity_reason_code": reason_code,
            **(
                {"finalized_at_utc": utc_text(published_at)}
                if assessment_status == "final"
                else {}
            ),
            **revision_metadata(
                dataset="delivery_point_capacity_assessment",
                immutable_id=immutable_id,
                source_revision=revision,
                revision_type=revision_type,
                published_at=published_at,
                approved_at=published_at - timedelta(minutes=15),
                supersedes_source_revision=supersedes,
                correction_reason_code=correction_reason,
                correction_at=published_at - timedelta(minutes=30) if correction_reason else None,
                approval_reference=f"SYN-APP-{immutable_id}",
            ),
        }

    for entity in ENTITIES:
        for interval in intervals:
            if (
                entity.delivery_point_id == "DP-001"
                and interval.local_period_number == 16
            ):
                continue  # no assessment: deliberately unknown rather than zero

            if (
                entity.delivery_point_id == "DP-001"
                and interval.local_period_number == 13
            ):
                records.extend(
                    [
                        make_record(
                            entity=entity,
                            interval=interval,
                            revision=1,
                            assessment_status="provisional",
                            restriction=Decimal("1.500000"),
                            reason_code="derated",
                            revision_type="original",
                            published_at=interval.start_utc - timedelta(hours=1),
                        ),
                        make_record(
                            entity=entity,
                            interval=interval,
                            revision=2,
                            assessment_status="final",
                            restriction=Decimal("1.500000"),
                            reason_code="derated",
                            revision_type="finalization",
                            published_at=interval.end_utc + timedelta(hours=2),
                            supersedes=1,
                        ),
                        make_record(
                            entity=entity,
                            interval=interval,
                            revision=3,
                            assessment_status="final",
                            restriction=Decimal("2.500000"),
                            reason_code="derated",
                            revision_type="correction",
                            published_at=interval.end_utc + timedelta(days=1),
                            supersedes=2,
                            correction_reason="assessment_reconciliation",
                        ),
                    ]
                )
                continue

            if (
                entity.delivery_point_id == "DP-001"
                and interval.local_period_number == 14
            ):
                records.append(
                    make_record(
                        entity=entity,
                        interval=interval,
                        revision=1,
                        assessment_status="provisional",
                        restriction=Decimal("1.000000"),
                        reason_code="derated",
                        revision_type="original",
                        published_at=interval.start_utc - timedelta(hours=1),
                    )
                )
                continue

            restriction = Decimal("0.500000")
            reason_code = "normal"
            if (
                entity.delivery_point_id == "DP-001"
                and interval.local_period_number == 15
            ):
                restriction = entity.nameplate_ceiling
                reason_code = "unavailable"
            elif (
                entity.delivery_point_id == "DP-001"
                and interval.local_period_number == 17
            ):
                restriction = Decimal("3.000000")
                reason_code = "planned_restriction"

            records.append(
                make_record(
                    entity=entity,
                    interval=interval,
                    revision=1,
                    assessment_status="final",
                    restriction=restriction,
                    reason_code=reason_code,
                    revision_type="original",
                    published_at=interval.end_utc + timedelta(hours=2),
                )
            )
    return records


def sort_records(dataset: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = {
        "customer_master": lambda r: (
            r["customer_natural_id"], r["effective_from_utc"], r["customer_version_id"], r["source_revision"]
        ),
        "industrial_site_master": lambda r: (
            r["site_natural_id"], r["effective_from_utc"], r["site_version_id"], r["source_revision"]
        ),
        "delivery_point_assignment": lambda r: (
            r["delivery_point_assignment_id"], r["source_revision"]
        ),
        "revenue_meter_assignment": lambda r: (
            r["meter_assignment_id"], r["source_revision"]
        ),
        "contract_terms": lambda r: (
            r["contract_natural_id"], r["effective_from_utc"], r["contract_terms_version_id"], r["source_revision"]
        ),
        "commitment_schedule": lambda r: (
            r["delivery_point_natural_id"], r["interval_start_utc"], r["source_revision"]
        ),
        "approved_excess_order": lambda r: (
            r["delivery_point_natural_id"], r["interval_start_utc"], r["order_interval_line_id"], r["source_revision"]
        ),
        "revenue_meter_reading": lambda r: (
            r["meter_natural_id"], r["register_natural_id"], r["reading_at_utc"], r["source_revision"]
        ),
        "delivery_point_capacity_assessment": lambda r: (
            r["delivery_point_natural_id"], r["interval_start_utc"], r["source_revision"]
        ),
    }
    return sorted(records, key=keys[dataset])


def build_bundle(
    start_date: date,
    end_date: date,
    seed: int,
    generation_time_utc: datetime,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    if seed < 0:
        raise ValueError("seed must be a non-negative integer")
    if start_date < SYNTHETIC_TIMELINE_START_LOCAL_DATE:
        raise ValueError(
            "synthetic source timeline starts on "
            f"{SYNTHETIC_TIMELINE_START_LOCAL_DATE.isoformat()}"
        )
    intervals = intervals_for_local_dates(start_date, end_date)
    window_start = intervals[0].start_utc
    window_end = intervals[-1].end_utc
    if generation_time_utc.tzinfo is None:
        raise ValueError("generation_time_utc must be timezone-aware")
    generation_time_utc = generation_time_utc.astimezone(UTC)
    history_boundary = SYNTHETIC_HISTORY_BOUNDARY_UTC
    deliveries = delivery_quantities(intervals, seed)

    records = {
        "customer_master": customer_records(
            SYNTHETIC_TIMELINE_START_UTC, history_boundary
        ),
        "industrial_site_master": site_records(
            SYNTHETIC_TIMELINE_START_UTC, history_boundary
        ),
        "delivery_point_assignment": delivery_point_assignment_records(
            SYNTHETIC_TIMELINE_START_UTC
        ),
        "revenue_meter_assignment": revenue_meter_assignment_records(
            SYNTHETIC_TIMELINE_START_UTC
        ),
        "contract_terms": contract_records(
            SYNTHETIC_TIMELINE_START_UTC, history_boundary
        ),
        "commitment_schedule": commitment_records(intervals),
        "approved_excess_order": excess_order_records(intervals),
        "revenue_meter_reading": meter_reading_records(intervals, deliveries, seed),
        "delivery_point_capacity_assessment": capacity_records(intervals),
    }
    records = {name: sort_records(name, rows) for name, rows in records.items()}
    latest_publication = max(
        datetime.fromisoformat(row["published_at_utc"].replace("Z", "+00:00"))
        for rows in records.values()
        for row in rows
    )
    if generation_time_utc < latest_publication:
        raise ValueError(
            "generation_time_utc cannot precede source evidence published at "
            f"{utc_text(latest_publication)}"
        )

    day_counts = Counter(item.local_date.isoformat() for item in intervals)
    length_summary = Counter(day_counts.values())
    first = ENTITIES[0]
    shared_boundary = intervals[8].end_utc
    scenarios = [
        {
            "scenario_id": "shortfall",
            "dataset": "commitment_schedule + revenue_meter_reading",
            "delivery_point_natural_id": first.delivery_point_id,
            "interval_start_utc": utc_text(intervals[2].start_utc),
            "committed_mwh_th": "5.000000",
            "delivered_mwh_th": "4.700000",
            "expected_shortfall_mwh_th": "0.300000",
        },
        {
            "scenario_id": "approved_excess_order",
            "dataset": "approved_excess_order + revenue_meter_reading",
            "delivery_point_natural_id": first.delivery_point_id,
            "interval_start_utc": utc_text(intervals[3].start_utc),
            "committed_mwh_th": "5.000000",
            "approved_extra_mwh_th": "0.400000",
            "delivered_mwh_th": "5.600000",
            "expected_billable_mwh_th": "5.400000",
            "expected_unbilled_excess_mwh_th": "0.200000",
        },
        {
            "scenario_id": "explicit_no_commitment_approved_maintenance",
            "dataset": "commitment_schedule",
            "delivery_point_natural_id": first.delivery_point_id,
            "interval_start_utc": utc_text(intervals[5].start_utc),
            "current_obligation_status": "no_commitment",
            "committed_mwh_th": "0.000000",
        },
        {
            "scenario_id": "missing_commitment_is_unknown",
            "dataset": "commitment_schedule",
            "delivery_point_natural_id": first.delivery_point_id,
            "interval_start_utc": utc_text(intervals[6].start_utc),
            "source_record_expected": False,
        },
        {
            "scenario_id": "shared_boundary_meter_correction",
            "dataset": "revenue_meter_reading",
            "meter_natural_id": first.meter_id,
            "reading_at_utc": utc_text(shared_boundary),
            "original_left_delivery_mwh_th": "4.700000",
            "corrected_left_delivery_mwh_th": "4.900000",
            "original_right_delivery_mwh_th": "5.300000",
            "corrected_right_delivery_mwh_th": "5.100000",
            "total_delivery_mwh_th": "10.000000",
            "original_sla_pct": "97.000000",
            "corrected_sla_pct": "99.000000",
            "original_net_revenue_gbp": "455.000000",
            "corrected_net_revenue_gbp": "485.000000",
        },
        {
            "scenario_id": "approved_retroactive_commitment_correction",
            "dataset": "commitment_schedule",
            "delivery_point_natural_id": first.delivery_point_id,
            "interval_start_utc": utc_text(intervals[10].start_utc),
            "revision_1_committed_mwh_th": "5.000000",
            "revision_2_committed_mwh_th": "5.500000",
            "delivered_mwh_th": "5.200000",
            "current_shortfall_mwh_th": "0.300000",
        },
        {
            "scenario_id": "capacity_provisional_final_correction",
            "dataset": "delivery_point_capacity_assessment",
            "delivery_point_natural_id": first.delivery_point_id,
            "interval_start_utc": utc_text(intervals[12].start_utc),
            "revision_1_status": "provisional",
            "revision_2_capacity_mwh_th": "5.500000",
            "revision_3_capacity_mwh_th": "4.500000",
            "expected_current_availability_pct": "90.000000",
        },
        {
            "scenario_id": "capacity_provisional_only",
            "dataset": "delivery_point_capacity_assessment",
            "delivery_point_natural_id": first.delivery_point_id,
            "interval_start_utc": utc_text(intervals[13].start_utc),
        },
        {
            "scenario_id": "capacity_final_zero",
            "dataset": "delivery_point_capacity_assessment",
            "delivery_point_natural_id": first.delivery_point_id,
            "interval_start_utc": utc_text(intervals[14].start_utc),
            "deliverable_capacity_mwh_th": "0.000000",
        },
        {
            "scenario_id": "capacity_missing_is_unknown",
            "dataset": "delivery_point_capacity_assessment",
            "delivery_point_natural_id": first.delivery_point_id,
            "interval_start_utc": utc_text(intervals[15].start_utc),
            "source_record_expected": False,
        },
        {
            "scenario_id": "customer_contract_site_history",
            "dataset": "customer_master + industrial_site_master + contract_terms",
            "customer_natural_id": first.customer_id,
            "site_natural_id": first.site_id,
            "contract_natural_id": first.contract_id,
            "effective_change_at_utc": utc_text(history_boundary),
            "includes_source_corrections": True,
        },
    ]

    manifest_context = {
        "artifact_type": "synthetic_source_data_manifest",
        "synthetic_data": True,
        "data_origin": "synthetic",
        "warning": "All customer, site, contract, order, meter, delivery, and capacity records in this manifest are fictional and generated for learning and testing.",
        "generator": {"name": GENERATOR_NAME, "version": GENERATOR_VERSION},
        "generated_at_utc": utc_text(generation_time_utc),
        "generation_time_policy": (
            "explicit caller-supplied timestamp, not earlier than any generated "
            "source publication; never the wall clock"
        ),
        "generation_parameters": {
            "start_date_local_inclusive": start_date.isoformat(),
            "end_date_local_inclusive": end_date.isoformat(),
            "timezone": "Europe/London",
            "seed": seed,
        },
        "coverage": {
            "start_utc_inclusive": utc_text(window_start),
            "end_utc_exclusive": utc_text(window_end),
            "utc_half_hour_interval_count": len(intervals),
            "local_day_interval_counts": dict(sorted(day_counts.items())),
            "local_day_length_summary": {
                "46_interval_days": length_summary.get(46, 0),
                "48_interval_days": length_summary.get(48, 0),
                "50_interval_days": length_summary.get(50, 0),
            },
        },
        "entities": {
            "customer_count": len(ENTITIES),
            "site_count": len(ENTITIES),
            "delivery_point_count": len(ENTITIES),
            "identifiers": [
                {
                    "customer_natural_id": item.customer_id,
                    "site_natural_id": item.site_id,
                    "delivery_point_natural_id": item.delivery_point_id,
                    "meter_natural_id": item.meter_id,
                    "contract_natural_id": item.contract_id,
                }
                for item in ENTITIES
            ],
        },
        "scenario_catalog": scenarios,
    }
    return records, manifest_context


def jsonl_bytes(records: list[dict[str, Any]]) -> bytes:
    text = "".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        for record in records
    )
    return text.encode("utf-8")


def write_bytes_atomic(path: Path, content: bytes) -> None:
    temporary_path = path.with_name(f".{path.name}.tmp")
    if temporary_path.exists():
        temporary_path.unlink()
    with temporary_path.open("wb") as handle:
        handle.write(content)
    temporary_path.replace(path)


def write_bundle(
    *,
    start_date: date,
    end_date: date,
    seed: int,
    generation_time_utc: datetime,
    output_dir: Path,
    overwrite: bool = False,
) -> dict[str, Any]:
    records, manifest = build_bundle(start_date, end_date, seed, generation_time_utc)
    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists() and not output_dir.is_dir():
        raise ValueError(f"output path is not a directory: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    target_names = [*DATASET_FILES.values(), "manifest.json"]
    existing = [output_dir / name for name in target_names if (output_dir / name).exists()]
    if existing and not overwrite:
        joined = ", ".join(path.name for path in existing)
        raise FileExistsError(
            f"generated targets already exist ({joined}); pass --overwrite to replace only these known files"
        )

    dataset_manifest: list[dict[str, Any]] = []
    for dataset, filename in DATASET_FILES.items():
        content = jsonl_bytes(records[dataset])
        write_bytes_atomic(output_dir / filename, content)
        dataset_manifest.append(
            {
                "dataset": dataset,
                "file": filename,
                "source_schema_id": SOURCE_SCHEMA_IDS[dataset],
                "source_schema_version": SCHEMA_VERSION,
                "record_grain": DATASET_GRAINS[dataset],
                "record_count": len(records[dataset]),
                "sha256": hashlib.sha256(content).hexdigest(),
                "format": "JSON Lines",
                "synthetic_data": True,
            }
        )

    manifest["datasets"] = dataset_manifest
    manifest["total_record_count"] = sum(item["record_count"] for item in dataset_manifest)
    manifest_content = (
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")
    write_bytes_atomic(output_dir / "manifest.json", manifest_content)
    return manifest


def validate_decimal_strings(records: dict[str, list[dict[str, Any]]]) -> None:
    for dataset, rows in records.items():
        for row_number, row in enumerate(rows, start=1):
            for field in DECIMAL_FIELDS.intersection(row):
                value = row[field]
                if not isinstance(value, str) or not DECIMAL_PATTERN.fullmatch(value):
                    raise AssertionError(
                        f"{dataset} row {row_number} field {field} is not a six-place decimal string: {value!r}"
                    )


def run_self_check() -> dict[str, Any]:
    expected_counts = {
        date(2026, 2, 1): 48,
        date(2026, 3, 29): 46,
        date(2026, 10, 25): 50,
    }
    observed_counts = {
        local_date.isoformat(): len(intervals_for_local_dates(local_date, local_date))
        for local_date in expected_counts
    }
    for local_date, expected in expected_counts.items():
        observed = observed_counts[local_date.isoformat()]
        if observed != expected:
            raise AssertionError(
                f"{local_date.isoformat()} produced {observed} intervals, expected {expected}"
            )

    fixed_generation_time = datetime(2026, 12, 31, tzinfo=UTC)
    records, manifest_context = build_bundle(
        date(2026, 10, 25), date(2026, 10, 25), 20260828, fixed_generation_time
    )
    if set(records) != set(DATASET_FILES):
        raise AssertionError("the bundle does not contain exactly the nine accepted synthetic sources")
    validate_decimal_strings(records)
    scenario_ids = {item["scenario_id"] for item in manifest_context["scenario_catalog"]}
    required_scenarios = {
        "shortfall",
        "approved_excess_order",
        "explicit_no_commitment_approved_maintenance",
        "missing_commitment_is_unknown",
        "shared_boundary_meter_correction",
        "capacity_provisional_final_correction",
        "customer_contract_site_history",
    }
    if not required_scenarios.issubset(scenario_ids):
        raise AssertionError("one or more required scenario markers are missing")

    with tempfile.TemporaryDirectory(prefix="energy-synthetic-check-a-") as first_dir, tempfile.TemporaryDirectory(
        prefix="energy-synthetic-check-b-"
    ) as second_dir:
        first_manifest = write_bundle(
            start_date=date(2026, 10, 25),
            end_date=date(2026, 10, 25),
            seed=77,
            generation_time_utc=fixed_generation_time,
            output_dir=Path(first_dir),
        )
        second_manifest = write_bundle(
            start_date=date(2026, 10, 25),
            end_date=date(2026, 10, 25),
            seed=77,
            generation_time_utc=fixed_generation_time,
            output_dir=Path(second_dir),
        )
        for filename in [*DATASET_FILES.values(), "manifest.json"]:
            if (Path(first_dir) / filename).read_bytes() != (Path(second_dir) / filename).read_bytes():
                raise AssertionError(f"determinism check failed for {filename}")
        if first_manifest != second_manifest:
            raise AssertionError("manifest objects differ for identical inputs")

    return {
        "status": "ok",
        "synthetic_dataset_count": len(DATASET_FILES),
        "dst_interval_counts": observed_counts,
        "deterministic_replay": True,
        "decimal_strings": "six_fractional_digits",
    }


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date {value!r}; use YYYY-MM-DD") from exc


def parse_utc_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid UTC timestamp {value!r}; use an RFC 3339 value such as 2026-12-31T00:00:00Z"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise argparse.ArgumentTypeError("generation timestamp must carry the UTC offset Z or +00:00")
    return parsed.astimezone(UTC)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate deterministic fictional Phase 2 source JSONL files."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser("generate", help="write all nine synthetic source datasets")
    generate_parser.add_argument("--start-date", required=True, type=parse_iso_date, help="inclusive Europe/London date")
    generate_parser.add_argument("--end-date", required=True, type=parse_iso_date, help="inclusive Europe/London date")
    generate_parser.add_argument("--seed", type=int, default=20260828, help="deterministic integer seed")
    generate_parser.add_argument(
        "--generation-time-utc",
        required=True,
        type=parse_utc_datetime,
        help="explicit deterministic RFC 3339 UTC generation time; the wall clock is never used",
    )
    generate_parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="caller-provided output directory; there is deliberately no repository default",
    )
    generate_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="replace only the generator's ten known output files if they already exist",
    )
    subparsers.add_parser("self-check", help="verify DST, scenarios, decimal encoding, and deterministic replay")
    return parser


def main() -> int:
    parser = build_parser()
    arguments = parser.parse_args()
    try:
        if arguments.command == "self-check":
            result = run_self_check()
        else:
            result = write_bundle(
                start_date=arguments.start_date,
                end_date=arguments.end_date,
                seed=arguments.seed,
                generation_time_utc=arguments.generation_time_utc,
                output_dir=arguments.output_dir,
                overwrite=arguments.overwrite,
            )
            result = {
                "status": "ok",
                "output_dir": str(arguments.output_dir.expanduser().resolve()),
                "synthetic_data": True,
                "dataset_count": len(result["datasets"]),
                "total_record_count": result["total_record_count"],
                "utc_half_hour_interval_count": result["coverage"]["utc_half_hour_interval_count"],
            }
    except (AssertionError, FileExistsError, OSError, ValueError) as exc:
        parser.error(str(exc))
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
