"""Pure scheduling and freshness rules for the daily steam-delivery run.

This module deliberately has no Airflow imports.  Airflow supplies the resolved
data interval; these functions turn it into the one-day request that the
existing manual pipeline already understands.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from math import ceil
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from ingestion.batch.synthetic.generate import (
    SYNTHETIC_PROJECT_SEED,
    SYNTHETIC_TIMELINE_START_LOCAL_DATE,
)

LONDON = ZoneInfo("Europe/London")
DAILY_SCHEDULE_LOCAL_TIME = time(12, 0)
DAILY_FRESHNESS_DEADLINE_LOCAL_TIME = time(16, 0)
DAILY_FRESHNESS_CONTRACT_VERSION = "1.0.0"


class DailyScheduleError(ValueError):
    """The Airflow interval cannot safely identify one operating date."""


class DailyPipelineLate(RuntimeError):
    """The complete daily child pipeline finished after its deadline."""


def _aware_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise DailyScheduleError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _utc_text(value: datetime) -> str:
    return _aware_utc(value, "timestamp").isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _parse_utc_text(value: object, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise DailyScheduleError(f"{field_name} must be an RFC 3339 UTC string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise DailyScheduleError(
            f"{field_name} must be an RFC 3339 UTC string"
        ) from error
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise DailyScheduleError(f"{field_name} must carry a UTC offset")
    return parsed.astimezone(UTC)


def require_scheduled_run(run_type: object) -> None:
    """Prevent a manual trigger from receiving ambiguous Airflow 3 intervals."""

    normalized = str(getattr(run_type, "value", run_type)).strip().lower()
    if normalized != "scheduled":
        raise DailyScheduleError(
            "daily_steam_delivery_data_pipeline accepts scheduled runs only; "
            "use steam_delivery_data_pipeline for manual replays and backfills"
        )


def build_daily_run_request(
    *, data_interval_start: datetime, data_interval_end: datetime
) -> dict[str, Any]:
    """Map one noon-to-noon Airflow interval to the prior London date.

    A cron data interval can last 23, 24, or 25 hours when the UK clock changes.
    The local calendar dates, rather than an assumed 24-hour duration, identify
    the completed operating day.
    """

    start_utc = _aware_utc(data_interval_start, "data_interval_start")
    end_utc = _aware_utc(data_interval_end, "data_interval_end")
    start_local = start_utc.astimezone(LONDON)
    end_local = end_utc.astimezone(LONDON)

    for field_name, value in (
        ("data_interval_start", start_local),
        ("data_interval_end", end_local),
    ):
        if value.timetz().replace(tzinfo=None) != DAILY_SCHEDULE_LOCAL_TIME:
            raise DailyScheduleError(
                f"{field_name} must resolve to 12:00 Europe/London"
            )

    if end_local.date() != start_local.date() + timedelta(days=1):
        raise DailyScheduleError(
            "the daily Airflow interval must span consecutive London dates"
        )

    operating_date = end_local.date() - timedelta(days=1)
    if operating_date != start_local.date():
        raise DailyScheduleError(
            "the Airflow interval does not identify exactly one London operating date"
        )
    if operating_date < SYNTHETIC_TIMELINE_START_LOCAL_DATE:
        raise DailyScheduleError(
            "the operating date precedes the synthetic timeline start "
            f"{SYNTHETIC_TIMELINE_START_LOCAL_DATE.isoformat()}"
        )

    # The generation time is the scheduler's noon boundary, not the task wall
    # clock. Retrying the same scheduled interval therefore produces exactly the
    # same evidence identity even if the task starts later.
    generation_time_utc = _utc_text(end_utc)
    deadline_local = datetime.combine(
        operating_date + timedelta(days=1),
        DAILY_FRESHNESS_DEADLINE_LOCAL_TIME,
        tzinfo=LONDON,
    )
    date_text = operating_date.isoformat()
    return {
        "freshness_contract_version": DAILY_FRESHNESS_CONTRACT_VERSION,
        "operating_date": date_text,
        "scheduled_for_utc": generation_time_utc,
        "expected_ready_by_utc": _utc_text(deadline_local),
        "trigger_run_id": f"daily__{operating_date:%Y%m%d}",
        "manual_dag_conf": {
            "start_date": date_text,
            "end_date": date_text,
            "seed": SYNTHETIC_PROJECT_SEED,
            "generation_time_utc": generation_time_utc,
        },
    }


def assess_daily_run_completion(
    request: Mapping[str, Any], *, checked_at_utc: datetime
) -> dict[str, Any]:
    """Return an on-time result or raise after the daily workflow deadline.

    This check is intended to run only after the triggered manual pipeline has
    succeeded. That success includes the tested ClickHouse publication and its
    following serving-retention task. The check deliberately measures when the
    complete child run became observable to the wrapper, not the marker's own
    publication timestamp.
    """

    checked = _aware_utc(checked_at_utc, "checked_at_utc")
    deadline = _parse_utc_text(
        request.get("expected_ready_by_utc"), "expected_ready_by_utc"
    )
    operating_date_value = request.get("operating_date")
    if not isinstance(operating_date_value, str):
        raise DailyScheduleError("operating_date is missing from the daily request")
    try:
        operating_date = date.fromisoformat(operating_date_value)
    except ValueError as error:
        raise DailyScheduleError("operating_date must use YYYY-MM-DD") from error

    time_from_deadline = checked - deadline
    if time_from_deadline > timedelta(0):
        seconds_late = max(1, ceil(time_from_deadline.total_seconds()))
        unit = "second" if seconds_late == 1 else "seconds"
        raise DailyPipelineLate(
            f"daily pipeline for {operating_date.isoformat()} finished "
            f"{seconds_late} {unit} after the daily freshness deadline "
            f"{_utc_text(deadline)}"
        )

    return {
        "freshness_contract_version": str(
            request.get(
                "freshness_contract_version", DAILY_FRESHNESS_CONTRACT_VERSION
            )
        ),
        "operating_date": operating_date.isoformat(),
        "status": "on_time",
        "checked_at_utc": _utc_text(checked),
        "expected_ready_by_utc": _utc_text(deadline),
        "seconds_before_deadline": max(
            0, int((deadline - checked).total_seconds())
        ),
    }


__all__ = [
    "DAILY_FRESHNESS_CONTRACT_VERSION",
    "DAILY_FRESHNESS_DEADLINE_LOCAL_TIME",
    "DAILY_SCHEDULE_LOCAL_TIME",
    "DailyPipelineLate",
    "DailyScheduleError",
    "assess_daily_run_completion",
    "build_daily_run_request",
    "require_scheduled_run",
]
