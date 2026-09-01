from __future__ import annotations

import ast
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from orchestration.daily_schedule import (
    DailyPipelineLate,
    DailyScheduleError,
    assess_daily_run_completion,
    build_daily_run_request,
    require_scheduled_run,
)
from ingestion.batch.synthetic.generate import build_bundle

LONDON = ZoneInfo("Europe/London")
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DAILY_DAG_PATH = (
    REPOSITORY_ROOT
    / "orchestration"
    / "dags"
    / "daily_steam_delivery_data_pipeline.py"
)


@pytest.mark.parametrize(
    ("start_utc", "end_utc", "operating_date", "generation_time", "deadline"),
    [
        (
            datetime(2026, 8, 26, 11, tzinfo=UTC),
            datetime(2026, 8, 27, 11, tzinfo=UTC),
            "2026-08-26",
            "2026-08-27T11:00:00Z",
            "2026-08-27T15:00:00Z",
        ),
        # The UK clock moves back inside this 25-hour noon-to-noon interval.
        (
            datetime(2026, 10, 24, 11, tzinfo=UTC),
            datetime(2026, 10, 25, 12, tzinfo=UTC),
            "2026-10-24",
            "2026-10-25T12:00:00Z",
            "2026-10-25T16:00:00Z",
        ),
        # The UK clock moves forward inside this 23-hour noon-to-noon interval.
        (
            datetime(2027, 3, 27, 12, tzinfo=UTC),
            datetime(2027, 3, 28, 11, tzinfo=UTC),
            "2027-03-27",
            "2027-03-28T11:00:00Z",
            "2027-03-28T15:00:00Z",
        ),
    ],
)
def test_daily_request_uses_london_calendar_dates_across_clock_changes(
    start_utc: datetime,
    end_utc: datetime,
    operating_date: str,
    generation_time: str,
    deadline: str,
) -> None:
    request = build_daily_run_request(
        data_interval_start=start_utc,
        data_interval_end=end_utc,
    )

    assert request["operating_date"] == operating_date
    assert request["scheduled_for_utc"] == generation_time
    assert request["expected_ready_by_utc"] == deadline
    assert request["trigger_run_id"] == f"daily__{operating_date.replace('-', '')}"
    assert request["manual_dag_conf"] == {
        "start_date": operating_date,
        "end_date": operating_date,
        "seed": 20260828,
        "generation_time_utc": generation_time,
    }


@pytest.mark.parametrize(
    "operating_date",
    [
        date(2026, 8, 26),
        date(2026, 10, 24),  # the following morning changes from BST to GMT
        date(2027, 3, 27),  # the following morning changes from GMT to BST
    ],
)
def test_noon_schedule_is_after_every_generated_source_publication(
    operating_date: date,
) -> None:
    schedule_local = datetime.combine(
        operating_date + timedelta(days=1), time(12), tzinfo=LONDON
    )
    records, _manifest = build_bundle(
        operating_date,
        operating_date,
        20260828,
        schedule_local,
    )
    latest_publication = max(
        datetime.fromisoformat(row["published_at_utc"].replace("Z", "+00:00"))
        for dataset_rows in records.values()
        for row in dataset_rows
    )

    assert latest_publication < schedule_local.astimezone(UTC)


def test_daily_request_rejects_a_boundary_that_is_not_noon_in_london() -> None:
    with pytest.raises(DailyScheduleError, match="12:00 Europe/London"):
        build_daily_run_request(
            data_interval_start=datetime(2026, 8, 26, 10, tzinfo=UTC),
            data_interval_end=datetime(2026, 8, 27, 10, tzinfo=UTC),
        )


def test_daily_request_rejects_naive_datetimes() -> None:
    with pytest.raises(DailyScheduleError, match="timezone-aware"):
        build_daily_run_request(
            data_interval_start=datetime(2026, 8, 26, 12),
            data_interval_end=datetime(2026, 8, 27, 12, tzinfo=LONDON),
        )


def test_daily_request_rejects_dates_before_the_synthetic_timeline() -> None:
    with pytest.raises(DailyScheduleError, match="timeline start"):
        build_daily_run_request(
            data_interval_start=datetime(2026, 8, 25, 12, tzinfo=LONDON),
            data_interval_end=datetime(2026, 8, 26, 12, tzinfo=LONDON),
        )


def test_daily_dag_rejects_manual_and_backfill_run_types() -> None:
    require_scheduled_run("scheduled")
    with pytest.raises(DailyScheduleError, match="scheduled runs only"):
        require_scheduled_run("manual")
    with pytest.raises(DailyScheduleError, match="scheduled runs only"):
        require_scheduled_run("backfill_job")


def test_freshness_check_accepts_the_deadline_and_reports_time_remaining() -> None:
    request = build_daily_run_request(
        data_interval_start=datetime(2026, 8, 26, 12, tzinfo=LONDON),
        data_interval_end=datetime(2026, 8, 27, 12, tzinfo=LONDON),
    )

    result = assess_daily_run_completion(
        request,
        checked_at_utc=datetime(2026, 8, 27, 14, 59, tzinfo=UTC),
    )

    assert result["status"] == "on_time"
    assert result["seconds_before_deadline"] == 60

    at_deadline = assess_daily_run_completion(
        request,
        checked_at_utc=datetime(2026, 8, 27, 15, 0, tzinfo=UTC),
    )
    assert at_deadline["seconds_before_deadline"] == 0


def test_freshness_check_raises_after_the_deadline() -> None:
    request = build_daily_run_request(
        data_interval_start=datetime(2026, 8, 26, 12, tzinfo=LONDON),
        data_interval_end=datetime(2026, 8, 27, 12, tzinfo=LONDON),
    )

    with pytest.raises(DailyPipelineLate, match="61 seconds after"):
        assess_daily_run_completion(
            request,
            checked_at_utc=datetime(2026, 8, 27, 15, 1, 1, tzinfo=UTC),
        )

    with pytest.raises(DailyPipelineLate, match="1 second after"):
        assess_daily_run_completion(
            request,
            checked_at_utc=datetime(
                2026, 8, 27, 15, 0, 0, 1, tzinfo=UTC
            ),
        )


def _daily_dag_decorator() -> ast.Call:
    module = ast.parse(DAILY_DAG_PATH.read_text())
    function = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "daily_steam_delivery_data_pipeline"
    )
    return next(
        item
        for item in function.decorator_list
        if isinstance(item, ast.Call)
        and isinstance(item.func, ast.Name)
        and item.func.id == "dag"
    )


def test_daily_dag_is_bounded_scheduled_and_does_not_catch_up() -> None:
    keywords = {
        keyword.arg: keyword.value for keyword in _daily_dag_decorator().keywords
    }

    assert ast.literal_eval(keywords["dag_id"]) == (
        "daily_steam_delivery_data_pipeline"
    )
    assert ast.unparse(keywords["schedule"]) == (
        "CronDataIntervalTimetable('0 12 * * *', timezone='Europe/London')"
    )
    assert ast.literal_eval(keywords["catchup"]) is False
    assert ast.literal_eval(keywords["max_active_runs"]) == 1
    assert ast.unparse(keywords["start_date"]) == (
        "pendulum.datetime(2026, 8, 26, 12, 0, tz='Europe/London')"
    )


def test_daily_request_task_exposes_named_xcom_values() -> None:
    module = ast.parse(DAILY_DAG_PATH.read_text())
    pipeline = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "daily_steam_delivery_data_pipeline"
    )
    choose_task = next(
        node
        for node in pipeline.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "choose_completed_operating_date"
    )
    decorator = next(
        item
        for item in choose_task.decorator_list
        if isinstance(item, ast.Call)
        and isinstance(item.func, ast.Name)
        and item.func.id == "task"
    )
    keywords = {keyword.arg: keyword.value for keyword in decorator.keywords}
    assert ast.literal_eval(keywords["multiple_outputs"]) is True


def test_daily_dag_calls_the_existing_pipeline_and_waits_for_success() -> None:
    module = ast.parse(DAILY_DAG_PATH.read_text())
    trigger = next(
        node.value
        for node in ast.walk(module)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "run_existing_pipeline"
            for target in node.targets
        )
        and isinstance(node.value, ast.Call)
    )
    assert isinstance(trigger, ast.Call)
    assert isinstance(trigger.func, ast.Name)
    assert trigger.func.id == "TriggerDagRunOperator"
    keywords = {keyword.arg: keyword.value for keyword in trigger.keywords}

    assert ast.literal_eval(keywords["task_id"]) == (
        "run_existing_steam_delivery_pipeline_for_that_date"
    )
    assert ast.literal_eval(keywords["trigger_dag_id"]) == (
        "steam_delivery_data_pipeline"
    )
    assert ast.literal_eval(keywords["wait_for_completion"]) is True
    assert ast.literal_eval(keywords["allowed_states"]) == ["success"]
    assert ast.literal_eval(keywords["failed_states"]) == ["failed"]
    assert ast.literal_eval(keywords["reset_dag_run"]) is False
    assert ast.literal_eval(keywords["skip_when_already_exists"]) is False
    assert ast.literal_eval(keywords["fail_when_dag_is_paused"]) is True
    assert ast.literal_eval(keywords["deferrable"]) is True
    assert ast.literal_eval(keywords["openlineage_inject_parent_info"]) is False
    assert ast.literal_eval(keywords["retries"]) == 0


def test_airflow_runtime_pins_the_standard_provider_used_by_the_daily_dag() -> None:
    requirements = (
        REPOSITORY_ROOT / "orchestration" / "airflow-requirements.txt"
    ).read_text()
    assert "apache-airflow==3.3.1" in requirements
    assert "apache-airflow-providers-standard==1.17.0" in requirements


def test_airflow_runtime_applies_the_triggered_run_configuration() -> None:
    compose = (REPOSITORY_ROOT / "infrastructure" / "compose.yaml").read_text()

    assert 'AIRFLOW__CORE__DAG_RUN_CONF_OVERRIDES_PARAMS: "True"' in compose
