"""Schedule the existing bounded steam-delivery pipeline once per day."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import CronDataIntervalTimetable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from orchestration.daily_schedule import (
    DailyPipelineLate,
    DailyScheduleError,
    assess_daily_run_completion,
    build_daily_run_request,
    require_scheduled_run,
)


@dag(
    dag_id="daily_steam_delivery_data_pipeline",
    description=(
        "At noon Europe/London, load the previous completed operating date by "
        "running the existing bounded steam-delivery pipeline and report whether "
        "the complete child run finished by 16:00."
    ),
    schedule=CronDataIntervalTimetable(
        "0 12 * * *",
        timezone="Europe/London",
    ),
    start_date=pendulum.datetime(2026, 8, 26, 12, 0, tz="Europe/London"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4, minutes=15),
    default_args={
        "owner": "industrial-energy-data-platform",
        "retries": 0,
    },
    render_template_as_native_obj=True,
    tags=["batch", "daily", "freshness", "steam-delivery", "synthetic"],
)
def daily_steam_delivery_data_pipeline():
    """Load one completed London operating date through the proven manual DAG."""

    @task(task_id="choose_completed_operating_date", multiple_outputs=True)
    def choose_completed_operating_date() -> dict[str, Any]:
        """Turn this scheduled interval into one deterministic one-day request."""

        context = get_current_context()
        try:
            require_scheduled_run(context["dag_run"].run_type)
            return build_daily_run_request(
                data_interval_start=context["data_interval_start"],
                data_interval_end=context["data_interval_end"],
            )
        except DailyScheduleError as error:
            raise AirflowFailException(str(error)) from error

    daily_request = choose_completed_operating_date()

    run_existing_pipeline = TriggerDagRunOperator(
        task_id="run_existing_steam_delivery_pipeline_for_that_date",
        trigger_dag_id="steam_delivery_data_pipeline",
        trigger_run_id=daily_request["trigger_run_id"],
        conf=daily_request["manual_dag_conf"],
        logical_date=daily_request["scheduled_for_utc"],
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=False,
        skip_when_already_exists=False,
        fail_when_dag_is_paused=True,
        deferrable=True,
        # The child accepts exactly the four governed run parameters. Lineage
        # already links the stable parent/child run IDs, so do not add optional
        # provider metadata to that deterministic configuration.
        openlineage_inject_parent_info=False,
        retries=0,
        execution_timeout=timedelta(hours=4, minutes=5),
    )

    @task(task_id="check_daily_pipeline_finished_on_time", multiple_outputs=False)
    def check_daily_pipeline_finished_on_time(
        request: dict[str, Any]
    ) -> dict[str, Any]:
        """Make a missed 16:00 workflow deadline visible as a failed run."""

        try:
            return assess_daily_run_completion(
                request, checked_at_utc=datetime.now(UTC)
            )
        except (DailyPipelineLate, DailyScheduleError) as error:
            raise AirflowFailException(str(error)) from error

    freshness_result = check_daily_pipeline_finished_on_time(
        {
            "freshness_contract_version": daily_request[
                "freshness_contract_version"
            ],
            "operating_date": daily_request["operating_date"],
            "expected_ready_by_utc": daily_request["expected_ready_by_utc"],
        }
    )
    run_existing_pipeline >> freshness_result


daily_steam_delivery_data_pipeline()
