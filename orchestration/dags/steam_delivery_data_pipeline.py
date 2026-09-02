"""Generate, validate, load, and model bounded steam-delivery data."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.sdk import Param, dag, task

_MAX_XCOM_BYTES = 16 * 1024


def _small_xcom(stage: str, value: Any) -> dict[str, Any]:
    """Require stage functions to exchange summaries rather than row payloads."""

    if not isinstance(value, dict):
        raise TypeError(f"{stage} must return a dictionary, got {type(value).__name__}")

    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise TypeError(f"{stage} returned a non-JSON-serializable summary") from error

    if len(encoded) > _MAX_XCOM_BYTES:
        raise ValueError(
            f"{stage} returned {len(encoded)} XCom bytes; maximum is {_MAX_XCOM_BYTES}. "
            "Persist records externally and return only paths, identifiers, counts, and hashes."
        )
    return value


@dag(
    dag_id="steam_delivery_data_pipeline",
    description=(
        "Generate synthetic steam-delivery source files for a finite date range, "
        "save the originals in R2, validate every row, load accepted rows into "
        "Iceberg, verify every row was handled, record the loaded dates, and build "
        "and test the dimensional mart through restartable dbt checkpoints."
    ),
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=180),
    default_args={
        "owner": "industrial-energy-data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=20),
    },
    params={
        "start_date": Param(
            "2026-08-26",
            type="string",
            format="date",
            description="First Europe/London operating date to generate (inclusive).",
        ),
        "end_date": Param(
            "2026-08-26",
            type="string",
            format="date",
            description="Last Europe/London operating date to generate (inclusive).",
        ),
        "seed": Param(
            20260828,
            type="integer",
            minimum=0,
            maximum=9_223_372_036_854_775_807,
            enum=[20260828],
            description="Fixed project seed for the continuous synthetic timeline.",
        ),
        "generation_time_utc": Param(
            "2026-08-28T12:00:00Z",
            type="string",
            format="date-time",
            pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$",
            description=(
                "Fixed UTC evidence-generation timestamp. It must end in Z so retries "
                "produce the same source files."
            ),
        ),
    },
    render_template_as_native_obj=True,
    tags=["batch", "bounded", "dbt", "iceberg", "r2", "steam-delivery", "synthetic"],
)
def steam_delivery_data_pipeline():
    """Build the finite source-to-dimensional-mart data chain."""

    @task(task_id="validate_run_parameters", multiple_outputs=False)
    def validate_and_prepare_run() -> dict[str, Any]:
        """Check the requested dates and create this run's working folder."""

        from ingestion.batch.pipeline.workflow import plan_run_from_airflow

        return _small_xcom("validate_run_parameters", plan_run_from_airflow())

    @task(task_id="generate_synthetic_source_files", multiple_outputs=False)
    def generate_source_files(plan: dict[str, Any]) -> dict[str, Any]:
        """Create the nine fictional business-source files for the date range."""

        from ingestion.batch.pipeline.workflow import generate_source_bundle

        return _small_xcom(
            "generate_synthetic_source_files",
            generate_source_bundle(plan),
        )

    @task(task_id="save_original_source_files_to_r2", multiple_outputs=False)
    def save_original_source_files(
        plan: dict[str, Any], generation_result: dict[str, Any]
    ) -> dict[str, Any]:
        """Save the original generated files and their evidence details in R2."""

        from ingestion.batch.pipeline.workflow import land_raw_bundle

        return _small_xcom(
            "save_original_source_files_to_r2",
            land_raw_bundle(plan, generation_result),
        )

    @task(
        task_id="validate_source_rows_and_save_failures_separately",
        multiple_outputs=False,
    )
    def validate_source_rows(
        plan: dict[str, Any], raw_result: dict[str, Any]
    ) -> dict[str, Any]:
        """Accept valid rows and preserve failed rows in quarantine with reasons."""

        from ingestion.batch.pipeline.workflow import validate_raw_bundle

        return _small_xcom(
            "validate_source_rows_and_save_failures_separately",
            validate_raw_bundle(plan, raw_result),
        )

    @task(
        task_id="load_validated_rows_to_iceberg",
        multiple_outputs=False,
        pool="iceberg_writer",
        pool_slots=1,
    )
    def load_validated_rows(
        plan: dict[str, Any],
        raw_result: dict[str, Any],
        validation_result: dict[str, Any],
    ) -> dict[str, Any]:
        """Write accepted source rows into typed Iceberg tables through Trino."""

        from ingestion.batch.pipeline.workflow import load_validated_bundle

        return _small_xcom(
            "load_validated_rows_to_iceberg",
            load_validated_bundle(plan, raw_result, validation_result),
        )

    @task(task_id="verify_every_source_row_was_handled", multiple_outputs=False)
    def verify_every_source_row(
        plan: dict[str, Any],
        raw_result: dict[str, Any],
        validation_result: dict[str, Any],
        load_result: dict[str, Any],
    ) -> dict[str, Any]:
        """Prove that every original row was accepted, quarantined, or replayed."""

        from ingestion.batch.pipeline.workflow import reconcile_run

        return _small_xcom(
            "verify_every_source_row_was_handled",
            reconcile_run(plan, raw_result, validation_result, load_result),
        )

    @task(
        task_id="record_successfully_loaded_date_range",
        multiple_outputs=False,
        pool="iceberg_writer",
        pool_slots=1,
    )
    def record_successfully_loaded_date_range(
        plan: dict[str, Any],
        raw_result: dict[str, Any],
        reconciliation_result: dict[str, Any],
    ) -> dict[str, Any]:
        """Record which operating dates are complete enough for dbt to model."""

        from ingestion.batch.pipeline.workflow import publish_batch_run_coverage

        return _small_xcom(
            "record_successfully_loaded_date_range",
            publish_batch_run_coverage(plan, raw_result, reconciliation_result),
        )

    @task(
        task_id="run_one_dbt_checkpoint",
        multiple_outputs=False,
        pool="iceberg_writer",
        pool_slots=1,
        execution_timeout=timedelta(minutes=125),
        retries=1,
        retry_delay=timedelta(minutes=2),
    )
    def run_dbt_checkpoint(
        plan: dict[str, Any],
        coverage_result: dict[str, Any],
        step_name: str,
    ) -> dict[str, Any]:
        """Run one restartable part of the dimensional build and tests."""

        from airflow.sdk import get_current_context
        from airflow.sdk.exceptions import AirflowFailException

        from ingestion.batch.pipeline.dbt_build import DbtRemoteCleanupError
        from ingestion.batch.pipeline.workflow import build_dimensional_mart_step

        context = get_current_context()
        attempt_number = int(context["task_instance"].try_number)
        try:
            checkpoint_result = build_dimensional_mart_step(
                plan,
                coverage_result,
                step_name=step_name,
                attempt_number=attempt_number,
            )
        except DbtRemoteCleanupError as error:
            # A retry could overlap a Trino write whose terminal state was not
            # confirmed. Require an operator to verify Trino before rerunning.
            raise AirflowFailException(str(error)) from error
        return _small_xcom(
            f"{step_name}_with_dbt",
            checkpoint_result,
        )

    @task(
        task_id="publish_tested_dimensional_mart_to_clickhouse",
        multiple_outputs=False,
        pool="iceberg_writer",
        pool_slots=1,
        execution_timeout=timedelta(minutes=20),
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def publish_tested_dimensional_mart_to_clickhouse(
        plan: dict[str, Any],
        coverage_result: dict[str, Any],
        dbt_test_result: dict[str, Any],
    ) -> dict[str, Any]:
        """Incrementally publish the tested delivery read model to ClickHouse."""

        from ingestion.batch.pipeline.workflow import (
            publish_tested_dimensional_mart_to_clickhouse as publish_mart,
        )

        return _small_xcom(
            "publish_tested_dimensional_mart_to_clickhouse",
            publish_mart(
                plan,
                coverage_result,
                dbt_test_result,
            ),
        )

    @task(
        task_id="remove_old_clickhouse_serving_versions",
        multiple_outputs=False,
        pool="iceberg_writer",
        pool_slots=1,
        execution_timeout=timedelta(minutes=20),
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def remove_old_clickhouse_serving_versions(
        publication_result: dict[str, Any],
    ) -> dict[str, Any]:
        """Keep the newest serving versions and remove failed or older copies."""

        from ingestion.batch.pipeline.workflow import (
            remove_old_clickhouse_serving_versions as remove_old_versions,
        )

        return _small_xcom(
            "remove_old_clickhouse_serving_versions",
            remove_old_versions(publication_result),
        )

    plan = validate_and_prepare_run()
    generation_result = generate_source_files(plan)
    raw_result = save_original_source_files(plan, generation_result)
    validation_result = validate_source_rows(plan, raw_result)
    load_result = load_validated_rows(plan, raw_result, validation_result)
    reconciliation_result = verify_every_source_row(
        plan, raw_result, validation_result, load_result
    )
    coverage_result = record_successfully_loaded_date_range(
        plan, raw_result, reconciliation_result
    )
    prepare_loaded_data = run_dbt_checkpoint.override(
        task_id="prepare_and_test_loaded_data_with_dbt"
    )(plan, coverage_result, "prepare_and_test_loaded_data")
    prepare_delivery_calculations = run_dbt_checkpoint.override(
        task_id="prepare_and_test_delivery_calculations_with_dbt"
    )(plan, coverage_result, "prepare_and_test_delivery_calculations")
    build_current_fact = run_dbt_checkpoint.override(
        task_id="build_current_delivery_fact_with_dbt"
    )(plan, coverage_result, "build_current_delivery_fact")
    build_history_fact = run_dbt_checkpoint.override(
        task_id="build_delivery_history_fact_with_dbt"
    )(plan, coverage_result, "build_delivery_history_fact")
    build_dimensions = run_dbt_checkpoint.override(
        task_id="build_dimension_tables_with_dbt"
    )(plan, coverage_result, "build_dimension_tables")
    test_complete_mart = run_dbt_checkpoint.override(
        task_id="test_complete_dimensional_mart_with_dbt"
    )(plan, coverage_result, "test_complete_dimensional_mart")
    clickhouse_publication = publish_tested_dimensional_mart_to_clickhouse(
        plan,
        coverage_result,
        test_complete_mart,
    )
    serving_retention = remove_old_clickhouse_serving_versions(
        clickhouse_publication
    )

    (
        prepare_loaded_data
        >> prepare_delivery_calculations
        >> build_current_fact
        >> build_history_fact
        >> build_dimensions
        >> test_complete_mart
        >> clickhouse_publication
        >> serving_retention
    )


steam_delivery_data_pipeline()
