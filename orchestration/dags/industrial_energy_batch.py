"""Manual, bounded batch workflow for the Phase 2 source-contract slice."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
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
    dag_id="industrial_energy_bounded_batch",
    description=(
        "Generate a finite synthetic source range, land immutable raw evidence in R2, "
        "validate or quarantine it, load accepted rows into Iceberg, and reconcile the run."
    ),
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
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
                "produce the same source bundle."
            ),
        ),
    },
    render_template_as_native_obj=True,
    tags=["batch", "bounded", "iceberg", "r2", "synthetic"],
)
def industrial_energy_bounded_batch():
    """Build the finite raw-to-Iceberg evidence chain."""

    @task(task_id="plan_run", multiple_outputs=False)
    def plan_run() -> dict[str, Any]:
        from ingestion.batch.pipeline.workflow import plan_run_from_airflow

        return _small_xcom("plan_run", plan_run_from_airflow())

    @task(task_id="generate_source_bundle", multiple_outputs=False)
    def generate(plan: dict[str, Any]) -> dict[str, Any]:
        from ingestion.batch.pipeline.workflow import generate_source_bundle

        return _small_xcom("generate_source_bundle", generate_source_bundle(plan))

    @task(task_id="land_immutable_raw_bundle", multiple_outputs=False)
    def land_raw(plan: dict[str, Any], generation_result: dict[str, Any]) -> dict[str, Any]:
        from ingestion.batch.pipeline.workflow import land_raw_bundle

        return _small_xcom("land_raw_bundle", land_raw_bundle(plan, generation_result))

    @task(task_id="validate_and_quarantine", multiple_outputs=False)
    def validate(plan: dict[str, Any], raw_result: dict[str, Any]) -> dict[str, Any]:
        from ingestion.batch.pipeline.workflow import validate_raw_bundle

        return _small_xcom("validate_raw_bundle", validate_raw_bundle(plan, raw_result))

    @task(
        task_id="load_validated_rows_to_iceberg",
        multiple_outputs=False,
        pool="iceberg_writer",
        pool_slots=1,
    )
    def load_validated(
        plan: dict[str, Any],
        raw_result: dict[str, Any],
        validation_result: dict[str, Any],
    ) -> dict[str, Any]:
        from ingestion.batch.pipeline.workflow import load_validated_bundle

        return _small_xcom(
            "load_validated_bundle",
            load_validated_bundle(plan, raw_result, validation_result),
        )

    @task(task_id="reconcile_evidence_counts", multiple_outputs=False)
    def reconcile(
        plan: dict[str, Any],
        raw_result: dict[str, Any],
        validation_result: dict[str, Any],
        load_result: dict[str, Any],
    ) -> dict[str, Any]:
        from ingestion.batch.pipeline.workflow import reconcile_run

        return _small_xcom(
            "reconcile_run",
            reconcile_run(plan, raw_result, validation_result, load_result),
        )

    plan = plan_run()
    generation_result = generate(plan)
    raw_result = land_raw(plan, generation_result)
    validation_result = validate(plan, raw_result)
    load_result = load_validated(plan, raw_result, validation_result)
    reconcile(plan, raw_result, validation_result, load_result)


industrial_energy_bounded_batch()
