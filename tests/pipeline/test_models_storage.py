from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.batch.pipeline.models import (
    ImmutableObjectConflict,
    PipelineError,
    RunPlan,
    build_run_plan,
)
from ingestion.batch.synthetic.generate import GENERATOR_VERSION, SYNTHETIC_PROJECT_SEED
from tests.pipeline.fakes import FakeObjectStore


def environment(tmp_path: Path) -> dict[str, str]:
    return {
        "R2_RAW_BUCKET": "raw-test",
        "PIPELINE_WORK_ROOT": str(tmp_path / "work"),
        "TRINO_URL": "http://trino:8080",
    }


def test_run_plan_is_deterministic_and_contains_no_credentials(tmp_path: Path) -> None:
    first = build_run_plan(
        start_date="2026-08-27",
        end_date="2026-08-28",
        seed=SYNTHETIC_PROJECT_SEED,
        generation_time_utc="2026-08-31T12:00:00Z",
        orchestrator_run_id="manual__one",
        environment=environment(tmp_path),
    )
    second = build_run_plan(
        start_date="2026-08-27",
        end_date="2026-08-28",
        seed=SYNTHETIC_PROJECT_SEED,
        generation_time_utc="2026-08-31T12:00:00+00:00",
        orchestrator_run_id="manual__two",
        environment=environment(tmp_path),
    )
    assert first.pipeline_run_id == second.pipeline_run_id
    assert first.generation_time_utc == "2026-08-31T12:00:00Z"
    assert first.generator_version == GENERATOR_VERSION
    assert first.orchestrator_run_id != second.orchestrator_run_id
    serialized = first.to_dict()
    assert not any("secret" in key.lower() or "token" in key.lower() for key in serialized)
    assert RunPlan.from_mapping(serialized) == first


@pytest.mark.parametrize(
    ("start_date", "end_date", "seed", "generation_time", "message"),
    [
        (
            "2026-08-28",
            "2026-08-27",
            SYNTHETIC_PROJECT_SEED,
            "2026-09-01T00:00:00Z",
            "on or after",
        ),
        (
            "2026-01-01",
            "2026-02-02",
            SYNTHETIC_PROJECT_SEED,
            "2026-03-01T00:00:00Z",
            "31-day",
        ),
        (
            "2026-08-25",
            "2026-08-25",
            SYNTHETIC_PROJECT_SEED,
            "2026-09-01T00:00:00Z",
            "timeline start",
        ),
        ("2026-08-27", "2026-08-27", -1, "2026-09-01T00:00:00Z", "non-negative"),
        (
            "2026-08-27",
            "2026-08-27",
            1,
            "2026-09-01T00:00:00Z",
            "fixed synthetic project seed",
        ),
        (
            "2026-08-27",
            "2026-08-27",
            SYNTHETIC_PROJECT_SEED,
            "2026-09-01T03:00:00+03:00",
            "Z or",
        ),
    ],
)
def test_run_plan_rejects_unbounded_or_ambiguous_inputs(
    tmp_path: Path,
    start_date: str,
    end_date: str,
    seed: int,
    generation_time: str,
    message: str,
) -> None:
    with pytest.raises(PipelineError, match=message):
        build_run_plan(
            start_date=start_date,
            end_date=end_date,
            seed=seed,
            generation_time_utc=generation_time,
            orchestrator_run_id="manual__invalid",
            environment=environment(tmp_path),
        )


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"ICEBERG_CATALOG": "r2.invalid"}, "iceberg_catalog"),
        ({"R2_PIPELINE_PREFIX": ""}, "raw_prefix"),
        ({"R2_PIPELINE_PREFIX": "raw//source"}, "raw_prefix"),
    ],
)
def test_run_plan_rejects_unsafe_storage_names(
    tmp_path: Path, override: dict[str, str], message: str
) -> None:
    configured = {**environment(tmp_path), **override}
    with pytest.raises(PipelineError, match=message):
        build_run_plan(
            start_date="2026-08-27",
            end_date="2026-08-27",
            seed=SYNTHETIC_PROJECT_SEED,
            generation_time_utc="2026-09-01T00:00:00Z",
            orchestrator_run_id="manual__invalid-name",
            environment=configured,
        )


def test_fake_r2_store_creates_then_reuses_identical_content(tmp_path: Path) -> None:
    store = FakeObjectStore(tmp_path)
    first = store.put_immutable(
        bucket="raw-test",
        key="raw/example.jsonl",
        content=b'{"value":1}\n',
        content_type="application/x-ndjson",
    )
    second = store.put_immutable(
        bucket="raw-test",
        key="raw/example.jsonl",
        content=b'{"value":1}\n',
        content_type="application/x-ndjson",
    )
    assert first.disposition == "created"
    assert second.disposition == "reused"
    assert first.sha256 == second.sha256
    assert store.get_bytes(bucket="raw-test", key="raw/example.jsonl") == b'{"value":1}\n'


def test_fake_r2_store_rejects_conflicting_immutable_write(tmp_path: Path) -> None:
    store = FakeObjectStore(tmp_path)
    store.put_immutable(
        bucket="raw-test",
        key="raw/example.jsonl",
        content=b"first",
        content_type="application/x-ndjson",
    )
    with pytest.raises(ImmutableObjectConflict, match="different content"):
        store.put_immutable(
            bucket="raw-test",
            key="raw/example.jsonl",
            content=b"second",
            content_type="application/x-ndjson",
        )
