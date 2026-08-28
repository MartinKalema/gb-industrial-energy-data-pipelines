from __future__ import annotations

import json
from pathlib import Path

from ingestion.batch.pipeline.workflow import (
    generate_source_bundle,
    land_raw_bundle,
    plan_run,
)
from tests.pipeline.fakes import FakeObjectStore


def test_generation_and_raw_landing_are_bounded_immutable_and_replayable(
    tmp_path: Path,
) -> None:
    plan = plan_run(
        start_date="2026-08-27",
        end_date="2026-08-27",
        seed=20260828,
        generation_time_utc="2026-08-28T12:00:00Z",
        orchestrator_run_id="manual__raw-test",
        environment={
            "R2_RAW_BUCKET": "raw-test",
            "PIPELINE_WORK_ROOT": str(tmp_path / "work"),
            "TRINO_URL": "http://trino:8080",
        },
    )
    generated = generate_source_bundle(plan)
    store = FakeObjectStore(tmp_path / "fake-r2")

    first = land_raw_bundle(plan, generated, store=store)
    second = land_raw_bundle(plan, generated, store=store)

    assert first["raw_record_count"] == 313
    assert len(first["raw_artifacts"]) == 9
    assert len(first["evidence_envelopes"]) == 9
    assert {item["disposition"] for item in first["raw_artifacts"]} == {"created"}
    assert {item["disposition"] for item in second["raw_artifacts"]} == {"reused"}
    assert first["raw_manifest"]["disposition"] == "created"
    assert second["raw_manifest"]["disposition"] == "reused"

    first_envelope = first["evidence_envelopes"][0]
    envelope = json.loads(
        store.get_bytes(bucket="raw-test", key=first_envelope["key"])
    )
    assert envelope["raw_object_uri"].startswith("r2://raw-test/")
    assert envelope["payload_sha256"] == first["raw_artifacts"][0]["sha256"]
    assert (
        envelope["ingested_at_utc"]
        == first["raw_artifacts"][0]["last_modified_utc"]
    )
    assert envelope["ingested_at_utc"] != plan["generation_time_utc"]
    assert envelope["generator_run_id"] == plan["pipeline_run_id"]
