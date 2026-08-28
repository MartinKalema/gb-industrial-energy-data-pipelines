"""Focused invariants for the bounded Trino/Iceberg source loader."""

from __future__ import annotations

import json
import re
from copy import deepcopy
from datetime import date, datetime, timezone

import pytest

from ingestion.batch.pipeline import trino_loader as loader_module
from ingestion.batch.pipeline.trino_loader import (
    AcceptedRecord,
    IDENTITY_FIELDS,
    IcebergLoaderConfig,
    QueryResult,
    SYNTHETIC_DATASETS,
    TrinoHttpClient,
    TrinoIcebergLoader,
    TrinoProtocolError,
    TrinoQueryError,
)
from ingestion.batch.pipeline.validation import SOURCE_REVISION_KEY_FIELDS
from ingestion.batch.synthetic.generate import build_bundle


GENERATION_TIME = datetime(2026, 12, 31, 12, 0, tzinfo=timezone.utc)
SHA256 = re.compile(r"[a-f0-9]{64}")
FINAL_HASH_PAIR = re.compile(r", '([a-f0-9]{64})', '([a-f0-9]{64})'\)")


class MemoryStatementRunner:
    """Tiny Iceberg identity index driven by the SQL emitted by the loader."""

    def __init__(self, *, fail_once_after_merge_commit: bool = False):
        self.payload_by_identity: dict[str, str] = {}
        self.statements: list[str] = []
        self.merge_calls = 0
        self.fail_once_after_merge_commit = fail_once_after_merge_commit

    def execute(self, sql: str) -> QueryResult:
        self.statements.append(sql)
        if sql.startswith(("CREATE SCHEMA", "CREATE TABLE")):
            return QueryResult()
        if sql.startswith("SELECT pipeline_identity_sha256"):
            requested = SHA256.findall(sql.split(" IN (", maxsplit=1)[1])
            rows = tuple(
                (identity, self.payload_by_identity[identity])
                for identity in requested
                if identity in self.payload_by_identity
            )
            return QueryResult(
                columns=("pipeline_identity_sha256", "pipeline_payload_sha256"),
                rows=rows,
            )
        if sql.startswith("MERGE INTO"):
            assert "WHEN MATCHED" not in sql
            assert "WHEN NOT MATCHED THEN INSERT" in sql
            pairs = FINAL_HASH_PAIR.findall(sql)
            assert pairs, "test runner could not locate loader identity/hash metadata"
            self.merge_calls += 1
            inserted = 0
            for identity, payload in pairs:
                if identity not in self.payload_by_identity:
                    self.payload_by_identity[identity] = payload
                    inserted += 1
            if self.fail_once_after_merge_commit:
                self.fail_once_after_merge_commit = False
                raise TrinoQueryError("simulated lost response after committed snapshot")
            return QueryResult(update_type="MERGE", update_count=inserted)
        raise AssertionError(f"unexpected SQL in loader unit test: {sql[:100]}")


def source_rows(dataset: str = "customer_master") -> list[dict]:
    records, _ = build_bundle(
        date(2026, 8, 27),
        date(2026, 8, 27),
        20260828,
        GENERATION_TIME,
    )
    return records[dataset]


def accepted(payload: dict, line_number: int) -> AcceptedRecord:
    return AcceptedRecord(
        payload=payload,
        pipeline_run_id="batch-loader-unit-test",
        evidence_envelope_id="EVIDENCE-loader-unit-test",
        ingested_at_utc="2026-12-31T12:30:00Z",
        raw_object_uri="r2://raw-test/customer_master.jsonl",
        raw_object_sha256="0" * 64,
        raw_record_locator=f"line:{line_number}",
    )


def loader(runner: MemoryStatementRunner, *, chunk_size: int = 200) -> TrinoIcebergLoader:
    return TrinoIcebergLoader(
        IcebergLoaderConfig(
            trino_endpoint="http://trino:8080",
            catalog="r2",
            iceberg_schema="industrial_energy_loader_unit",
            chunk_size=chunk_size,
            verify_existing_table=False,
        ),
        statement_runner=runner,
    )


def test_plans_nine_typed_tables_from_contracts_without_querying_trino() -> None:
    class NoQueries:
        def execute(self, sql: str) -> QueryResult:
            raise AssertionError(f"planning unexpectedly called Trino: {sql}")

    planned = TrinoIcebergLoader(
        IcebergLoaderConfig(), statement_runner=NoQueries()
    ).plan_all_tables()

    assert tuple(plan.dataset for plan in planned) == SYNTHETIC_DATASETS
    assert IDENTITY_FIELDS == {
        dataset: SOURCE_REVISION_KEY_FIELDS[dataset] for dataset in SYNTHETIC_DATASETS
    }
    commitment = next(plan for plan in planned if plan.dataset == "commitment_schedule")
    commitment_types = {column.name: column.sql_type for column in commitment.columns}
    assert commitment_types["committed_mwh_th"] == "DECIMAL(20,6)"
    assert commitment_types["interval_start_utc"] == "TIMESTAMP(6) WITH TIME ZONE"
    assert commitment_types["source_revision"] == "BIGINT"
    assert commitment_types["synthetic_data"] == "BOOLEAN"
    assert commitment_types["pipeline_payload_sha256"] == "VARCHAR"
    assert "format_version = 2" in commitment.create_table_sql


def test_exact_replay_is_skipped_without_a_second_merge() -> None:
    runner = MemoryStatementRunner()
    iceberg = loader(runner)
    record = accepted(source_rows()[0], 1)

    first = iceberg.load_records("customer_master", [record])
    replay = iceberg.load_records("customer_master", [record])

    assert first.inserted_records == 1
    assert first.skipped_exact_replays == 0
    assert replay.inserted_records == 0
    assert replay.skipped_exact_replays == 1
    assert replay.conflict_records == 0
    assert runner.merge_calls == 1
    assert len(runner.payload_by_identity) == 1


def test_conflicting_payload_is_reported_and_never_updates_iceberg() -> None:
    runner = MemoryStatementRunner()
    iceberg = loader(runner)
    original_payload = source_rows()[0]
    iceberg.load_records("customer_master", [accepted(original_payload, 1)])
    stored_before = dict(runner.payload_by_identity)
    merges_before = runner.merge_calls
    conflicting_payload = deepcopy(original_payload)
    conflicting_payload["display_name"] = "Structurally valid conflicting name"

    result = iceberg.load_records(
        "customer_master", [accepted(conflicting_payload, 1)]
    )

    assert result.inserted_records == 0
    assert result.skipped_exact_replays == 0
    assert result.conflict_records == 1
    assert result.conflict_identities == 1
    assert not result.succeeded_without_conflicts
    assert result.conflicts[0].existing_payload_sha256
    assert result.conflicts[0].incoming_payload_sha256
    assert runner.merge_calls == merges_before
    assert runner.payload_by_identity == stored_before


def test_chunk_retry_converges_after_response_is_lost_post_commit() -> None:
    runner = MemoryStatementRunner(fail_once_after_merge_commit=True)
    iceberg = loader(runner, chunk_size=2)
    records = [accepted(row, index) for index, row in enumerate(source_rows()[:3], 1)]

    with pytest.raises(TrinoQueryError, match="lost response"):
        iceberg.load_records("customer_master", records)

    assert len(runner.payload_by_identity) == 2
    retry = iceberg.load_records("customer_master", records)

    assert retry.input_records == 3
    assert retry.inserted_records == 1
    assert retry.skipped_exact_replays == 2
    assert retry.conflict_records == 0
    assert retry.chunks_processed == 2
    assert len(runner.payload_by_identity) == 3


def test_http_client_cancels_a_query_that_exceeds_its_overall_deadline(
    monkeypatch,
) -> None:
    client = TrinoHttpClient(
        "http://trino:8080",
        timeout_seconds=60,
        query_timeout_seconds=1,
    )
    monotonic_values = iter((0.0, 2.0))
    monkeypatch.setattr(
        loader_module.time,
        "monotonic",
        lambda: next(monotonic_values),
    )
    requests: list[tuple[str, str]] = []
    cancelled: list[str] = []

    def request(url: str, *, method: str, **kwargs):
        requests.append((method, url))
        return {"nextUri": "http://trino:8080/v1/statement/query/1"}

    monkeypatch.setattr(client, "_request", request)
    monkeypatch.setattr(client, "_cancel", cancelled.append)

    with pytest.raises(TrinoProtocolError, match="exceeded 1 seconds"):
        client.execute("SELECT 1")

    assert requests == [("POST", "http://trino:8080/v1/statement")]
    assert cancelled == ["http://trino:8080/v1/statement/query/1"]


def test_http_client_cancels_next_uri_after_a_socket_read_timeout(
    monkeypatch,
) -> None:
    methods: list[str] = []

    class Response:
        def __init__(self, method: str):
            self.method = method

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            if self.method == "POST":
                return json.dumps(
                    {"nextUri": "http://trino:8080/v1/statement/query/2"}
                ).encode()
            if self.method == "GET":
                raise TimeoutError("simulated socket read timeout")
            return b""

    def urlopen(request, *, timeout):
        del timeout
        method = request.get_method()
        methods.append(method)
        return Response(method)

    monkeypatch.setattr(loader_module.urllib.request, "urlopen", urlopen)
    client = TrinoHttpClient(
        "http://trino:8080",
        query_timeout_seconds=300,
    )

    with pytest.raises(TrinoProtocolError, match="timed out"):
        client.execute("SELECT 1")

    assert methods == ["POST", "GET", "DELETE"]
    assert client.headers["X-Trino-Session"] == "query_max_execution_time=300s"
