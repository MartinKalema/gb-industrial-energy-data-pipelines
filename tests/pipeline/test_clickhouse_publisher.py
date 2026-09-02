from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Self

import pytest

from ingestion.batch.pipeline.clickhouse_publisher import (
    CHANGE_SUMMARY_COLUMNS,
    CHANGE_SUMMARY_SORTING_KEY,
    CHANGE_SUMMARY_TABLE,
    CURRENT_DATASET,
    CURRENT_SORTING_KEY,
    CURRENT_TABLE,
    HISTORY_DATASET,
    HISTORY_SORTING_KEY,
    HISTORY_TABLE,
    PUBLICATION_COLUMNS,
    PUBLICATION_SORTING_KEY,
    PUBLICATION_TABLE,
    ClickHouseHttpClient,
    ClickHouseServingStore,
    DatasetChangeSummary,
    DatasetSpec,
    PublicationRecord,
    PublisherConfig,
    ServingPublicationError,
    ServingPublisher,
    dbt_result_identity,
    publisher_config_from_environment,
)
from ingestion.batch.pipeline.models import PipelineError
from ingestion.batch.pipeline.workflow import (
    plan_run,
    publish_tested_dimensional_mart_to_clickhouse,
)


def _config() -> PublisherConfig:
    return PublisherConfig(
        trino_endpoint="http://trino:8080",
        clickhouse_host="clickhouse",
        clickhouse_user="publisher",
        clickhouse_password="test-only-password",
    )


def test_publisher_config_builder_applies_shared_environment_contract() -> None:
    config = publisher_config_from_environment(
        {
            "TRINO_URL": "http://ignored-trino:8080",
            "TRINO_USER": "configured-trino-user",
            "DBT_TRINO_CATALOG": "fallback_catalog",
            "CLICKHOUSE_SOURCE_TRINO_CATALOG": "source_catalog",
            "CLICKHOUSE_SOURCE_TRINO_SCHEMA": "source_schema",
            "CLICKHOUSE_HOST": "serving-clickhouse",
            "CLICKHOUSE_PORT": "8443",
            "CLICKHOUSE_DATABASE": "serving_database",
            "CLICKHOUSE_PUBLISHER_USER": "publisher",
            "CLICKHOUSE_PUBLISHER_PASSWORD": "test-only-password",
            "CLICKHOUSE_SECURE": "true",
            "CLICKHOUSE_INSERT_BATCH_SIZE": "250",
            "TRINO_HTTP_TIMEOUT_SECONDS": "12.5",
            "TRINO_QUERY_TIMEOUT_SECONDS": "45",
            "CLICKHOUSE_HTTP_TIMEOUT_SECONDS": "13.5",
            "CLICKHOUSE_QUERY_TIMEOUT_SECONDS": "46",
        },
        trino_endpoint="http://bounded-plan-trino:8080",
        default_trino_catalog="plan_catalog",
    )

    assert config.trino_endpoint == "http://bounded-plan-trino:8080"
    assert config.trino_catalog == "source_catalog"
    assert config.trino_schema == "source_schema"
    assert config.trino_user == "configured-trino-user"
    assert config.trino_timeout_seconds == 12.5
    assert config.trino_query_timeout_seconds == 45
    assert config.clickhouse_host == "serving-clickhouse"
    assert config.clickhouse_port == 8443
    assert config.clickhouse_database == "serving_database"
    assert config.clickhouse_user == "publisher"
    assert config.clickhouse_password == "test-only-password"
    assert config.clickhouse_secure is True
    assert config.clickhouse_timeout_seconds == 13.5
    assert config.clickhouse_query_timeout_seconds == 46
    assert config.insert_batch_size == 250


@pytest.mark.parametrize(
    ("environment_update", "message"),
    [
        ({}, "CLICKHOUSE_PUBLISHER_USER"),
        ({"CLICKHOUSE_PUBLISHER_USER": "publisher"}, "CLICKHOUSE_PUBLISHER_PASSWORD"),
        (
            {
                "CLICKHOUSE_PUBLISHER_USER": "publisher",
                "CLICKHOUSE_PUBLISHER_PASSWORD": "password",
                "CLICKHOUSE_SECURE": "sometimes",
            },
            "CLICKHOUSE_SECURE",
        ),
        (
            {
                "CLICKHOUSE_PUBLISHER_USER": "publisher",
                "CLICKHOUSE_PUBLISHER_PASSWORD": "password",
                "CLICKHOUSE_PORT": "not-a-number",
            },
            "CLICKHOUSE_PORT",
        ),
    ],
)
def test_publisher_config_builder_fails_closed_on_invalid_environment(
    environment_update: dict[str, str], message: str
) -> None:
    with pytest.raises(ServingPublicationError, match=message):
        publisher_config_from_environment(environment_update)


def _row(dataset: DatasetSpec, number: int) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for field in dataset.fields:
        if field.name == dataset.key_name:
            value: Any = f"{number:064x}"
        elif field.name == "interval_key":
            value = f"{number + 10:064x}"
        elif field.name == "tenant_authorization_scope_id":
            value = "tenant-customer-001"
        elif field.name == "customer_access_status":
            value = "authorized"
        elif field.name == "reporting_date":
            value = "2026-08-26"
        elif field.name == "date_key":
            value = 20260826
        elif field.name == "local_period_number":
            value = number
        elif field.name in {"interval_start_at", "known_from_at"}:
            value = f"2026-08-26T0{number - 1}:00:00.000000Z"
        elif field.name == "interval_end_at":
            value = f"2026-08-26T0{number - 1}:30:00.000000Z"
        elif field.name == "known_to_at":
            value = None
        elif field.name == "latest_coverage_published_at_utc":
            value = "2026-08-28T12:00:00.000000Z"
        elif field.name == "interval_start_local":
            value = f"2026-08-26T0{number}:00:00.000000"
        elif field.name == "interval_end_local":
            value = f"2026-08-26T0{number}:30:00.000000"
        elif field.name == "operating_timezone":
            value = "Europe/London"
        elif field.name == "utc_offset_minutes":
            value = 60
        elif (
            field.name
            in {
                "applicable_interval_count",
                "accepted_applicable_delivery_count",
                "final_applicable_capacity_count",
            }
            and number == 1
        ):
            value = None
        elif field.value_kind == "boolean":
            value = True
        elif field.value_kind == "decimal":
            # Exercise both governed null preservation and exact fixed scale.
            value = None if field.name == "deliverable_capacity_mwh_th" else "1.25"
        elif field.value_kind == "integer":
            value = 1
        elif field.name == "currency_code":
            value = "GBP"
        elif field.name == "delivery_measurement_status":
            value = "accepted"
        elif field.name == "commitment_status":
            value = "committed"
        elif field.name == "capacity_status" or field.name in {
            "sla_result_status",
            "availability_result_status",
            "financial_result_status",
        }:
            value = "final"
        elif field.name == "correction_status":
            value = "original"
        else:
            value = f"{field.name}-{number}"
        values[field.name] = value
    return values


class FakeMartReader:
    def __init__(self) -> None:
        self.rows = {
            CURRENT_TABLE: [_row(CURRENT_DATASET, 1), _row(CURRENT_DATASET, 2)],
            HISTORY_TABLE: [_row(HISTORY_DATASET, 1), _row(HISTORY_DATASET, 2)],
        }
        self.reads: list[str] = []

    def read_dataset(self, dataset: DatasetSpec) -> list[dict[str, Any]]:
        self.reads.append(dataset.table_name)
        return deepcopy(self.rows[dataset.table_name])


class FakeServingStore:
    def __init__(self) -> None:
        self.schema_ensured = False
        self.candidates: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self.publications: list[PublicationRecord] = []
        self.change_summaries: dict[str, list[DatasetChangeSummary]] = {}
        self.events: list[str] = []
        self.fail_table: str | None = None
        self.corrupt_table: str | None = None
        self.fail_change_summary = False
        self.return_clickhouse_timestamps = False

    def ensure_schema(self) -> None:
        self.schema_ensured = True
        self.events.append("ensure_schema")

    def find_ready_publication(
        self, source_fingerprint_sha256: str
    ) -> PublicationRecord | None:
        self.events.append("find_publication")
        return next(
            (
                marker
                for marker in reversed(self.publications)
                if marker.source_fingerprint_sha256 == source_fingerprint_sha256
                and marker.publication_status == "ready"
            ),
            None,
        )

    def find_latest_ready_publication(self) -> PublicationRecord | None:
        self.events.append("find_latest_publication")
        return self.publications[-1] if self.publications else None

    def clone_candidate_rows(
        self,
        dataset: DatasetSpec,
        *,
        base_publication_id: str,
        load_attempt_id: str,
    ) -> None:
        self.events.append(
            f"clone:{dataset.table_name}:{base_publication_id}:{load_attempt_id}"
        )
        self.candidates[(dataset.table_name, load_attempt_id)] = deepcopy(
            self.candidates[(dataset.table_name, base_publication_id)]
        )

    def delete_candidate_keys(
        self,
        dataset: DatasetSpec,
        *,
        load_attempt_id: str,
        keys: Sequence[str],
    ) -> None:
        self.events.append(
            f"delete:{dataset.table_name}:{load_attempt_id}:{len(keys)}"
        )
        key_set = set(keys)
        self.candidates[(dataset.table_name, load_attempt_id)] = [
            row
            for row in self.candidates[(dataset.table_name, load_attempt_id)]
            if row[dataset.key_name] not in key_set
        ]

    def insert_candidate_rows(
        self,
        dataset: DatasetSpec,
        load_attempt_id: str,
        rows: Sequence[Mapping[str, Any]],
    ) -> None:
        if not rows:
            return
        self.events.append(f"insert:{dataset.table_name}:{load_attempt_id}")
        if self.fail_table == dataset.table_name:
            raise RuntimeError("simulated partial insert failure")
        self.candidates.setdefault((dataset.table_name, load_attempt_id), []).extend(
            deepcopy([dict(row) for row in rows])
        )

    def read_candidate_rows(
        self, dataset: DatasetSpec, load_attempt_id: str
    ) -> list[dict[str, Any]]:
        self.events.append(f"read:{dataset.table_name}:{load_attempt_id}")
        rows = deepcopy(self.candidates[(dataset.table_name, load_attempt_id)])
        if self.corrupt_table == dataset.table_name and rows:
            rows[0][dataset.key_name] = "f" * 64
        if self.return_clickhouse_timestamps:
            for row in rows:
                for field in dataset.fields:
                    value = row[field.name]
                    if (
                        field.value_kind == "utc_datetime"
                        and isinstance(value, str)
                        and value.endswith("Z")
                    ):
                        row[field.name] = value[:-1].replace("T", " ")
        return rows

    def insert_change_summaries(
        self, summaries: Sequence[DatasetChangeSummary]
    ) -> None:
        attempt_id = summaries[0].load_attempt_id
        self.events.append(f"summary:{attempt_id}")
        if self.fail_change_summary:
            raise RuntimeError("simulated change-summary failure")
        self.change_summaries[attempt_id] = list(summaries)

    def read_change_summaries(
        self, load_attempt_id: str
    ) -> list[DatasetChangeSummary]:
        self.events.append(f"read_summary:{load_attempt_id}")
        return deepcopy(self.change_summaries.get(load_attempt_id, []))

    def insert_publication(self, publication: PublicationRecord) -> None:
        self.events.append(f"marker:{publication.publication_id}")
        self.publications.append(publication)


def _publisher(
    reader: FakeMartReader,
    store: FakeServingStore,
    attempts: list[str],
) -> ServingPublisher:
    attempt_iterator = iter(attempts)
    return ServingPublisher(
        _config(),
        mart_reader=reader,
        serving_store=store,
        clock=lambda: datetime(2026, 8, 29, 3, 0, tzinfo=UTC),
        attempt_id_factory=lambda: next(attempt_iterator),
    )


def _publish(publisher: ServingPublisher) -> dict[str, Any]:
    return _publish_with_dbt_hash(publisher, "b" * 64)


def _publish_with_dbt_hash(
    publisher: ServingPublisher, dbt_result_identity_sha256: str
) -> dict[str, Any]:
    return publisher.publish(
        pipeline_run_id="batch-20260826-20260826-0123456789abcdef",
        coverage_payload_sha256="a" * 64,
        dbt_result_identity_sha256=dbt_result_identity_sha256,
    )


def test_success_marks_candidate_last_and_returns_only_compact_metadata() -> None:
    reader = FakeMartReader()
    store = FakeServingStore()
    attempt = "publication-" + "1" * 32

    result = _publish(_publisher(reader, store, [attempt]))

    assert result["disposition"] == "created"
    assert result["publication_id"] == attempt
    assert result["publication_mode"] == "full"
    assert result["base_publication_id"] is None
    assert result["change_counts"][CURRENT_TABLE] == {
        "source_row_count": 2,
        "inserted_row_count": 2,
        "updated_row_count": 0,
        "deleted_row_count": 0,
        "unchanged_row_count": 0,
    }
    assert result["current_row_count"] == 2
    assert result["history_row_count"] == 2
    assert result["minimum_reporting_date"] == "2026-08-26"
    assert result["maximum_reporting_date"] == "2026-08-26"
    assert len(result["current_content_sha256"]) == 64
    assert len(result["history_content_sha256"]) == 64
    assert len(str(result).encode()) < 2_048
    assert store.events[-1] == f"marker:{attempt}"
    assert store.publications[0].publication_id == attempt
    current_candidate = store.candidates[(CURRENT_TABLE, attempt)]
    assert current_candidate[0]["deliverable_capacity_mwh_th"] is None
    assert current_candidate[0]["applicable_interval_count"] is None
    assert current_candidate[0]["committed_mwh_th"] == "1.250000"
    assert current_candidate[0]["interval_start_at"].endswith(".000000Z")


def test_incremental_publication_clones_base_and_applies_insert_update_delete() -> None:
    reader = FakeMartReader()
    reader.rows[CURRENT_TABLE].append(_row(CURRENT_DATASET, 3))
    store = FakeServingStore()
    base_attempt = "publication-" + "a" * 32
    next_attempt = "publication-" + "b" * 32
    publisher = _publisher(reader, store, [base_attempt, next_attempt])
    _publish_with_dbt_hash(publisher, "1" * 64)

    updated = deepcopy(reader.rows[CURRENT_TABLE][1])
    updated["customer_name"] = "Updated customer name"
    reader.rows[CURRENT_TABLE] = [
        reader.rows[CURRENT_TABLE][0],
        updated,
        _row(CURRENT_DATASET, 4),
    ]
    result = _publish_with_dbt_hash(publisher, "2" * 64)

    assert result["publication_mode"] == "incremental"
    assert result["base_publication_id"] == base_attempt
    assert result["change_counts"][CURRENT_TABLE] == {
        "source_row_count": 3,
        "inserted_row_count": 1,
        "updated_row_count": 1,
        "deleted_row_count": 1,
        "unchanged_row_count": 1,
    }
    assert result["change_counts"][HISTORY_TABLE] == {
        "source_row_count": 2,
        "inserted_row_count": 0,
        "updated_row_count": 0,
        "deleted_row_count": 0,
        "unchanged_row_count": 2,
    }
    assert store.candidates[(CURRENT_TABLE, base_attempt)][1][
        "customer_name"
    ] != "Updated customer name"
    next_rows = store.candidates[(CURRENT_TABLE, next_attempt)]
    assert {row[CURRENT_DATASET.key_name] for row in next_rows} == {
        row[CURRENT_DATASET.key_name] for row in reader.rows[CURRENT_TABLE]
    }
    assert next(
        row for row in next_rows if row[CURRENT_DATASET.key_name] == f"{2:064x}"
    )["customer_name"] == "Updated customer name"
    second_events = store.events[store.events.index("find_latest_publication", 1) :]
    assert second_events[-2:] == [f"summary:{next_attempt}", f"marker:{next_attempt}"]
    assert any(event.startswith(f"clone:{CURRENT_TABLE}:{base_attempt}") for event in second_events)
    assert f"delete:{CURRENT_TABLE}:{next_attempt}:2" in second_events
    assert f"insert:{CURRENT_TABLE}:{next_attempt}" in second_events


def test_zero_change_publication_clones_without_reinserting_source_rows() -> None:
    reader = FakeMartReader()
    store = FakeServingStore()
    base_attempt = "publication-" + "c" * 32
    next_attempt = "publication-" + "d" * 32
    publisher = _publisher(reader, store, [base_attempt, next_attempt])
    _publish_with_dbt_hash(publisher, "3" * 64)
    event_count = len(store.events)

    result = _publish_with_dbt_hash(publisher, "4" * 64)

    assert result["publication_mode"] == "incremental"
    assert result["base_publication_id"] == base_attempt
    assert all(
        counts["inserted_row_count"] == 0
        and counts["updated_row_count"] == 0
        and counts["deleted_row_count"] == 0
        and counts["unchanged_row_count"] == 2
        for counts in result["change_counts"].values()
    )
    new_events = store.events[event_count:]
    assert sum(event.startswith("clone:") for event in new_events) == 2
    assert sum(event.startswith("delete:") for event in new_events) == 2
    assert not any(event.startswith("insert:") for event in new_events)
    assert new_events[-2:] == [f"summary:{next_attempt}", f"marker:{next_attempt}"]


def test_clickhouse_timestamp_format_can_be_reused_as_incremental_base() -> None:
    reader = FakeMartReader()
    store = FakeServingStore()
    base_attempt = "publication-" + "1" * 32
    next_attempt = "publication-" + "2" * 32
    publisher = _publisher(reader, store, [base_attempt, next_attempt])
    _publish_with_dbt_hash(publisher, "5" * 64)
    store.return_clickhouse_timestamps = True

    result = _publish_with_dbt_hash(publisher, "6" * 64)

    assert result["publication_mode"] == "incremental"
    assert result["base_publication_id"] == base_attempt
    assert all(
        counts["unchanged_row_count"] == 2
        for counts in result["change_counts"].values()
    )


def test_partial_failure_has_no_marker_and_retry_uses_a_new_attempt() -> None:
    reader = FakeMartReader()
    store = FakeServingStore()
    first_attempt = "publication-" + "2" * 32
    second_attempt = "publication-" + "3" * 32
    publisher = _publisher(reader, store, [first_attempt, second_attempt])
    store.fail_table = HISTORY_TABLE

    with pytest.raises(RuntimeError, match="partial insert failure"):
        _publish(publisher)

    assert not store.publications
    assert (CURRENT_TABLE, first_attempt) in store.candidates
    store.fail_table = None

    result = _publish(publisher)

    assert result["publication_id"] == second_attempt
    assert store.publications[0].publication_id == second_attempt
    assert (CURRENT_TABLE, first_attempt) in store.candidates
    assert (CURRENT_TABLE, second_attempt) in store.candidates


def test_destination_validation_failure_never_inserts_marker() -> None:
    reader = FakeMartReader()
    store = FakeServingStore()
    store.corrupt_table = HISTORY_TABLE
    attempt = "publication-" + "4" * 32

    with pytest.raises(ServingPublicationError, match="candidate failed exact"):
        _publish(_publisher(reader, store, [attempt]))

    assert not store.publications
    assert (CURRENT_TABLE, attempt) in store.candidates
    assert (HISTORY_TABLE, attempt) in store.candidates


def test_change_summary_failure_leaves_validated_candidate_invisible() -> None:
    reader = FakeMartReader()
    store = FakeServingStore()
    store.fail_change_summary = True
    attempt = "publication-" + "4" * 32

    with pytest.raises(RuntimeError, match="change-summary failure"):
        _publish(_publisher(reader, store, [attempt]))

    assert not store.publications
    assert (CURRENT_TABLE, attempt) in store.candidates
    assert store.events[-1] == f"summary:{attempt}"


def test_exact_retry_reuses_ready_publication_without_new_candidate_rows() -> None:
    reader = FakeMartReader()
    store = FakeServingStore()
    first_attempt = "publication-" + "5" * 32
    publisher = _publisher(reader, store, [first_attempt])
    created = _publish(publisher)
    event_count = len(store.events)

    replayed = _publish(publisher)

    assert replayed == {**created, "disposition": "reused"}
    assert len(store.publications) == 1
    assert not any(event.startswith("insert:") for event in store.events[event_count:])
    assert store.events[event_count:] == [
        "ensure_schema",
        "find_publication",
        f"read:{CURRENT_TABLE}:{first_attempt}",
        f"read:{HISTORY_TABLE}:{first_attempt}",
        f"read_summary:{first_attempt}",
    ]


@pytest.mark.parametrize(
    "damage", ["missing", "corrupt", "coordinated_count", "timestamp"]
)
def test_exact_retry_fails_closed_on_invalid_change_summary(damage: str) -> None:
    reader = FakeMartReader()
    store = FakeServingStore()
    attempt = "publication-" + "e" * 32
    publisher = _publisher(reader, store, [attempt])
    _publish(publisher)
    if damage == "missing":
        store.change_summaries[attempt] = []
    elif damage == "corrupt":
        current_summary = store.change_summaries[attempt][0]
        store.change_summaries[attempt][0] = replace(
            current_summary,
            source_row_count=current_summary.source_row_count + 1,
        )
    elif damage == "coordinated_count":
        current_summary = store.change_summaries[attempt][0]
        store.change_summaries[attempt][0] = replace(
            current_summary,
            source_row_count=current_summary.source_row_count + 1,
            inserted_row_count=current_summary.inserted_row_count + 1,
        )
    else:
        current_summary = store.change_summaries[attempt][0]
        store.change_summaries[attempt][0] = replace(
            current_summary,
            recorded_at_utc="2020-01-01T00:00:00.000000Z",
        )

    with pytest.raises(ServingPublicationError, match="change summary"):
        _publish(publisher)

    assert len(store.publications) == 1


def test_damaged_ready_publication_is_replaced_with_a_fresh_valid_candidate() -> None:
    reader = FakeMartReader()
    store = FakeServingStore()
    first_attempt = "publication-" + "8" * 32
    replacement_attempt = "publication-" + "9" * 32
    publisher = _publisher(reader, store, [first_attempt, replacement_attempt])
    created = _publish(publisher)
    store.candidates[(HISTORY_TABLE, first_attempt)].pop()

    repaired = _publish(publisher)

    assert created["publication_id"] == first_attempt
    assert repaired["disposition"] == "created"
    assert repaired["publication_id"] == replacement_attempt
    assert repaired["publication_mode"] == "full"
    assert repaired["base_publication_id"] is None
    assert repaired["change_counts"][CURRENT_TABLE]["inserted_row_count"] == 2
    assert len(store.publications) == 2
    assert store.publications[-1].publication_id == replacement_attempt
    assert len(store.candidates[(HISTORY_TABLE, replacement_attempt)]) == 2


def test_missing_tenant_scope_is_rejected_before_any_candidate_insert() -> None:
    reader = FakeMartReader()
    reader.rows[CURRENT_TABLE][0]["tenant_authorization_scope_id"] = None
    store = FakeServingStore()

    with pytest.raises(ServingPublicationError, match="must not be null"):
        _publish(_publisher(reader, store, ["publication-" + "6" * 32]))

    assert not store.candidates
    assert not store.publications


class NoopClickHouseClient:
    pass


class RecordingClickHouseClient:
    def __init__(self) -> None:
        self.executed: list[tuple[str, str | None]] = []

    def execute(self, sql: str, *, database: str | None = None) -> None:
        self.executed.append((sql, database))


def test_incremental_store_clones_server_side_and_validates_delete_keys() -> None:
    client = RecordingClickHouseClient()
    store = ClickHouseServingStore(_config(), client=client)  # type: ignore[arg-type]
    base_attempt = "publication-" + "1" * 32
    next_attempt = "publication-" + "2" * 32
    safe_key = "a" * 64

    store.clone_candidate_rows(
        CURRENT_DATASET,
        base_publication_id=base_attempt,
        load_attempt_id=next_attempt,
    )
    store.delete_candidate_keys(
        CURRENT_DATASET,
        load_attempt_id=next_attempt,
        keys=[safe_key],
    )

    clone_sql, clone_database = client.executed[0]
    delete_sql, delete_database = client.executed[1]
    assert "INSERT INTO" in clone_sql and "SELECT" in clone_sql
    assert base_attempt in clone_sql and next_attempt in clone_sql
    assert "ALTER TABLE" in delete_sql and "DELETE WHERE" in delete_sql
    assert "mutations_sync = 2" in delete_sql
    assert safe_key in delete_sql
    assert clone_database == delete_database == "industrial_energy_serving"

    with pytest.raises(ServingPublicationError, match="invalid interval_key"):
        store.delete_candidate_keys(
            CURRENT_DATASET,
            load_attempt_id=next_attempt,
            keys=["' OR 1 = 1 --"],
        )
    assert len(client.executed) == 2


class SchemaContractClient:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.queried: list[str] = []
        self.columns = {
            CURRENT_TABLE: [("load_attempt_id", "String")]
            + [(field.name, field.clickhouse_type) for field in CURRENT_DATASET.fields],
            HISTORY_TABLE: [("load_attempt_id", "String")]
            + [(field.name, field.clickhouse_type) for field in HISTORY_DATASET.fields],
            PUBLICATION_TABLE: list(PUBLICATION_COLUMNS),
            CHANGE_SUMMARY_TABLE: list(CHANGE_SUMMARY_COLUMNS),
        }
        self.metadata = {
            CURRENT_TABLE: {
                "engine": "MergeTree",
                "sorting_key": CURRENT_SORTING_KEY,
            },
            HISTORY_TABLE: {
                "engine": "MergeTree",
                "sorting_key": HISTORY_SORTING_KEY,
            },
            PUBLICATION_TABLE: {
                "engine": "MergeTree",
                "sorting_key": PUBLICATION_SORTING_KEY,
            },
            CHANGE_SUMMARY_TABLE: {
                "engine": "MergeTree",
                "sorting_key": CHANGE_SUMMARY_SORTING_KEY,
            },
        }

    def execute(self, sql: str, *, database: str | None = None) -> None:
        assert database is None
        self.executed.append(sql)

    def query_json_rows(self, sql: str, *, database: str) -> list[dict[str, Any]]:
        assert database == "industrial_energy_serving"
        self.queried.append(sql)
        table_name = next(
            table for table in sorted(self.columns, key=len, reverse=True) if table in sql
        )
        if "DESCRIBE TABLE" in sql:
            return [
                {"name": name, "type": column_type}
                for name, column_type in self.columns[table_name]
            ]
        if "FROM system.tables" in sql:
            return [dict(self.metadata[table_name])]
        raise AssertionError(f"Unexpected schema query: {sql}")


def test_clickhouse_request_timeout_covers_the_server_query_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, float] = {}

    class Response:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_arguments: object) -> None:
            return None

        def read(self) -> bytes:
            return b""

    def fake_urlopen(_request: object, *, timeout: float) -> Response:
        observed["timeout"] = timeout
        return Response()

    monkeypatch.setattr(
        "ingestion.batch.pipeline.clickhouse_publisher.urllib.request.urlopen",
        fake_urlopen,
    )
    client = ClickHouseHttpClient(
        PublisherConfig(
            trino_endpoint="http://trino:8080",
            clickhouse_timeout_seconds=60,
            clickhouse_query_timeout_seconds=300,
        )
    )

    client.execute("SELECT 1")

    assert observed["timeout"] == 305


def test_serving_schema_is_marker_gated_and_uses_only_merge_tree_tables() -> None:
    store = ClickHouseServingStore(_config(), client=NoopClickHouseClient())  # type: ignore[arg-type]

    assert "load_attempt_id String" in store.current_table_sql
    assert "`tenant_authorization_scope_id` String" in store.current_table_sql
    assert "`sla_attainment_numerator_mwh_th` Nullable(Decimal(20, 6))" in (
        store.current_table_sql
    )
    assert "`expected_interval_count` UInt8" in store.current_table_sql
    assert "`applicable_interval_count` Nullable(UInt8)" in (store.current_table_sql)
    assert "`history_key` String" in store.history_table_sql
    assert "`known_from_at` DateTime64(6, 'UTC')" in store.history_table_sql
    assert "publication_id String" in store.publication_table_sql
    assert "source_fingerprint_sha256 FixedString(64)" in (store.publication_table_sql)
    assert "load_attempt_id String" in store.change_summary_table_sql
    assert "updated_row_count UInt64" in store.change_summary_table_sql
    for statement in (
        store.current_table_sql,
        store.history_table_sql,
        store.publication_table_sql,
        store.change_summary_table_sql,
    ):
        assert "ENGINE = MergeTree" in statement
        assert "ReplacingMergeTree" not in statement


def test_ensure_schema_verifies_exact_columns_engine_and_sorting_keys() -> None:
    client = SchemaContractClient()
    store = ClickHouseServingStore(_config(), client=client)  # type: ignore[arg-type]

    store.ensure_schema()

    assert len(client.executed) == 4
    assert len(client.queried) == 8
    assert sum("DESCRIBE TABLE" in query for query in client.queried) == 4
    assert sum("FROM system.tables" in query for query in client.queried) == 4


@pytest.mark.parametrize("mismatch", ["column_type", "engine", "sorting_key"])
def test_ensure_schema_rejects_incompatible_persistent_table(
    mismatch: str,
) -> None:
    client = SchemaContractClient()
    if mismatch == "column_type":
        client.columns[CURRENT_TABLE][0] = ("load_attempt_id", "Nullable(String)")
    else:
        client.metadata[CURRENT_TABLE][mismatch] = (
            "ReplacingMergeTree" if mismatch == "engine" else "load_attempt_id"
        )
    store = ClickHouseServingStore(_config(), client=client)  # type: ignore[arg-type]

    with pytest.raises(
        ServingPublicationError,
        match="controlled schema migration or rebuild the disposable serving database",
    ):
        store.ensure_schema()


class RecordingPublisher:
    def __init__(self) -> None:
        self.arguments: dict[str, Any] = {}

    def publish(self, **arguments: Any) -> dict[str, Any]:
        self.arguments = arguments
        return {
            "pipeline_run_id": arguments["pipeline_run_id"],
            "publication_id": "publication-" + "7" * 32,
            "disposition": "created",
            "source_fingerprint_sha256": "c" * 64,
            "coverage_payload_sha256": arguments["coverage_payload_sha256"],
            "dbt_result_identity_sha256": arguments["dbt_result_identity_sha256"],
            "current_row_count": 96,
            "history_row_count": 110,
            "current_content_sha256": "d" * 64,
            "history_content_sha256": "e" * 64,
            "minimum_reporting_date": "2026-08-26",
            "maximum_reporting_date": "2026-08-26",
            "published_at_utc": "2026-08-29T03:00:00.000000Z",
        }


def test_workflow_gates_publication_on_the_complete_dbt_test_identity(
    tmp_path: Path,
) -> None:
    plan = plan_run(
        start_date="2026-08-26",
        end_date="2026-08-26",
        seed=20260828,
        generation_time_utc="2026-08-28T12:00:00Z",
        orchestrator_run_id="manual__clickhouse-publisher-test",
        environment={
            "R2_RAW_BUCKET": "raw-test",
            "PIPELINE_WORK_ROOT": str(tmp_path / "work"),
            "TRINO_URL": "http://trino:8080",
        },
    )
    coverage = {
        "pipeline_run_id": plan["pipeline_run_id"],
        "coverage_payload_sha256": "a" * 64,
        "disposition": "reused",
    }
    dbt_result = {
        "pipeline_run_id": plan["pipeline_run_id"],
        "coverage_payload_sha256": "a" * 64,
        "dbt_step_name": "test_complete_dimensional_mart",
        "dbt_command_name": "test",
        "status": "succeeded",
        "test_result_count": 70,
        "model_result_count": 0,
        "result_count": 70,
        "attempt_number": 1,
        "dbt_invocation_id": "dbt-invocation-1",
        "dbt_version": "1.12.3",
        "generated_at_utc": "2026-08-29T02:00:00Z",
        "status_counts": {"pass": 70},
    }
    publisher = RecordingPublisher()

    result = publish_tested_dimensional_mart_to_clickhouse(
        plan,
        coverage,
        dbt_result,
        publisher=publisher,
    )

    assert result["publication_id"] == "publication-" + "7" * 32
    assert publisher.arguments == {
        "pipeline_run_id": plan["pipeline_run_id"],
        "coverage_payload_sha256": "a" * 64,
        "dbt_result_identity_sha256": dbt_result_identity(dbt_result),
    }
    assert (
        Path(plan["work_dir"]) / "clickhouse-dimensional-mart-publication-result.json"
    ).is_file()

    with pytest.raises(PipelineError, match="successful complete"):
        publish_tested_dimensional_mart_to_clickhouse(
            plan,
            coverage,
            {**dbt_result, "status": "failed"},
            publisher=publisher,
        )
