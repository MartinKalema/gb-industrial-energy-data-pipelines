from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

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
    PublisherConfig,
)
from ingestion.batch.pipeline.clickhouse_retention import (
    ClickHouseRetentionStore,
    ServingRetentionError,
    ServingRetentionManager,
)
from ingestion.batch.pipeline.models import PipelineError
from ingestion.batch.pipeline.workflow import remove_old_clickhouse_serving_versions


def _config() -> PublisherConfig:
    return PublisherConfig(
        trino_endpoint="http://trino:8080",
        clickhouse_host="clickhouse",
        clickhouse_user="publisher",
        clickhouse_password="test-only-password",
    )


def _attempt(character: str) -> str:
    return "publication-" + character * 32


class FakeRetentionStore:
    def __init__(
        self,
        *,
        ready_ids: Sequence[str],
        candidate_ids: Sequence[str],
    ) -> None:
        self.ready_ids = list(ready_ids)
        self.candidate_ids = set(candidate_ids)
        self.events: list[str] = []
        self.leave_marker_behind = False
        self.leave_candidate_behind = False

    def ensure_schema(self) -> None:
        self.events.append("ensure_schema")

    def list_ready_publication_ids(self) -> list[str]:
        self.events.append("list_ready")
        return list(self.ready_ids)

    def list_candidate_attempt_ids(self) -> set[str]:
        self.events.append("list_candidates")
        return set(self.candidate_ids)

    def delete_ready_publication_markers(
        self, publication_ids: Sequence[str]
    ) -> None:
        self.events.append("delete_markers:" + ",".join(publication_ids))
        if not self.leave_marker_behind:
            self.ready_ids = [
                value for value in self.ready_ids if value not in publication_ids
            ]

    def delete_candidate_rows(self, attempt_ids: Sequence[str]) -> None:
        self.events.append("delete_candidates:" + ",".join(attempt_ids))
        if not self.leave_candidate_behind:
            self.candidate_ids.difference_update(attempt_ids)


def test_plan_keeps_newest_two_and_identifies_old_and_invisible_rows() -> None:
    newest, previous, old_one, old_two, failed = (
        _attempt("a"),
        _attempt("b"),
        _attempt("c"),
        _attempt("d"),
        _attempt("e"),
    )
    store = FakeRetentionStore(
        ready_ids=[newest, previous, old_one, old_two],
        candidate_ids=[newest, previous, old_one, old_two, failed],
    )

    plan = ServingRetentionManager(_config(), store=store).plan()

    assert plan.retained_ready_publication_ids == (newest, previous)
    assert plan.retired_ready_publication_ids == (old_one, old_two)
    assert plan.invisible_attempt_ids == (failed,)
    assert plan.candidate_attempt_ids_to_delete == tuple(
        sorted((old_one, old_two, failed))
    )
    assert store.events == ["list_ready", "list_candidates"]


def test_dry_run_never_deletes_rows() -> None:
    newest, previous, old, failed = (
        _attempt("1"),
        _attempt("2"),
        _attempt("3"),
        _attempt("4"),
    )
    store = FakeRetentionStore(
        ready_ids=[newest, previous, old],
        candidate_ids=[newest, previous, old, failed],
    )

    result = ServingRetentionManager(_config(), store=store).cleanup()

    assert result["disposition"] == "planned"
    assert result["retained_ready_publication_ids"] == [newest, previous]
    assert result["retired_ready_publication_count"] == 1
    assert result["invisible_attempt_count"] == 1
    assert "ensure_schema" not in store.events
    assert not any(event.startswith("delete_") for event in store.events)


def test_applied_cleanup_requires_exclusive_writer_lock_confirmation() -> None:
    store = FakeRetentionStore(
        ready_ids=[_attempt("1"), _attempt("2")],
        candidate_ids=[_attempt("1"), _attempt("2"), _attempt("3")],
    )

    with pytest.raises(ServingRetentionError, match="exclusive.*lock"):
        ServingRetentionManager(_config(), store=store).cleanup(apply=True)

    assert store.events == []


def test_applied_cleanup_removes_marker_first_then_candidate_rows() -> None:
    newest, previous, old, failed = (
        _attempt("1"),
        _attempt("2"),
        _attempt("3"),
        _attempt("4"),
    )
    store = FakeRetentionStore(
        ready_ids=[newest, previous, old],
        candidate_ids=[newest, previous, old, failed],
    )

    result = ServingRetentionManager(_config(), store=store).cleanup(
        apply=True,
        exclusive_writer_lock_confirmed=True,
    )

    assert result["disposition"] == "removed"
    assert result["removed_ready_publication_count"] == 1
    assert result["removed_candidate_attempt_count"] == 2
    assert store.events[0] == "ensure_schema"
    assert store.ready_ids == [newest, previous]
    assert store.candidate_ids == {newest, previous}
    marker_event = next(
        index
        for index, event in enumerate(store.events)
        if event.startswith("delete_markers:")
    )
    candidate_event = next(
        index
        for index, event in enumerate(store.events)
        if event.startswith("delete_candidates:")
    )
    assert marker_event < candidate_event


def test_cleanup_stops_before_rows_if_marker_removal_is_not_confirmed() -> None:
    newest, previous, old = _attempt("1"), _attempt("2"), _attempt("3")
    store = FakeRetentionStore(
        ready_ids=[newest, previous, old],
        candidate_ids=[newest, previous, old],
    )
    store.leave_marker_behind = True

    with pytest.raises(ServingRetentionError, match="rows were left untouched"):
        ServingRetentionManager(_config(), store=store).cleanup(
            apply=True,
            exclusive_writer_lock_confirmed=True,
        )

    assert not any(
        event.startswith("delete_candidates:") for event in store.events
    )
    assert store.candidate_ids == {newest, previous, old}


def test_cleanup_after_partial_row_removal_is_safe_to_retry() -> None:
    newest, previous, old = _attempt("1"), _attempt("2"), _attempt("3")
    store = FakeRetentionStore(
        ready_ids=[newest, previous, old],
        candidate_ids=[newest, previous, old],
    )
    manager = ServingRetentionManager(_config(), store=store)
    store.leave_candidate_behind = True

    with pytest.raises(ServingRetentionError, match="safe to retry"):
        manager.cleanup(apply=True, exclusive_writer_lock_confirmed=True)

    assert store.ready_ids == [newest, previous]
    assert old in store.candidate_ids
    store.leave_candidate_behind = False

    result = manager.cleanup(apply=True, exclusive_writer_lock_confirmed=True)

    assert result["disposition"] == "removed"
    assert result["retired_ready_publication_count"] == 0
    assert result["invisible_attempt_count"] == 1
    assert store.candidate_ids == {newest, previous}


def test_protected_publication_is_checked_before_any_mutation() -> None:
    newest, previous, old = _attempt("1"), _attempt("2"), _attempt("3")
    store = FakeRetentionStore(
        ready_ids=[newest, previous, old],
        candidate_ids=[newest, previous, old],
    )

    with pytest.raises(ServingRetentionError, match="protected publication"):
        ServingRetentionManager(_config(), store=store).cleanup(
            apply=True,
            exclusive_writer_lock_confirmed=True,
            protected_ready_publication_ids=(old,),
        )

    assert not any(event.startswith("delete_") for event in store.events)


@pytest.mark.parametrize("keep_ready_versions", [0, 1, True, 2.5])
def test_cleanup_never_accepts_less_than_two_ready_versions(
    keep_ready_versions: Any,
) -> None:
    store = FakeRetentionStore(ready_ids=[], candidate_ids=[])

    with pytest.raises(ServingRetentionError, match="at least 2|must be an integer"):
        ServingRetentionManager(_config(), store=store).cleanup(
            keep_ready_versions=keep_ready_versions,
        )


def test_unexpected_attempt_id_fails_closed_before_delete() -> None:
    store = FakeRetentionStore(
        ready_ids=[_attempt("1"), _attempt("2")],
        candidate_ids=[_attempt("1"), _attempt("2"), "unsafe-id' OR 1=1"],
    )

    with pytest.raises(ServingRetentionError, match="not a supported"):
        ServingRetentionManager(_config(), store=store).cleanup(
            apply=True,
            exclusive_writer_lock_confirmed=True,
        )

    assert not any(event.startswith("delete_") for event in store.events)


class RecordingRetentionManager:
    def __init__(self, retained_ids: Sequence[str]) -> None:
        self.retained_ids = list(retained_ids)
        self.arguments: dict[str, Any] = {}

    def cleanup(self, **arguments: Any) -> dict[str, Any]:
        self.arguments = arguments
        return {
            "disposition": "removed",
            "retained_ready_publication_ids": list(self.retained_ids),
            "ready_publication_count": 3,
            "candidate_attempt_count": 4,
            "retired_ready_publication_count": 1,
            "invisible_attempt_count": 1,
        }


def test_workflow_cleanup_applies_policy_and_protects_trigger_publication() -> None:
    publication_id = _attempt("9")
    previous_id = _attempt("8")
    manager = RecordingRetentionManager([publication_id, previous_id])

    result = remove_old_clickhouse_serving_versions(
        {
            "publication_id": publication_id,
            "disposition": "created",
        },
        environment={"CLICKHOUSE_READY_VERSIONS_TO_KEEP": "3"},
        manager=manager,
    )

    assert result["trigger_publication_id"] == publication_id
    assert manager.arguments == {
        "keep_ready_versions": 3,
        "apply": True,
        "exclusive_writer_lock_confirmed": True,
        "protected_ready_publication_ids": (publication_id,),
    }


@pytest.mark.parametrize(
    "publication_result",
    [
        {},
        {"publication_id": "unsafe", "disposition": "created"},
        {"publication_id": _attempt("1"), "disposition": "failed"},
    ],
)
def test_workflow_cleanup_rejects_untrusted_publication_result(
    publication_result: dict[str, Any],
) -> None:
    manager = RecordingRetentionManager([])

    with pytest.raises(PipelineError, match="publication"):
        remove_old_clickhouse_serving_versions(
            publication_result,
            environment={},
            manager=manager,
        )

    assert manager.arguments == {}


@pytest.mark.parametrize("keep_value", ["1", "zero"])
def test_workflow_cleanup_rejects_unsafe_keep_count(keep_value: str) -> None:
    publication_id = _attempt("1")
    manager = RecordingRetentionManager([publication_id])

    with pytest.raises(PipelineError, match="CLICKHOUSE_READY_VERSIONS_TO_KEEP"):
        remove_old_clickhouse_serving_versions(
            {"publication_id": publication_id, "disposition": "reused"},
            environment={"CLICKHOUSE_READY_VERSIONS_TO_KEEP": keep_value},
            manager=manager,
        )

    assert manager.arguments == {}


def test_workflow_cleanup_requires_proof_trigger_publication_was_retained() -> None:
    publication_id = _attempt("1")
    manager = RecordingRetentionManager([_attempt("2"), _attempt("3")])

    with pytest.raises(PipelineError, match="did not prove"):
        remove_old_clickhouse_serving_versions(
            {"publication_id": publication_id, "disposition": "created"},
            environment={},
            manager=manager,
        )


class RecordingClickHouseClient:
    def __init__(self) -> None:
        self.executed: list[tuple[str, str | None]] = []
        self.queried: list[str] = []
        self.ready_ids = [_attempt("1"), _attempt("2"), _attempt("3")]
        self.current_ids = set(self.ready_ids) | {_attempt("4")}
        self.history_ids = set(self.ready_ids)
        self.change_summary_ids = set(self.ready_ids) | {_attempt("5")}

    def execute(self, sql: str, *, database: str | None = None) -> None:
        self.executed.append((sql, database))

    def query_json_rows(self, sql: str, *, database: str) -> list[dict[str, Any]]:
        assert database == "industrial_energy_serving"
        self.queried.append(sql)
        if f"FROM `industrial_energy_serving`.`{PUBLICATION_TABLE}`" in sql:
            return [{"publication_id": value} for value in self.ready_ids]
        if f"FROM `industrial_energy_serving`.`{CURRENT_TABLE}`" in sql:
            return [{"load_attempt_id": value} for value in sorted(self.current_ids)]
        if f"FROM `industrial_energy_serving`.`{HISTORY_TABLE}`" in sql:
            return [{"load_attempt_id": value} for value in sorted(self.history_ids)]
        if f"FROM `industrial_energy_serving`.`{CHANGE_SUMMARY_TABLE}`" in sql:
            return [
                {"load_attempt_id": value}
                for value in sorted(self.change_summary_ids)
            ]
        raise AssertionError(f"Unexpected retention query: {sql}")


def test_clickhouse_store_mutations_are_synchronous_and_serving_only() -> None:
    client = RecordingClickHouseClient()
    store = ClickHouseRetentionStore(_config(), client=client)  # type: ignore[arg-type]

    assert store.list_ready_publication_ids() == client.ready_ids
    assert store.list_candidate_attempt_ids() == (
        client.current_ids | client.history_ids | client.change_summary_ids
    )
    store.delete_ready_publication_markers([_attempt("3")])
    store.delete_candidate_rows([_attempt("3"), _attempt("4"), _attempt("5")])

    mutation_sql = "\n".join(sql for sql, _database in client.executed)
    assert f"`industrial_energy_serving`.`{PUBLICATION_TABLE}`" in mutation_sql
    assert f"`industrial_energy_serving`.`{CURRENT_TABLE}`" in mutation_sql
    assert f"`industrial_energy_serving`.`{HISTORY_TABLE}`" in mutation_sql
    assert f"`industrial_energy_serving`.`{CHANGE_SUMMARY_TABLE}`" in mutation_sql
    assert mutation_sql.count("SETTINGS mutations_sync = 2") == 4
    assert PUBLICATION_TABLE in client.executed[0][0]
    assert all(
        table_name in client.executed[index][0]
        for index, table_name in enumerate(
            (CURRENT_TABLE, HISTORY_TABLE, CHANGE_SUMMARY_TABLE), start=1
        )
    )
    assert "r2." not in mutation_sql.lower()
    assert "iceberg" not in mutation_sql.lower()
    assert all(
        database == "industrial_energy_serving"
        for _sql, database in client.executed
    )


def test_publisher_role_has_only_the_required_retention_delete_grant() -> None:
    repository_root = Path(__file__).resolve().parents[2]
    bootstrap_sql = (
        repository_root
        / "infrastructure"
        / "clickhouse"
        / "initdb.d"
        / "01-create-read-only-api-user.sh"
    ).read_text()

    assert "GRANT CREATE TABLE, SELECT, INSERT, ALTER DELETE" in bootstrap_sql
    assert "ON industrial_energy_serving.* TO industrial_energy_publisher" in (
        bootstrap_sql
    )
    assert "GRANT DROP" not in bootstrap_sql
    assert "GRANT TRUNCATE" not in bootstrap_sql


def test_retention_store_schema_contract_is_the_publisher_contract() -> None:
    class SchemaClient(RecordingClickHouseClient):
        def __init__(self) -> None:
            super().__init__()
            self.columns = {
                CURRENT_TABLE: [
                    ("load_attempt_id", "String"),
                    *[
                        (field.name, field.clickhouse_type)
                        for field in CURRENT_DATASET.fields
                    ],
                ],
                HISTORY_TABLE: [
                    ("load_attempt_id", "String"),
                    *[
                        (field.name, field.clickhouse_type)
                        for field in HISTORY_DATASET.fields
                    ],
                ],
                CHANGE_SUMMARY_TABLE: list(CHANGE_SUMMARY_COLUMNS),
                PUBLICATION_TABLE: list(PUBLICATION_COLUMNS),
            }
            self.sorting_keys = {
                CURRENT_TABLE: CURRENT_SORTING_KEY,
                HISTORY_TABLE: HISTORY_SORTING_KEY,
                CHANGE_SUMMARY_TABLE: CHANGE_SUMMARY_SORTING_KEY,
                PUBLICATION_TABLE: PUBLICATION_SORTING_KEY,
            }

        def query_json_rows(
            self, sql: str, *, database: str
        ) -> list[dict[str, Any]]:
            if "DESCRIBE TABLE" in sql:
                table = next(name for name in self.columns if f"`{name}`" in sql)
                return [
                    {"name": name, "type": column_type}
                    for name, column_type in self.columns[table]
                ]
            if "FROM system.tables" in sql:
                table = next(
                    name for name in self.sorting_keys if f"name = '{name}'" in sql
                )
                return [
                    {"engine": "MergeTree", "sorting_key": self.sorting_keys[table]}
                ]
            return super().query_json_rows(sql, database=database)

    client = SchemaClient()

    ClickHouseRetentionStore(_config(), client=client).ensure_schema()  # type: ignore[arg-type]

    assert len(client.executed) == 4
