"""Conservative retention for the disposable ClickHouse serving copy.

R2-backed Iceberg remains canonical.  This module has no R2 or Trino write
boundary: it can only inspect and remove ClickHouse serving rows.  Cleanup
never deletes the newest two ready publications, removes older ready markers
before their rows, and removes candidate rows that have no ready marker.

An unmarked candidate can also be a publication that is still being written.
Callers must therefore hold the same exclusive writer lock used by the
publisher before applying a plan.  Airflow provides that boundary through the
one-slot ``iceberg_writer`` pool.  A dry run never requires the lock and never
mutates ClickHouse.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from .clickhouse_publisher import (
    CHANGE_SUMMARY_TABLE,
    CURRENT_TABLE,
    HISTORY_TABLE,
    PUBLICATION_TABLE,
    READY_STATUS,
    SAFE_ATTEMPT_ID,
    ClickHouseHttpClient,
    ClickHouseServingStore,
    PublisherConfig,
    ServingPublicationError,
)

MINIMUM_READY_VERSIONS = 2
DELETE_BATCH_SIZE = 100
CANDIDATE_TABLES = (CURRENT_TABLE, HISTORY_TABLE, CHANGE_SUMMARY_TABLE)


class ServingRetentionError(ServingPublicationError):
    """The serving-copy cleanup could not be planned or completed safely."""


class RetentionStore(Protocol):
    """The ClickHouse-only operations required by conservative cleanup."""

    def ensure_schema(self) -> None: ...

    def list_ready_publication_ids(self) -> list[str]: ...

    def list_candidate_attempt_ids(self) -> set[str]: ...

    def delete_ready_publication_markers(
        self, publication_ids: Sequence[str]
    ) -> None: ...

    def delete_candidate_rows(self, attempt_ids: Sequence[str]) -> None: ...


@dataclass(frozen=True, slots=True)
class RetentionPlan:
    """One deterministic snapshot of what serving cleanup would retain/remove."""

    retained_ready_publication_ids: tuple[str, ...]
    retired_ready_publication_ids: tuple[str, ...]
    invisible_attempt_ids: tuple[str, ...]
    all_ready_publication_count: int
    all_candidate_attempt_count: int

    @property
    def candidate_attempt_ids_to_delete(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                set(self.retired_ready_publication_ids)
                | set(self.invisible_attempt_ids)
            )
        )

    def summary(self, *, disposition: str) -> dict[str, Any]:
        deletion_ids = self.candidate_attempt_ids_to_delete
        return {
            "disposition": disposition,
            "minimum_ready_versions": MINIMUM_READY_VERSIONS,
            "retained_ready_publication_ids": list(
                self.retained_ready_publication_ids
            ),
            "ready_publication_count": self.all_ready_publication_count,
            "candidate_attempt_count": self.all_candidate_attempt_count,
            "retired_ready_publication_count": len(
                self.retired_ready_publication_ids
            ),
            "invisible_attempt_count": len(self.invisible_attempt_ids),
            "candidate_attempt_count_to_delete": len(deletion_ids),
            "deletion_set_sha256": _identity_set_sha256(deletion_ids),
        }


class ClickHouseRetentionStore:
    """Retention operations over the disposable ClickHouse serving tables only."""

    def __init__(
        self,
        config: PublisherConfig,
        *,
        client: ClickHouseHttpClient | None = None,
    ) -> None:
        config.validate()
        self._config = config
        self._client = client or ClickHouseHttpClient(config)
        self._schema_store = ClickHouseServingStore(config, client=self._client)

    def ensure_schema(self) -> None:
        self._schema_store.ensure_schema()

    def list_ready_publication_ids(self) -> list[str]:
        rows = self._client.query_json_rows(
            f"""
                SELECT publication_id
                FROM {self._qualified(PUBLICATION_TABLE)}
                WHERE publication_status = '{READY_STATUS}'
                GROUP BY publication_id
                ORDER BY max(published_at_utc) DESC, publication_id DESC
            """,
            database=self._config.clickhouse_database,
        )
        publication_ids = [str(row.get("publication_id", "")) for row in rows]
        _validate_attempt_ids(publication_ids, "ready publication")
        return publication_ids

    def list_candidate_attempt_ids(self) -> set[str]:
        attempt_ids: set[str] = set()
        for table_name in CANDIDATE_TABLES:
            rows = self._client.query_json_rows(
                f"""
                    SELECT DISTINCT load_attempt_id
                    FROM {self._qualified(table_name)}
                    ORDER BY load_attempt_id
                """,
                database=self._config.clickhouse_database,
            )
            table_ids = [str(row.get("load_attempt_id", "")) for row in rows]
            _validate_attempt_ids(table_ids, f"{table_name} candidate")
            attempt_ids.update(table_ids)
        return attempt_ids

    def delete_ready_publication_markers(
        self, publication_ids: Sequence[str]
    ) -> None:
        for batch in _attempt_id_batches(publication_ids):
            self._client.execute(
                f"""
                    ALTER TABLE {self._qualified(PUBLICATION_TABLE)}
                    DELETE WHERE publication_id IN ({_attempt_id_literals(batch)})
                    SETTINGS mutations_sync = 2
                """,
                database=self._config.clickhouse_database,
            )

    def delete_candidate_rows(self, attempt_ids: Sequence[str]) -> None:
        for batch in _attempt_id_batches(attempt_ids):
            literals = _attempt_id_literals(batch)
            for table_name in CANDIDATE_TABLES:
                self._client.execute(
                    f"""
                        ALTER TABLE {self._qualified(table_name)}
                        DELETE WHERE load_attempt_id IN ({literals})
                        SETTINGS mutations_sync = 2
                    """,
                    database=self._config.clickhouse_database,
                )

    def _qualified(self, table_name: str) -> str:
        # PublisherConfig validates the database identifier and every table name
        # here is a module constant, not caller input.
        return f"`{self._config.clickhouse_database}`.`{table_name}`"


class ServingRetentionManager:
    """Plan or apply retention without touching the canonical lakehouse."""

    def __init__(
        self,
        config: PublisherConfig,
        *,
        store: RetentionStore | None = None,
    ) -> None:
        config.validate()
        self._store = store or ClickHouseRetentionStore(config)

    def plan(self, *, keep_ready_versions: int = MINIMUM_READY_VERSIONS) -> RetentionPlan:
        _validate_keep_ready_versions(keep_ready_versions)
        ready_ids = self._store.list_ready_publication_ids()
        candidate_ids = self._store.list_candidate_attempt_ids()
        _validate_attempt_ids(ready_ids, "ready publication")
        _validate_attempt_ids(candidate_ids, "candidate")
        if len(ready_ids) != len(set(ready_ids)):
            raise ServingRetentionError(
                "ready publication listing contains duplicate publication IDs"
            )

        retained_ids = tuple(ready_ids[:keep_ready_versions])
        retired_ids = tuple(ready_ids[keep_ready_versions:])
        invisible_ids = tuple(sorted(candidate_ids - set(ready_ids)))
        return RetentionPlan(
            retained_ready_publication_ids=retained_ids,
            retired_ready_publication_ids=retired_ids,
            invisible_attempt_ids=invisible_ids,
            all_ready_publication_count=len(ready_ids),
            all_candidate_attempt_count=len(candidate_ids),
        )

    def cleanup(
        self,
        *,
        keep_ready_versions: int = MINIMUM_READY_VERSIONS,
        apply: bool = False,
        exclusive_writer_lock_confirmed: bool = False,
        protected_ready_publication_ids: Sequence[str] = (),
    ) -> dict[str, Any]:
        """Return a dry-run plan or apply it while the writer lock is held.

        The marker deletion happens first.  If later row deletion is interrupted,
        those rows are invisible and a retry will classify them as unmarked
        candidates.  The reverse order is forbidden because it could leave a
        ready marker pointing at partially removed rows.
        """

        _validate_keep_ready_versions(keep_ready_versions)
        _validate_attempt_ids(
            protected_ready_publication_ids, "protected ready publication"
        )
        if apply and not exclusive_writer_lock_confirmed:
            raise ServingRetentionError(
                "cleanup requires confirmation that the exclusive ClickHouse "
                "publisher/writer lock is held"
            )

        # Only applied cleanup may create missing serving tables. A dry-run plan
        # is strictly read-only and fails if the expected serving schema does
        # not already exist.
        if apply:
            self._store.ensure_schema()

        # For an applied cleanup the caller must acquire its lock before this
        # method starts, so the inventory cannot include an active writer.
        plan = self.plan(keep_ready_versions=keep_ready_versions)
        unprotected = set(protected_ready_publication_ids) - set(
            plan.retained_ready_publication_ids
        )
        if unprotected:
            raise ServingRetentionError(
                "a protected publication is missing or would be retired; cleanup "
                "was not applied"
            )
        if not apply:
            return plan.summary(disposition="planned")

        deletion_ids = plan.candidate_attempt_ids_to_delete
        if not deletion_ids:
            return plan.summary(disposition="nothing_to_remove")

        self._store.delete_ready_publication_markers(
            plan.retired_ready_publication_ids
        )
        remaining_ready = self._store.list_ready_publication_ids()
        if set(plan.retired_ready_publication_ids) & set(remaining_ready):
            raise ServingRetentionError(
                "ClickHouse did not remove every retired ready marker; candidate "
                "rows were left untouched"
            )
        if not set(plan.retained_ready_publication_ids).issubset(remaining_ready):
            raise ServingRetentionError(
                "a retained ready publication disappeared during cleanup; "
                "candidate rows were left untouched"
            )

        self._store.delete_candidate_rows(deletion_ids)
        remaining_candidates = self._store.list_candidate_attempt_ids()
        not_removed = set(deletion_ids) & remaining_candidates
        if not_removed:
            raise ServingRetentionError(
                "ClickHouse did not remove every retired or invisible candidate; "
                "cleanup is safe to retry"
            )
        result = plan.summary(disposition="removed")
        result["removed_ready_publication_count"] = len(
            plan.retired_ready_publication_ids
        )
        result["removed_candidate_attempt_count"] = len(deletion_ids)
        return result


def cleanup_clickhouse_serving_copy(
    config: PublisherConfig,
    *,
    keep_ready_versions: int = MINIMUM_READY_VERSIONS,
    apply: bool = False,
    exclusive_writer_lock_confirmed: bool = False,
    protected_ready_publication_ids: Sequence[str] = (),
    store: RetentionStore | None = None,
) -> dict[str, Any]:
    """Convenience boundary for Airflow tasks and local dry-run commands."""

    return ServingRetentionManager(config, store=store).cleanup(
        keep_ready_versions=keep_ready_versions,
        apply=apply,
        exclusive_writer_lock_confirmed=exclusive_writer_lock_confirmed,
        protected_ready_publication_ids=protected_ready_publication_ids,
    )


def _validate_keep_ready_versions(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ServingRetentionError("keep_ready_versions must be an integer")
    if value < MINIMUM_READY_VERSIONS:
        raise ServingRetentionError(
            f"keep_ready_versions must be at least {MINIMUM_READY_VERSIONS} so the "
            "current and previous ready publications are retained"
        )


def _validate_attempt_ids(values: Sequence[str] | set[str], label: str) -> None:
    for value in values:
        if not SAFE_ATTEMPT_ID.fullmatch(value):
            raise ServingRetentionError(
                f"{label} ID is not a supported serving publication ID"
            )


def _attempt_id_batches(values: Sequence[str]) -> list[tuple[str, ...]]:
    unique_values = tuple(sorted(set(values)))
    _validate_attempt_ids(unique_values, "cleanup")
    return [
        unique_values[offset : offset + DELETE_BATCH_SIZE]
        for offset in range(0, len(unique_values), DELETE_BATCH_SIZE)
    ]


def _attempt_id_literals(values: Sequence[str]) -> str:
    _validate_attempt_ids(values, "cleanup")
    if not values:
        raise ServingRetentionError("cleanup mutation requires at least one ID")
    return ", ".join(f"'{value}'" for value in values)


def _identity_set_sha256(values: Sequence[str]) -> str:
    payload = json.dumps(sorted(set(values)), separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
