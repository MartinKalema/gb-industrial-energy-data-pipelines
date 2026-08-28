"""Run the finite dbt dimensional build after a reconciled batch load.

The runner keeps dbt's verbose output in the Airflow task log and exchanges
only a compact, JSON-serializable artifact summary with downstream code.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
from typing import Any, Callable, Mapping, Protocol, Sequence
import urllib.error
import urllib.parse
import urllib.request

from .models import PipelineError, SAFE_RUN_IDENTIFIER


DEFAULT_DBT_TIMEOUT_SECONDS = 120 * 60
MAX_DBT_TIMEOUT_SECONDS = 2 * 60 * 60
PROCESS_TERMINATION_GRACE_SECONDS = 30
DEFAULT_TRINO_CLEANUP_TIMEOUT_SECONDS = 60
MAX_TRINO_CLEANUP_TIMEOUT_SECONDS = 5 * 60
TRINO_CLEANUP_POLL_SECONDS = 0.5
# A short stable-empty window covers a request already in flight when the local
# dbt process group is stopped. Cleanup runs only on failure or interruption.
TRINO_CLEANUP_QUIET_POLLS = 5
TRINO_TERMINAL_QUERY_STATES = frozenset({"FAILED", "FINISHED"})
_CHILD_ENVIRONMENT_KEYS = {
    "CURL_CA_BUNDLE",
    "HOME",
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "LOGNAME",
    "NO_PROXY",
    "PATH",
    "REQUESTS_CA_BUNDLE",
    "SSL_CERT_FILE",
    "TZ",
    "USER",
    "https_proxy",
    "http_proxy",
    "no_proxy",
}
_SAFE_DBT_STEP_NAME = re.compile(r"^[a-z][a-z0-9_]{0,47}$")
_MAX_DBT_SELECTOR_LENGTH = 256
_ALLOWED_DBT_COMMAND_NAMES = frozenset({"build", "run", "test"})


class DbtBuildError(PipelineError):
    """The governed dbt build did not complete successfully."""


class DbtRemoteCleanupError(DbtBuildError):
    """A stopped dbt attempt could not be proven safe for an automatic retry."""


@dataclass(frozen=True)
class DbtCommandStep:
    """One finite, restartable part of the dimensional build.

    ``name`` is both an operator-facing checkpoint name and a safe directory
    component. ``command_name`` is deliberately restricted to dbt commands
    that write a ``run_results.json`` artifact. ``selectors`` are dbt selection
    expressions rather than arbitrary command-line arguments.
    """

    name: str
    command_name: str
    selectors: tuple[str, ...] = ()
    indirect_selection: str | None = None
    expected_model_result_count: int | None = None
    expected_test_result_count: int | None = None

    def validate(self) -> None:
        if not isinstance(self.name, str) or not _SAFE_DBT_STEP_NAME.fullmatch(
            self.name
        ):
            raise DbtBuildError(
                "dbt step name must start with a lowercase letter and contain "
                "only lowercase letters, numbers, or underscores"
            )
        if (
            not isinstance(self.command_name, str)
            or self.command_name not in _ALLOWED_DBT_COMMAND_NAMES
        ):
            raise DbtBuildError("dbt step command must be build, run, or test")
        if not isinstance(self.selectors, tuple):
            raise DbtBuildError("dbt step selectors must be an immutable tuple")
        for selector in self.selectors:
            if (
                not isinstance(selector, str)
                or not selector
                or selector != selector.strip()
                or any(character.isspace() for character in selector)
                or selector.startswith("-")
                or len(selector) > _MAX_DBT_SELECTOR_LENGTH
            ):
                raise DbtBuildError(
                    "each dbt selector must be a non-empty selection expression "
                    "without whitespace or a leading option marker"
                )
        if len(set(self.selectors)) != len(self.selectors):
            raise DbtBuildError("dbt step selectors must not contain duplicates")
        if self.command_name in {"run", "test"} and not self.selectors:
            raise DbtBuildError(
                f"dbt {self.command_name} steps must have an explicit selector"
            )
        if self.indirect_selection not in (None, "cautious"):
            raise DbtBuildError(
                "dbt indirect selection must be omitted or set to cautious"
            )
        if self.indirect_selection is not None and self.command_name != "build":
            raise DbtBuildError(
                "dbt indirect selection is supported only for build checkpoints"
            )
        for field_name, expected_count in (
            ("expected_model_result_count", self.expected_model_result_count),
            ("expected_test_result_count", self.expected_test_result_count),
        ):
            if expected_count is not None and (
                not isinstance(expected_count, int)
                or isinstance(expected_count, bool)
                or expected_count < 0
            ):
                raise DbtBuildError(f"{field_name} must be a non-negative integer")
        if (
            self.expected_model_result_count is not None
            and self.expected_test_result_count is not None
            and self.expected_model_result_count + self.expected_test_result_count < 1
        ):
            raise DbtBuildError("a dbt step must expect at least one result")


FULL_DIMENSIONAL_MART_STEP = DbtCommandStep(
    name="build_dimensional_mart",
    command_name="build",
)


DIMENSIONAL_MART_DBT_STEPS = (
    DbtCommandStep(
        name="prepare_and_test_loaded_data",
        command_name="build",
        selectors=("path:models/staging",),
        indirect_selection="cautious",
        expected_model_result_count=9,
        expected_test_result_count=235,
    ),
    DbtCommandStep(
        name="prepare_and_test_delivery_calculations",
        command_name="build",
        selectors=("path:models/intermediate",),
        indirect_selection="cautious",
        expected_model_result_count=33,
        expected_test_result_count=8,
    ),
    DbtCommandStep(
        name="build_current_delivery_fact",
        command_name="run",
        selectors=("fct_steam_delivery_interval",),
        expected_model_result_count=1,
        expected_test_result_count=0,
    ),
    DbtCommandStep(
        name="build_delivery_history_fact",
        command_name="run",
        selectors=("fct_steam_delivery_interval_history",),
        expected_model_result_count=1,
        expected_test_result_count=0,
    ),
    DbtCommandStep(
        name="build_dimension_tables",
        command_name="run",
        selectors=("path:models/marts/dim_*",),
        expected_model_result_count=13,
        expected_test_result_count=0,
    ),
    DbtCommandStep(
        name="test_complete_dimensional_mart",
        command_name="test",
        selectors=(
            "path:models/marts",
            "assert_accepted_reconciliation_scenarios",
            "assert_revision_state_machine_fixtures",
        ),
        expected_model_result_count=0,
        expected_test_result_count=70,
    ),
)


@dataclass(frozen=True)
class DbtBuildConfig:
    """Non-secret runtime settings for one finite dbt invocation."""

    project_dir: Path
    profiles_dir: Path
    target_base_dir: Path
    log_base_dir: Path
    executable: str = "dbt"
    target_name: str = "dev"
    timeout_seconds: int = DEFAULT_DBT_TIMEOUT_SECONDS
    trino_endpoint: str = "http://127.0.0.1:8080"
    trino_user: str = "airflow"
    trino_cleanup_timeout_seconds: int = DEFAULT_TRINO_CLEANUP_TIMEOUT_SECONDS

    @classmethod
    def from_environment(
        cls,
        repository_root: Path,
        environment: Mapping[str, str] | None = None,
    ) -> "DbtBuildConfig":
        environment = environment or os.environ
        project_dir = Path(
            environment.get("DBT_PROJECT_DIR", repository_root / "transformations")
        )
        profiles_dir = Path(
            environment.get("DBT_PROFILES_DIR", project_dir)
        )
        return cls(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            target_base_dir=Path(
                environment.get("DBT_TARGET_PATH", project_dir / "target")
            ),
            log_base_dir=Path(
                environment.get("DBT_LOG_PATH", project_dir / "logs")
            ),
            executable=environment.get("DBT_EXECUTABLE", "dbt"),
            target_name=environment.get("DBT_TARGET", "dev"),
            timeout_seconds=_parse_timeout(
                environment.get(
                    "DBT_BUILD_TIMEOUT_SECONDS",
                    str(DEFAULT_DBT_TIMEOUT_SECONDS),
                )
            ),
            trino_endpoint=environment.get(
                "TRINO_URL",
                "http://127.0.0.1:8080",
            ),
            trino_user=environment.get(
                "DBT_TRINO_USER",
                environment.get("TRINO_USER", "airflow"),
            ),
            trino_cleanup_timeout_seconds=_parse_trino_cleanup_timeout(
                environment.get(
                    "DBT_TRINO_CLEANUP_TIMEOUT_SECONDS",
                    str(DEFAULT_TRINO_CLEANUP_TIMEOUT_SECONDS),
                )
            ),
        )

    def validate(self) -> None:
        if not self.project_dir.is_absolute():
            raise DbtBuildError("DBT project directory must be absolute")
        if not self.profiles_dir.is_absolute():
            raise DbtBuildError("DBT profiles directory must be absolute")
        if not self.target_base_dir.is_absolute():
            raise DbtBuildError("DBT target directory must be absolute")
        if not self.log_base_dir.is_absolute():
            raise DbtBuildError("DBT log directory must be absolute")
        if not (self.project_dir / "dbt_project.yml").is_file():
            raise DbtBuildError(
                f"dbt_project.yml is missing from {self.project_dir}"
            )
        if not (self.profiles_dir / "profiles.yml").is_file():
            raise DbtBuildError(f"profiles.yml is missing from {self.profiles_dir}")
        if not self.executable.strip():
            raise DbtBuildError("DBT executable must be non-empty")
        if not self.target_name.strip():
            raise DbtBuildError("DBT target must be non-empty")
        if not 1 <= self.timeout_seconds <= MAX_DBT_TIMEOUT_SECONDS:
            raise DbtBuildError(
                f"DBT timeout must be between 1 and {MAX_DBT_TIMEOUT_SECONDS} seconds"
            )
        parsed_endpoint = urllib.parse.urlparse(self.trino_endpoint)
        if (
            parsed_endpoint.scheme not in {"http", "https"}
            or not parsed_endpoint.netloc
        ):
            raise DbtBuildError("Trino endpoint must be an absolute HTTP(S) URL")
        if not self.trino_user.strip():
            raise DbtBuildError("Trino user must be non-empty")
        if not (
            1
            <= self.trino_cleanup_timeout_seconds
            <= MAX_TRINO_CLEANUP_TIMEOUT_SECONDS
        ):
            raise DbtBuildError(
                "Trino cleanup timeout must be between 1 and "
                f"{MAX_TRINO_CLEANUP_TIMEOUT_SECONDS} seconds"
            )


CommandRunner = Callable[..., subprocess.CompletedProcess[Any]]


def _parse_timeout(value: str) -> int:
    try:
        timeout = int(value)
    except (TypeError, ValueError) as exc:
        raise DbtBuildError("DBT_BUILD_TIMEOUT_SECONDS must be an integer") from exc
    if not 1 <= timeout <= MAX_DBT_TIMEOUT_SECONDS:
        raise DbtBuildError(
            f"DBT_BUILD_TIMEOUT_SECONDS must be between 1 and "
            f"{MAX_DBT_TIMEOUT_SECONDS}"
        )
    return timeout


def _parse_trino_cleanup_timeout(value: str) -> int:
    try:
        timeout = int(value)
    except (TypeError, ValueError) as exc:
        raise DbtBuildError(
            "DBT_TRINO_CLEANUP_TIMEOUT_SECONDS must be an integer"
        ) from exc
    if not 1 <= timeout <= MAX_TRINO_CLEANUP_TIMEOUT_SECONDS:
        raise DbtBuildError(
            "DBT_TRINO_CLEANUP_TIMEOUT_SECONDS must be between 1 and "
            f"{MAX_TRINO_CLEANUP_TIMEOUT_SECONDS}"
        )
    return timeout


def _attempt_key(orchestrator_run_id: str) -> str:
    return hashlib.sha256(orchestrator_run_id.encode("utf-8")).hexdigest()[:16]


def _trino_attempt_tag(
    pipeline_run_id: str,
    orchestrator_run_id: str,
    step_name: str,
    attempt_number: int,
) -> str:
    identity = (
        f"{pipeline_run_id}\0{orchestrator_run_id}\0{step_name}"
    ).encode("utf-8")
    identity_digest = hashlib.sha256(identity).hexdigest()[:12]
    readable_step_name = step_name.replace("_", "-")[:24].rstrip("-")
    return (
        f"steam-delivery-dbt-{readable_step_name}-{identity_digest}"
        f"-try-{attempt_number:02d}"
    )


def _child_environment(source: Mapping[str, str]) -> dict[str, str]:
    environment = {
        key: value
        for key, value in source.items()
        if key in _CHILD_ENVIRONMENT_KEYS or key.startswith("DBT_TRINO_")
    }
    environment["PYTHONUNBUFFERED"] = "1"
    return environment


class AttemptQueryController(Protocol):
    """Cleanup boundary for Trino queries owned by one dbt attempt."""

    def cancel_and_wait(self) -> tuple[str, ...]: ...


class TrinoAttemptQueryController:
    """Cancel only active Trino queries carrying one exact dbt attempt tag.

    dbt-trino fixes the Trino ``source`` header to its adapter name and version,
    but exposes native Trino client tags in ``profiles.yml``. Trino's query API
    returns those tags for both active and completed queries. An exact tag plus
    the expected Trino user therefore gives every Airflow try its own cleanup
    boundary without matching SQL text or affecting another dbt invocation.
    """

    def __init__(
        self,
        endpoint: str,
        *,
        user: str,
        attempt_tag: str,
        timeout_seconds: float = DEFAULT_TRINO_CLEANUP_TIMEOUT_SECONDS,
        poll_seconds: float = TRINO_CLEANUP_POLL_SECONDS,
        quiet_polls: int = TRINO_CLEANUP_QUIET_POLLS,
        urlopen: Callable[..., Any] = urllib.request.urlopen,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        endpoint = endpoint.rstrip("/")
        parsed_endpoint = urllib.parse.urlparse(endpoint)
        if (
            parsed_endpoint.scheme not in {"http", "https"}
            or not parsed_endpoint.netloc
        ):
            raise ValueError("Trino endpoint must be an absolute HTTP(S) URL")
        if not user.strip() or not attempt_tag.strip():
            raise ValueError("Trino user and dbt attempt tag must be non-empty")
        if timeout_seconds <= 0 or poll_seconds <= 0 or quiet_polls < 1:
            raise ValueError("Trino cleanup timing values must be positive")
        self.endpoint = endpoint
        self.user = user
        self.attempt_tag = attempt_tag
        self.timeout_seconds = float(timeout_seconds)
        self.poll_seconds = float(poll_seconds)
        self.quiet_polls = quiet_polls
        self._urlopen = urlopen
        self._monotonic = monotonic
        self._sleep = sleep

    def cancel_and_wait(self) -> tuple[str, ...]:
        """Cancel matching active queries and wait for a stable terminal state."""

        deadline = self._monotonic() + self.timeout_seconds
        cancelled_query_ids: set[str] = set()
        remaining_query_ids: tuple[str, ...] = ()
        quiet_poll_count = 0

        while True:
            remaining_seconds = deadline - self._monotonic()
            if remaining_seconds <= 0:
                detail = (
                    ", ".join(remaining_query_ids)
                    if remaining_query_ids
                    else "matching query state did not remain quiet"
                )
                raise DbtBuildError(
                    "Trino did not confirm terminal state for dbt attempt "
                    f"{self.attempt_tag} within {self.timeout_seconds:g} seconds: "
                    f"{detail}"
                )

            remaining_query_ids = self._matching_active_query_ids(
                remaining_seconds=remaining_seconds
            )
            if remaining_query_ids:
                quiet_poll_count = 0
                for query_id in remaining_query_ids:
                    if query_id not in cancelled_query_ids:
                        self._cancel_query(
                            query_id,
                            remaining_seconds=max(
                                deadline - self._monotonic(),
                                0.001,
                            ),
                        )
                        cancelled_query_ids.add(query_id)
            else:
                quiet_poll_count += 1
                if quiet_poll_count >= self.quiet_polls:
                    return tuple(sorted(cancelled_query_ids))

            remaining_seconds = deadline - self._monotonic()
            if remaining_seconds > 0:
                self._sleep(min(self.poll_seconds, remaining_seconds))

    def _matching_active_query_ids(
        self,
        *,
        remaining_seconds: float,
    ) -> tuple[str, ...]:
        request = urllib.request.Request(
            f"{self.endpoint}/v1/query",
            headers={
                "Accept": "application/json",
                "X-Trino-User": self.user,
            },
            method="GET",
        )
        payload = self._read_response(
            request,
            remaining_seconds=remaining_seconds,
            operation="list active queries",
        )
        try:
            query_values = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DbtBuildError("Trino query-list response was not valid JSON") from exc
        if not isinstance(query_values, list):
            raise DbtBuildError("Trino query-list response was not an array")

        matching_query_ids: list[str] = []
        for query_value in query_values:
            if not isinstance(query_value, Mapping):
                raise DbtBuildError("Trino query-list entry was not an object")
            session = query_value.get("session")
            if not isinstance(session, Mapping):
                continue
            client_tags = session.get("clientTags", ())
            if (
                session.get("user") != self.user
                or not isinstance(client_tags, list)
                or self.attempt_tag not in client_tags
            ):
                continue
            state = str(query_value.get("state", "UNKNOWN")).upper()
            if state in TRINO_TERMINAL_QUERY_STATES:
                continue
            query_id = query_value.get("queryId")
            if not isinstance(query_id, str) or not query_id.strip():
                raise DbtBuildError(
                    "matching active Trino query did not include a query ID"
                )
            matching_query_ids.append(query_id)
        return tuple(sorted(set(matching_query_ids)))

    def _cancel_query(self, query_id: str, *, remaining_seconds: float) -> None:
        encoded_query_id = urllib.parse.quote(query_id, safe="")
        request = urllib.request.Request(
            f"{self.endpoint}/v1/query/{encoded_query_id}",
            headers={"X-Trino-User": self.user},
            method="DELETE",
        )
        try:
            with self._urlopen(
                request,
                timeout=min(5.0, remaining_seconds),
            ) as response:
                response.read()
        except urllib.error.HTTPError as exc:
            if exc.code in {404, 410}:
                # The query reached a terminal state between the list and DELETE.
                return
            detail = exc.read().decode("utf-8", errors="replace")
            raise DbtBuildError(
                f"Trino rejected cancellation for query {query_id} with "
                f"HTTP {exc.code}: {detail[:500]}"
            ) from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise DbtBuildError(
                f"Trino cancellation request failed for query {query_id}"
            ) from exc

    def _read_response(
        self,
        request: urllib.request.Request,
        *,
        remaining_seconds: float,
        operation: str,
    ) -> bytes:
        try:
            with self._urlopen(
                request,
                timeout=min(5.0, remaining_seconds),
            ) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise DbtBuildError(
                f"Trino could not {operation}; HTTP {exc.code}: {detail[:500]}"
            ) from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise DbtBuildError(f"Trino could not {operation}") from exc


def _stop_process_group(
    process: subprocess.Popen[Any],
    *,
    grace_seconds: int = PROCESS_TERMINATION_GRACE_SECONDS,
) -> None:
    """Stop dbt and every local child before the Airflow task releases its pool."""

    process_group_id = process.pid
    try:
        os.killpg(process_group_id, signal.SIGTERM)
    except ProcessLookupError:
        process.poll()
        return

    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        process.poll()
        try:
            os.killpg(process_group_id, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)

    try:
        os.killpg(process_group_id, signal.SIGKILL)
    except ProcessLookupError:
        process.poll()
        return

    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        process.poll()
        try:
            os.killpg(process_group_id, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    raise DbtRemoteCleanupError(
        "dbt local process group did not stop after SIGKILL"
    )


def _managed_command(
    command: Sequence[str],
    *,
    cwd: str,
    env: Mapping[str, str],
    check: bool,
    timeout: int,
) -> subprocess.CompletedProcess[Any]:
    """Run one command in its own process group and clean it up on any interruption."""

    process = subprocess.Popen(
        list(command),
        cwd=cwd,
        env=dict(env),
        start_new_session=True,
    )
    try:
        returncode = process.wait(timeout=timeout)
    except BaseException:
        try:
            _stop_process_group(process)
        except DbtRemoteCleanupError:
            raise
        except BaseException as cleanup_error:
            raise DbtRemoteCleanupError(
                "dbt local process-group cleanup was interrupted or could not "
                "be confirmed"
            ) from cleanup_error
        raise
    completed = subprocess.CompletedProcess(list(command), returncode)
    if check and returncode:
        raise subprocess.CalledProcessError(returncode, list(command))
    return completed


def _command(
    config: DbtBuildConfig,
    target_path: Path,
    log_path: Path,
    step: DbtCommandStep,
) -> list[str]:
    command = [
        config.executable,
        step.command_name,
        "--project-dir",
        str(config.project_dir),
        "--profiles-dir",
        str(config.profiles_dir),
        "--target",
        config.target_name,
        "--target-path",
        str(target_path),
        "--log-path",
        str(log_path),
        "--no-populate-cache",
    ]
    if step.selectors:
        command.extend(["--select", *step.selectors])
    if step.indirect_selection is not None:
        command.extend(["--indirect-selection", step.indirect_selection])
    return command


def _read_run_results(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise DbtBuildError(f"cannot read dbt run artifact {path}") from exc
    if not isinstance(value, Mapping) or not isinstance(value.get("results"), list):
        raise DbtBuildError("dbt run_results.json is malformed")
    return value


def _result_summary(
    *,
    pipeline_run_id: str,
    step: DbtCommandStep,
    attempt_number: int,
    trino_attempt_tag: str,
    target_path: Path,
    log_path: Path,
    run_results: Mapping[str, Any],
) -> dict[str, Any]:
    results = run_results["results"]
    statuses = Counter(str(item.get("status", "unknown")) for item in results)
    unsuccessful = {
        status: count
        for status, count in statuses.items()
        if status not in {"pass", "success"}
    }
    if not results:
        raise DbtBuildError("dbt completed without building or testing any resources")
    if unsuccessful:
        raise DbtBuildError(
            "dbt returned unsuccessful resource statuses: "
            + ", ".join(
                f"{status}={count}" for status, count in sorted(unsuccessful.items())
            )
        )
    resource_types = Counter(
        str(item.get("unique_id", "unknown")).split(".", 1)[0]
        for item in results
    )
    model_result_count = resource_types.get("model", 0)
    test_result_count = resource_types.get("test", 0)
    expected_counts = {
        "model": step.expected_model_result_count,
        "test": step.expected_test_result_count,
    }
    actual_counts = {"model": model_result_count, "test": test_result_count}
    count_mismatches = {
        resource_type: (expected_count, actual_counts[resource_type])
        for resource_type, expected_count in expected_counts.items()
        if expected_count is not None
        and actual_counts[resource_type] != expected_count
    }
    if count_mismatches:
        raise DbtBuildError(
            f"dbt step {step.name} returned unexpected resource counts: "
            + ", ".join(
                f"{resource_type} expected={expected} actual={actual}"
                for resource_type, (expected, actual) in sorted(
                    count_mismatches.items()
                )
            )
        )
    if all(expected_count is not None for expected_count in expected_counts.values()):
        expected_result_count = sum(
            int(expected_count) for expected_count in expected_counts.values()
        )
        if len(results) != expected_result_count:
            raise DbtBuildError(
                f"dbt step {step.name} expected {expected_result_count} total "
                f"results but received {len(results)}"
            )
    metadata = run_results.get("metadata", {})
    if not isinstance(metadata, Mapping):
        metadata = {}
    return {
        "pipeline_run_id": pipeline_run_id,
        "dbt_step_name": step.name,
        "dbt_command_name": step.command_name,
        "dbt_selectors": list(step.selectors),
        "dbt_indirect_selection": step.indirect_selection,
        "attempt_number": attempt_number,
        "trino_attempt_tag": trino_attempt_tag,
        "status": "succeeded",
        "dbt_version": str(metadata.get("dbt_version", "unknown")),
        "dbt_invocation_id": str(metadata.get("invocation_id", "unknown")),
        "generated_at_utc": str(metadata.get("generated_at", "unknown")),
        "elapsed_seconds": round(float(run_results.get("elapsed_time", 0.0)), 3),
        "result_count": len(results),
        "model_result_count": model_result_count,
        "test_result_count": test_result_count,
        "status_counts": dict(sorted(statuses.items())),
        "target_path": str(target_path),
        "log_path": str(log_path),
    }


def run_dbt_command_step(
    config: DbtBuildConfig,
    *,
    step: DbtCommandStep,
    pipeline_run_id: str,
    orchestrator_run_id: str,
    attempt_number: int = 1,
    command_runner: CommandRunner = _managed_command,
    query_controller: AttemptQueryController | None = None,
) -> dict[str, Any]:
    """Execute one typed dbt checkpoint exactly once."""

    config.validate()
    if not isinstance(step, DbtCommandStep):
        raise DbtBuildError("step must be a DbtCommandStep")
    step.validate()
    if not SAFE_RUN_IDENTIFIER.fullmatch(pipeline_run_id):
        raise DbtBuildError("pipeline_run_id contains unsupported characters")
    if not orchestrator_run_id.strip():
        raise DbtBuildError("orchestrator_run_id must be non-empty")
    if not isinstance(attempt_number, int) or isinstance(attempt_number, bool):
        raise DbtBuildError("attempt_number must be a positive integer")
    if attempt_number < 1:
        raise DbtBuildError("attempt_number must be a positive integer")

    attempt_key = _attempt_key(orchestrator_run_id)
    trino_attempt_tag = _trino_attempt_tag(
        pipeline_run_id,
        orchestrator_run_id,
        step.name,
        attempt_number,
    )
    attempt_dir = Path(attempt_key) / step.name / f"try-{attempt_number:02d}"
    target_path = config.target_base_dir / pipeline_run_id / attempt_dir
    log_path = config.log_base_dir / pipeline_run_id / attempt_dir
    target_path.mkdir(parents=True, exist_ok=True)
    log_path.mkdir(parents=True, exist_ok=True)
    run_results_path = target_path / "run_results.json"
    run_results_path.unlink(missing_ok=True)

    environment = _child_environment(os.environ)
    environment.update(
        {
            "DBT_LOG_PATH": str(log_path),
            "DBT_PROFILES_DIR": str(config.profiles_dir),
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "DBT_TARGET_PATH": str(target_path),
            "DBT_TRINO_ATTEMPT_TAG": trino_attempt_tag,
            "DBT_TRINO_USER": config.trino_user,
        }
    )
    query_controller = query_controller or TrinoAttemptQueryController(
        config.trino_endpoint,
        user=config.trino_user,
        attempt_tag=trino_attempt_tag,
        timeout_seconds=config.trino_cleanup_timeout_seconds,
    )
    command: Sequence[str] = _command(config, target_path, log_path, step)
    try:
        completed = command_runner(
            list(command),
            cwd=str(config.project_dir),
            env=environment,
            check=False,
            timeout=config.timeout_seconds,
        )
    except FileNotFoundError as exc:
        raise DbtBuildError(f"dbt executable was not found: {config.executable}") from exc
    except BaseException as exc:
        try:
            query_controller.cancel_and_wait()
        except BaseException as cleanup_error:
            raise DbtRemoteCleanupError(
                "dbt stopped, but matching Trino queries could not be confirmed "
                f"stopped for attempt {trino_attempt_tag}"
            ) from cleanup_error
        if isinstance(exc, subprocess.TimeoutExpired):
            raise DbtBuildError(
                f"dbt step {step.name} exceeded its "
                f"{config.timeout_seconds}-second limit"
            ) from exc
        raise

    if completed.returncode != 0:
        try:
            query_controller.cancel_and_wait()
        except BaseException as cleanup_error:
            raise DbtRemoteCleanupError(
                "dbt failed, but matching Trino queries could not be confirmed "
                f"stopped for attempt {trino_attempt_tag}"
            ) from cleanup_error
        raise DbtBuildError(
            f"dbt step {step.name} failed with exit code {completed.returncode}; "
            f"inspect {log_path}"
        )
    if not run_results_path.is_file():
        raise DbtBuildError("dbt reported success without writing run_results.json")

    return _result_summary(
        pipeline_run_id=pipeline_run_id,
        step=step,
        attempt_number=attempt_number,
        trino_attempt_tag=trino_attempt_tag,
        target_path=target_path,
        log_path=log_path,
        run_results=_read_run_results(run_results_path),
    )


def run_dbt_build(
    config: DbtBuildConfig,
    *,
    pipeline_run_id: str,
    orchestrator_run_id: str,
    attempt_number: int = 1,
    command_runner: CommandRunner = _managed_command,
    query_controller: AttemptQueryController | None = None,
) -> dict[str, Any]:
    """Execute the original full-mart build as one compatibility checkpoint."""

    return run_dbt_command_step(
        config,
        step=FULL_DIMENSIONAL_MART_STEP,
        pipeline_run_id=pipeline_run_id,
        orchestrator_run_id=orchestrator_run_id,
        attempt_number=attempt_number,
        command_runner=command_runner,
        query_controller=query_controller,
    )
