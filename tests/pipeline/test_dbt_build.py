from __future__ import annotations

import ast
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

import pytest

from ingestion.batch.pipeline import dbt_build
from ingestion.batch.pipeline.dbt_build import (
    DEFAULT_DBT_TIMEOUT_SECONDS,
    DIMENSIONAL_MART_DBT_STEPS,
    DbtCommandStep,
    DbtBuildConfig,
    DbtBuildError,
    DbtRemoteCleanupError,
    TrinoAttemptQueryController,
    _managed_command,
    run_dbt_build,
    run_dbt_command_step,
)
from ingestion.batch.pipeline.models import PipelineError
from ingestion.batch.pipeline.workflow import (
    build_dimensional_mart,
    build_dimensional_mart_step,
    plan_run,
)


def test_default_dbt_timeout_has_real_r2_execution_headroom() -> None:
    assert DEFAULT_DBT_TIMEOUT_SECONDS == 120 * 60


def _config(
    tmp_path: Path,
    *,
    timeout_seconds: int = 30,
    trino_user: str = "airflow",
) -> DbtBuildConfig:
    project_dir = tmp_path / "project"
    profiles_dir = tmp_path / "profiles"
    project_dir.mkdir()
    profiles_dir.mkdir()
    (project_dir / "dbt_project.yml").write_text("name: test_project\n")
    (profiles_dir / "profiles.yml").write_text("test_project: {}\n")
    return DbtBuildConfig(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        target_base_dir=tmp_path / "target",
        log_base_dir=tmp_path / "logs",
        executable="/opt/dbt/bin/dbt",
        timeout_seconds=timeout_seconds,
        trino_user=trino_user,
    )


def _write_success_artifact(target_path: Path) -> None:
    artifact = {
        "metadata": {
            "dbt_version": "1.12.3",
            "invocation_id": "dbt-invocation-1",
            "generated_at": "2026-08-28T20:00:00Z",
        },
        "elapsed_time": 12.34567,
        "results": [
            {
                "unique_id": "model.test_project.fct_delivery",
                "status": "success",
            },
            {
                "unique_id": "test.test_project.not_null_fact_key",
                "status": "success",
            },
            {
                "unique_id": "test.test_project.accepted_values_status",
                "status": "success",
            },
        ],
    }
    target_path.mkdir(parents=True, exist_ok=True)
    (target_path / "run_results.json").write_text(json.dumps(artifact))


class RecordingQueryController:
    def __init__(self, *, error: BaseException | None = None):
        self.call_count = 0
        self.error = error

    def cancel_and_wait(self) -> tuple[str, ...]:
        self.call_count += 1
        if self.error is not None:
            raise self.error
        return ("query-1",)


def test_run_dbt_build_uses_finite_direct_command_and_returns_small_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path, trino_user="mart-builder")
    captured: dict[str, object] = {}

    def fake_runner(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        target_path = Path(kwargs["env"]["DBT_TARGET_PATH"])
        assert not (target_path / "run_results.json").exists()
        _write_success_artifact(target_path)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "must-not-reach-dbt")
    result = run_dbt_build(
        config,
        pipeline_run_id="batch-20260826-20260826-0123456789abcdef",
        orchestrator_run_id="manual__2026-08-28T20:00:00+00:00",
        command_runner=fake_runner,
    )

    command = captured["command"]
    kwargs = captured["kwargs"]
    assert command == [
        "/opt/dbt/bin/dbt",
        "build",
        "--project-dir",
        str(config.project_dir),
        "--profiles-dir",
        str(config.profiles_dir),
        "--target",
        "dev",
        "--target-path",
        result["target_path"],
        "--log-path",
        result["log_path"],
        "--no-populate-cache",
    ]
    assert kwargs["timeout"] == 30
    assert kwargs["check"] is False
    assert kwargs["env"]["DBT_SEND_ANONYMOUS_USAGE_STATS"] == "false"
    assert kwargs["env"]["DBT_TRINO_ATTEMPT_TAG"] == result["trino_attempt_tag"]
    assert kwargs["env"]["DBT_TRINO_USER"] == config.trino_user
    assert re.fullmatch(
        r"steam-delivery-dbt-build-dimensional-mart-[a-f0-9]{12}-try-01",
        result["trino_attempt_tag"],
    )
    assert "R2_SECRET_ACCESS_KEY" not in kwargs["env"]
    assert result["status"] == "succeeded"
    assert result["dbt_step_name"] == "build_dimensional_mart"
    assert result["dbt_selectors"] == []
    assert result["attempt_number"] == 1
    assert "/build_dimensional_mart/try-01" in result["target_path"]
    assert "/try-01" in result["target_path"]
    assert result["result_count"] == 3
    assert result["model_result_count"] == 1
    assert result["test_result_count"] == 2
    assert result["status_counts"] == {"success": 3}
    assert result["elapsed_seconds"] == 12.346
    assert len(json.dumps(result, separators=(",", ":")).encode()) < 2_048


def test_selected_dbt_steps_have_separate_commands_artifacts_and_query_tags(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, trino_user="mart-builder")
    captured_commands: list[list[str]] = []

    def successful_runner(command, **kwargs):
        captured_commands.append(command)
        _write_success_artifact(Path(kwargs["env"]["DBT_TARGET_PATH"]))
        return subprocess.CompletedProcess(command, 0)

    common_arguments = {
        "pipeline_run_id": "batch-20260826-20260826-0123456789abcdef",
        "orchestrator_run_id": "manual__same-dag-run",
        "attempt_number": 1,
        "command_runner": successful_runner,
    }
    staging = run_dbt_command_step(
        config,
        step=DbtCommandStep(
            name="prepare_and_test_loaded_data",
            command_name="build",
            selectors=("path:models/staging",),
            indirect_selection="cautious",
        ),
        **common_arguments,
    )
    current_mart = run_dbt_command_step(
        config,
        step=DbtCommandStep(
            name="build_current_delivery_fact",
            command_name="run",
            selectors=("fct_steam_delivery_interval",),
        ),
        **common_arguments,
    )

    assert captured_commands[0][-4:] == [
        "--select",
        "path:models/staging",
        "--indirect-selection",
        "cautious",
    ]
    assert captured_commands[1][-2:] == [
        "--select",
        "fct_steam_delivery_interval",
    ]
    assert captured_commands[0][1] == "build"
    assert captured_commands[1][1] == "run"
    assert staging["dbt_step_name"] == "prepare_and_test_loaded_data"
    assert staging["dbt_command_name"] == "build"
    assert staging["dbt_selectors"] == ["path:models/staging"]
    assert staging["dbt_indirect_selection"] == "cautious"
    assert current_mart["dbt_step_name"] == "build_current_delivery_fact"
    assert current_mart["dbt_command_name"] == "run"
    assert current_mart["dbt_selectors"] == ["fct_steam_delivery_interval"]
    assert "/prepare_and_test_loaded_data/try-01" in staging["target_path"]
    assert "/build_current_delivery_fact/try-01" in current_mart["target_path"]
    assert staging["target_path"] != current_mart["target_path"]
    assert staging["log_path"] != current_mart["log_path"]
    assert staging["trino_attempt_tag"] != current_mart["trino_attempt_tag"]
    assert "prepare-and-test-loaded" in staging["trino_attempt_tag"]
    assert "build-current-delivery-f" in current_mart["trino_attempt_tag"]


def test_governed_dbt_steps_cover_every_model_and_test_exactly_once() -> None:
    assert [
        (
            step.name,
            step.command_name,
            step.selectors,
            step.indirect_selection,
            step.expected_model_result_count,
            step.expected_test_result_count,
        )
        for step in DIMENSIONAL_MART_DBT_STEPS
    ] == [
        (
            "prepare_and_test_loaded_data",
            "build",
            ("path:models/staging",),
            "cautious",
            9,
            235,
        ),
        (
            "prepare_and_test_delivery_calculations",
            "build",
            ("path:models/intermediate",),
            "cautious",
            33,
            8,
        ),
        (
            "build_current_delivery_fact",
            "run",
            ("fct_steam_delivery_interval",),
            None,
            1,
            0,
        ),
        (
            "build_delivery_history_fact",
            "run",
            ("fct_steam_delivery_interval_history",),
            None,
            1,
            0,
        ),
        (
            "build_dimension_tables",
            "run",
            ("path:models/marts/dim_*",),
            None,
            13,
            0,
        ),
        (
            "test_complete_dimensional_mart",
            "test",
            (
                "path:models/marts",
                "assert_accepted_reconciliation_scenarios",
                "assert_revision_state_machine_fixtures",
            ),
            None,
            0,
            70,
        ),
    ]
    assert sum(
        int(step.expected_model_result_count or 0)
        for step in DIMENSIONAL_MART_DBT_STEPS
    ) == 57
    assert sum(
        int(step.expected_test_result_count or 0)
        for step in DIMENSIONAL_MART_DBT_STEPS
    ) == 313


def test_governed_dbt_selectors_match_the_real_project_graph(
    tmp_path: Path,
) -> None:
    repository_root = Path(__file__).resolve().parents[2]
    project_dir = repository_root / "transformations"
    target_path = tmp_path / "target"
    log_path = tmp_path / "logs"
    base_command = [
        str(Path(sys.executable).with_name("dbt")),
        "ls",
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(project_dir),
        "--target",
        "dev",
        "--target-path",
        str(target_path),
        "--log-path",
        str(log_path),
        "--no-populate-cache",
        "--output",
        "json",
    ]

    def listed_resources(arguments: list[str]) -> dict[str, dict[str, object]]:
        completed = subprocess.run(
            [*base_command, *arguments],
            check=True,
            capture_output=True,
            text=True,
        )
        resources: dict[str, dict[str, object]] = {}
        for line in completed.stdout.splitlines():
            try:
                resource = json.loads(line)
            except json.JSONDecodeError:
                continue
            unique_id = str(resource["unique_id"])
            resources[unique_id] = resource
        return resources

    all_resources = {
        unique_id: resource
        for unique_id, resource in listed_resources([]).items()
        if resource["resource_type"] in {"model", "test"}
    }
    selected_by_step: list[set[str]] = []
    model_stage: dict[str, int] = {}
    for stage, step in enumerate(DIMENSIONAL_MART_DBT_STEPS, start=1):
        arguments = ["--select", *step.selectors]
        if step.command_name == "run":
            arguments = ["--resource-type", "model", *arguments]
        elif step.command_name == "test":
            arguments = ["--resource-type", "test", *arguments]
        if step.indirect_selection is not None:
            arguments.extend(["--indirect-selection", step.indirect_selection])
        selected = set(listed_resources(arguments))
        assert not any(selected & earlier for earlier in selected_by_step)
        selected_by_step.append(selected)
        model_stage.update(
            {
                unique_id: stage
                for unique_id in selected
                if unique_id.startswith("model.")
            }
        )

    assert set().union(*selected_by_step) == set(all_resources)

    manifest = json.loads((target_path / "manifest.json").read_text())
    for unique_id, stage in model_stage.items():
        for dependency in manifest["nodes"][unique_id]["depends_on"]["nodes"]:
            if dependency.startswith("model."):
                assert model_stage[dependency] <= stage


def test_governed_dbt_step_rejects_a_silent_selector_count_change(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)

    def incomplete_runner(command, **kwargs):
        _write_success_artifact(Path(kwargs["env"]["DBT_TARGET_PATH"]))
        return subprocess.CompletedProcess(command, 0)

    with pytest.raises(DbtBuildError, match="unexpected resource counts"):
        run_dbt_command_step(
            config,
            step=DIMENSIONAL_MART_DBT_STEPS[0],
            pipeline_run_id="batch-20260826-20260826-0123456789abcdef",
            orchestrator_run_id="manual__selector-count-change",
            command_runner=incomplete_runner,
        )


@pytest.mark.parametrize(
    "step,error_match",
    [
        (DbtCommandStep(name="../staging", command_name="build"), "step name"),
        (DbtCommandStep(name="Build_staging", command_name="build"), "step name"),
        (DbtCommandStep(name="build_staging", command_name="seed"), "command"),
        (
            DbtCommandStep(
                name="build_staging",
                command_name="build",
                selectors=["path:models/staging"],
            ),
            "immutable tuple",
        ),
        (
            DbtCommandStep(
                name="build_staging",
                command_name="build",
                selectors=("--exclude",),
            ),
            "selection expression",
        ),
        (
            DbtCommandStep(
                name="build_staging",
                command_name="build",
                selectors=("path:models staging",),
            ),
            "selection expression",
        ),
        (
            DbtCommandStep(
                name="build_staging",
                command_name="build",
                selectors=("path:models/staging", "path:models/staging"),
            ),
            "duplicates",
        ),
        (DbtCommandStep(name="run_marts", command_name="run"), "explicit selector"),
        (
            DbtCommandStep(
                name="run_marts",
                command_name="run",
                selectors=("path:models/marts",),
                indirect_selection="cautious",
            ),
            "only for build",
        ),
        (
            DbtCommandStep(
                name="build_staging",
                command_name="build",
                indirect_selection="eager",
            ),
            "omitted or set to cautious",
        ),
        (
            DbtCommandStep(
                name="build_staging",
                command_name="build",
                expected_model_result_count=True,
            ),
            "non-negative integer",
        ),
    ],
)
def test_dbt_step_rejects_unsafe_names_and_selectors(
    tmp_path: Path,
    step: DbtCommandStep,
    error_match: str,
) -> None:
    with pytest.raises(DbtBuildError, match=error_match):
        run_dbt_command_step(
            _config(tmp_path),
            step=step,
            pipeline_run_id="batch-20260826-20260826-0123456789abcdef",
            orchestrator_run_id="manual__unsafe-step",
            command_runner=lambda *_args, **_kwargs: pytest.fail(
                "unsafe dbt step reached the command runner"
            ),
        )


def test_run_dbt_build_removes_stale_artifact_before_invocation(tmp_path: Path) -> None:
    config = _config(tmp_path)

    def first_runner(command, **kwargs):
        _write_success_artifact(Path(kwargs["env"]["DBT_TARGET_PATH"]))
        return subprocess.CompletedProcess(command, 0)

    first = run_dbt_build(
        config,
        pipeline_run_id="batch-20260826-20260826-0123456789abcdef",
        orchestrator_run_id="manual__stable",
        command_runner=first_runner,
    )

    def second_runner(command, **kwargs):
        target_path = Path(kwargs["env"]["DBT_TARGET_PATH"])
        assert not (target_path / "run_results.json").exists()
        return subprocess.CompletedProcess(command, 0)

    with pytest.raises(DbtBuildError, match="without writing run_results"):
        run_dbt_build(
            config,
            pipeline_run_id=first["pipeline_run_id"],
            orchestrator_run_id="manual__stable",
            command_runner=second_runner,
        )


def test_airflow_retries_keep_separate_dbt_artifacts(tmp_path: Path) -> None:
    config = _config(tmp_path)

    def successful_runner(command, **kwargs):
        _write_success_artifact(Path(kwargs["env"]["DBT_TARGET_PATH"]))
        return subprocess.CompletedProcess(command, 0)

    first = run_dbt_build(
        config,
        pipeline_run_id="batch-20260826-20260826-0123456789abcdef",
        orchestrator_run_id="manual__same-dag-run",
        attempt_number=1,
        command_runner=successful_runner,
    )
    second = run_dbt_build(
        config,
        pipeline_run_id=first["pipeline_run_id"],
        orchestrator_run_id="manual__same-dag-run",
        attempt_number=2,
        command_runner=successful_runner,
    )

    assert first["target_path"] != second["target_path"]
    assert "/try-01" in first["target_path"]
    assert "/try-02" in second["target_path"]
    assert (Path(first["target_path"]) / "run_results.json").is_file()
    assert (Path(second["target_path"]) / "run_results.json").is_file()


def test_run_dbt_build_propagates_exit_and_timeout_failures(tmp_path: Path) -> None:
    config = _config(tmp_path, timeout_seconds=15)
    failed_controller = RecordingQueryController()

    def failed_runner(command, **kwargs):
        return subprocess.CompletedProcess(command, 2)

    with pytest.raises(DbtBuildError, match="exit code 2"):
        run_dbt_build(
            config,
            pipeline_run_id="batch-20260826-20260826-0123456789abcdef",
            orchestrator_run_id="manual__failed",
            command_runner=failed_runner,
            query_controller=failed_controller,
        )
    assert failed_controller.call_count == 1

    def timed_out_runner(command, **kwargs):
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    timed_out_controller = RecordingQueryController()
    with pytest.raises(DbtBuildError, match="15-second limit"):
        run_dbt_build(
            config,
            pipeline_run_id="batch-20260826-20260826-0123456789abcdef",
            orchestrator_run_id="manual__timeout",
            command_runner=timed_out_runner,
            query_controller=timed_out_controller,
        )
    assert timed_out_controller.call_count == 1


def test_run_dbt_build_cleans_remote_queries_before_propagating_interrupt(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    controller = RecordingQueryController()

    def interrupted_runner(command, **kwargs):
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        run_dbt_build(
            config,
            pipeline_run_id="batch-20260826-20260826-0123456789abcdef",
            orchestrator_run_id="manual__interrupted",
            command_runner=interrupted_runner,
            query_controller=controller,
        )

    assert controller.call_count == 1


def test_run_dbt_build_reports_unconfirmed_remote_cleanup(tmp_path: Path) -> None:
    config = _config(tmp_path, timeout_seconds=15)
    controller = RecordingQueryController(error=DbtBuildError("query still running"))

    def timed_out_runner(command, **kwargs):
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    with pytest.raises(
        DbtRemoteCleanupError,
        match="could not be confirmed stopped",
    ):
        run_dbt_build(
            config,
            pipeline_run_id="batch-20260826-20260826-0123456789abcdef",
            orchestrator_run_id="manual__uncleared",
            command_runner=timed_out_runner,
            query_controller=controller,
        )

    assert controller.call_count == 1


def test_run_dbt_build_marks_interrupted_remote_cleanup_nonretryable(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, timeout_seconds=15)
    controller = RecordingQueryController(error=KeyboardInterrupt())

    def timed_out_runner(command, **kwargs):
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    with pytest.raises(
        DbtRemoteCleanupError,
        match="could not be confirmed stopped",
    ):
        run_dbt_build(
            config,
            pipeline_run_id="batch-20260826-20260826-0123456789abcdef",
            orchestrator_run_id="manual__cleanup-interrupted",
            command_runner=timed_out_runner,
            query_controller=controller,
        )

    assert controller.call_count == 1


@pytest.mark.parametrize("status", ["fail", "warn", "skipped", "unknown"])
def test_run_dbt_build_rejects_unsuccessful_artifact_status(
    tmp_path: Path, status: str
) -> None:
    config = _config(tmp_path)

    def misleading_runner(command, **kwargs):
        target_path = Path(kwargs["env"]["DBT_TARGET_PATH"])
        _write_success_artifact(target_path)
        artifact_path = target_path / "run_results.json"
        artifact = json.loads(artifact_path.read_text())
        artifact["results"][1]["status"] = status
        artifact_path.write_text(json.dumps(artifact))
        return subprocess.CompletedProcess(command, 0)

    with pytest.raises(DbtBuildError, match=f"{status}=1"):
        run_dbt_build(
            config,
            pipeline_run_id="batch-20260826-20260826-0123456789abcdef",
            orchestrator_run_id="manual__misleading",
            command_runner=misleading_runner,
        )


def test_run_dbt_build_rejects_empty_success_artifact(tmp_path: Path) -> None:
    config = _config(tmp_path)

    def empty_runner(command, **kwargs):
        target_path = Path(kwargs["env"]["DBT_TARGET_PATH"])
        _write_success_artifact(target_path)
        artifact_path = target_path / "run_results.json"
        artifact = json.loads(artifact_path.read_text())
        artifact["results"] = []
        artifact_path.write_text(json.dumps(artifact))
        return subprocess.CompletedProcess(command, 0)

    with pytest.raises(DbtBuildError, match="without building or testing"):
        run_dbt_build(
            config,
            pipeline_run_id="batch-20260826-20260826-0123456789abcdef",
            orchestrator_run_id="manual__empty",
            command_runner=empty_runner,
        )


@pytest.mark.skipif(not hasattr(os, "killpg"), reason="requires POSIX process groups")
def test_managed_command_stops_child_process_group_before_timeout_returns(
    tmp_path: Path,
) -> None:
    stopped_path = tmp_path / "child-stopped"
    ready_path = tmp_path / "child-ready"
    child_script = tmp_path / "child.py"
    parent_script = tmp_path / "parent.py"
    child_script.write_text(
        "import signal, sys, time\n"
        "from pathlib import Path\n"
        "stopped, ready = map(Path, sys.argv[1:3])\n"
        "def stop(*_args):\n"
        "    stopped.write_text('stopped')\n"
        "    raise SystemExit(0)\n"
        "signal.signal(signal.SIGTERM, stop)\n"
        "ready.write_text('ready')\n"
        "while True:\n"
        "    time.sleep(1)\n"
    )
    parent_script.write_text(
        "import subprocess, sys, time\n"
        "from pathlib import Path\n"
        "child, stopped, ready = sys.argv[1:4]\n"
        "subprocess.Popen([sys.executable, child, stopped, ready])\n"
        "while not Path(ready).exists():\n"
        "    time.sleep(0.01)\n"
        "while True:\n"
        "    time.sleep(1)\n"
    )

    with pytest.raises(subprocess.TimeoutExpired):
        _managed_command(
            [
                sys.executable,
                str(parent_script),
                str(child_script),
                str(stopped_path),
                str(ready_path),
            ],
            cwd=str(tmp_path),
            env={"PATH": os.environ.get("PATH", "")},
            check=False,
            timeout=1,
        )

    deadline = time.monotonic() + 2
    while not stopped_path.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert stopped_path.read_text() == "stopped"


def test_local_process_group_that_survives_sigkill_is_nonretryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Process:
        pid = 12345

        def poll(self):
            return None

    signals: list[int] = []
    monkeypatch.setattr(
        dbt_build.os,
        "killpg",
        lambda _process_group_id, sent_signal: signals.append(sent_signal),
    )

    with pytest.raises(
        DbtRemoteCleanupError,
        match="did not stop after SIGKILL",
    ):
        dbt_build._stop_process_group(Process(), grace_seconds=0)

    assert signals == [dbt_build.signal.SIGTERM, dbt_build.signal.SIGKILL]


def test_interrupted_local_process_cleanup_is_nonretryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Process:
        pid = 12345

        def wait(self, *, timeout):
            raise subprocess.TimeoutExpired(["dbt", "build"], timeout)

    monkeypatch.setattr(
        dbt_build.subprocess,
        "Popen",
        lambda *_args, **_kwargs: Process(),
    )

    def interrupted_cleanup(_process):
        raise KeyboardInterrupt

    monkeypatch.setattr(dbt_build, "_stop_process_group", interrupted_cleanup)

    with pytest.raises(
        DbtRemoteCleanupError,
        match="cleanup was interrupted or could not be confirmed",
    ):
        _managed_command(
            ["dbt", "build"],
            cwd="/tmp",
            env={},
            check=False,
            timeout=30,
        )


def test_trino_cleanup_cancels_only_matching_active_attempt_and_waits_until_quiet(
) -> None:
    attempt_tag = "steam-delivery-dbt-0123456789abcdefabcd-try-02"
    query_lists = [
        [
            {
                "queryId": "matching_active_1",
                "state": "RUNNING",
                "session": {"user": "airflow", "clientTags": [attempt_tag]},
            },
            {
                "queryId": "another_attempt",
                "state": "RUNNING",
                "session": {"user": "airflow", "clientTags": ["another-tag"]},
            },
            {
                "queryId": "another_user",
                "state": "RUNNING",
                "session": {"user": "analyst", "clientTags": [attempt_tag]},
            },
            {
                "queryId": "already_finished",
                "state": "FINISHED",
                "session": {"user": "airflow", "clientTags": [attempt_tag]},
            },
        ],
        [
            {
                "queryId": "matching_active_1",
                "state": "FAILED",
                "session": {"user": "airflow", "clientTags": [attempt_tag]},
            },
            {
                "queryId": "matching_active_2",
                "state": "QUEUED",
                "session": {"user": "airflow", "clientTags": [attempt_tag]},
            },
        ],
        [],
        [],
        [],
    ]
    cancelled_urls: list[str] = []

    class Response:
        def __init__(self, payload: bytes = b""):
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self) -> bytes:
            return self.payload

    def urlopen(request, *, timeout):
        assert timeout > 0
        if request.get_method() == "GET":
            assert request.full_url == "http://trino:8080/v1/query"
            assert request.get_header("X-trino-user") == "airflow"
            return Response(json.dumps(query_lists.pop(0)).encode())
        assert request.get_method() == "DELETE"
        cancelled_urls.append(request.full_url)
        return Response()

    clock_value = 0.0

    def monotonic() -> float:
        nonlocal clock_value
        clock_value += 0.01
        return clock_value

    controller = TrinoAttemptQueryController(
        "http://trino:8080",
        user="airflow",
        attempt_tag=attempt_tag,
        timeout_seconds=5,
        poll_seconds=0.01,
        quiet_polls=3,
        urlopen=urlopen,
        monotonic=monotonic,
        sleep=lambda _seconds: None,
    )

    cancelled_query_ids = controller.cancel_and_wait()

    assert cancelled_query_ids == ("matching_active_1", "matching_active_2")
    assert cancelled_urls == [
        "http://trino:8080/v1/query/matching_active_1",
        "http://trino:8080/v1/query/matching_active_2",
    ]
    assert query_lists == []


def test_workflow_requires_matching_published_coverage_and_persists_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = plan_run(
        start_date="2026-08-26",
        end_date="2026-08-26",
        seed=20260828,
        generation_time_utc="2026-08-28T12:00:00Z",
        orchestrator_run_id="manual__dbt-workflow-test",
        environment={
            "R2_RAW_BUCKET": "raw-test",
            "PIPELINE_WORK_ROOT": str(tmp_path / "work"),
            "TRINO_URL": "http://trino:8080",
        },
    )
    coverage = {
        "pipeline_run_id": plan["pipeline_run_id"],
        "table": '"r2"."industrial_energy_control"."batch_run_coverage"',
        "coverage_payload_sha256": "a" * 64,
        "disposition": "created",
    }
    expected = {
        "pipeline_run_id": plan["pipeline_run_id"],
        "status": "succeeded",
        "result_count": 370,
        "dbt_step_name": "build_dimensional_mart",
        "dbt_command_name": "build",
        "dbt_selectors": [],
    }

    def fake_run(config, **kwargs):
        assert kwargs["step"] == DbtCommandStep(
            name="build_dimensional_mart",
            command_name="build",
        )
        assert kwargs["pipeline_run_id"] == plan["pipeline_run_id"]
        assert kwargs["orchestrator_run_id"] == plan["orchestrator_run_id"]
        assert kwargs["attempt_number"] == 2
        return dict(expected)

    monkeypatch.setattr(dbt_build, "run_dbt_command_step", fake_run)
    result = build_dimensional_mart(
        plan,
        coverage,
        attempt_number=2,
        environment={
            "DBT_PROJECT_DIR": str(tmp_path),
            "DBT_PROFILES_DIR": str(tmp_path),
            "DBT_TARGET_PATH": str(tmp_path / "target"),
            "DBT_LOG_PATH": str(tmp_path / "logs"),
        },
    )

    assert result["pipeline_run_id"] == expected["pipeline_run_id"]
    assert result["status"] == "succeeded"
    assert result["result_count"] == 370
    assert result["coverage_payload_sha256"] == "a" * 64
    assert result["result_path"].endswith(
        "/build_dimensional_mart/try-02.result.json"
    )
    persisted = json.loads(Path(result["result_path"]).read_text())
    assert persisted == result

    with pytest.raises(PipelineError, match="another pipeline run"):
        build_dimensional_mart(
            plan,
            {**coverage, "pipeline_run_id": "batch-wrong"},
        )
    with pytest.raises(PipelineError, match="successful coverage"):
        build_dimensional_mart(
            plan,
            {**coverage, "disposition": "conflict"},
        )
    with pytest.raises(PipelineError, match="invalid canonical payload hash"):
        build_dimensional_mart(
            plan,
            {**coverage, "coverage_payload_sha256": "not-a-hash"},
        )


def test_workflow_persists_each_dbt_step_in_its_own_checkpoint_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = plan_run(
        start_date="2026-08-26",
        end_date="2026-08-26",
        seed=20260828,
        generation_time_utc="2026-08-28T12:00:00Z",
        orchestrator_run_id="manual__split-dbt-workflow-test",
        environment={
            "R2_RAW_BUCKET": "raw-test",
            "PIPELINE_WORK_ROOT": str(tmp_path / "work"),
            "TRINO_URL": "http://trino:8080",
        },
    )
    coverage = {
        "pipeline_run_id": plan["pipeline_run_id"],
        "coverage_payload_sha256": "b" * 64,
        "disposition": "reused",
    }
    captured_steps: list[DbtCommandStep] = []

    def fake_run(_config, **kwargs):
        step = kwargs["step"]
        captured_steps.append(step)
        return {
            "pipeline_run_id": plan["pipeline_run_id"],
            "dbt_step_name": step.name,
            "dbt_selectors": list(step.selectors),
            "status": "succeeded",
            "result_count": 12,
        }

    monkeypatch.setattr(dbt_build, "run_dbt_command_step", fake_run)
    common_arguments = {
        "plan_value": plan,
        "coverage_result_value": coverage,
        "attempt_number": 2,
        "environment": {
            "DBT_PROJECT_DIR": str(tmp_path),
            "DBT_PROFILES_DIR": str(tmp_path),
            "DBT_TARGET_PATH": str(tmp_path / "target"),
            "DBT_LOG_PATH": str(tmp_path / "logs"),
        },
    }
    staging = build_dimensional_mart_step(
        **common_arguments,
        step_name="prepare_and_test_loaded_data",
    )
    marts = build_dimensional_mart_step(
        **common_arguments,
        step_name="build_dimension_tables",
    )

    assert captured_steps == [
        DIMENSIONAL_MART_DBT_STEPS[0],
        DIMENSIONAL_MART_DBT_STEPS[4],
    ]
    assert staging["result_path"].endswith(
        "/prepare_and_test_loaded_data/try-02.result.json"
    )
    assert marts["result_path"].endswith(
        "/build_dimension_tables/try-02.result.json"
    )
    assert staging["result_path"] != marts["result_path"]
    assert json.loads(Path(staging["result_path"]).read_text()) == staging
    assert json.loads(Path(marts["result_path"]).read_text()) == marts

    with pytest.raises(PipelineError, match="unknown governed dbt step"):
        build_dimensional_mart_step(
            **common_arguments,
            step_name="skip_required_tests",
        )


def test_mart_project_keeps_the_full_rebuild_baseline() -> None:
    repository_root = Path(__file__).resolve().parents[2]
    model_files = list((repository_root / "transformations" / "models").rglob("*"))
    model_text = "\n".join(
        path.read_text().lower()
        for path in model_files
        if path.suffix in {".sql", ".yml", ".yaml"}
    )

    assert re.search(
        r"materialized\s*[:=]\s*['\"]?incremental",
        model_text,
    ) is None


def test_dag_has_six_restartable_dbt_checkpoints_with_safe_retry_boundaries() -> None:
    repository_root = Path(__file__).resolve().parents[2]
    dag_path = repository_root / "orchestration" / "dags" / "steam_delivery_data_pipeline.py"
    module = ast.parse(dag_path.read_text())
    pipeline_function = next(
        node
        for node in ast.walk(module)
        if isinstance(node, ast.FunctionDef)
        and node.name == "steam_delivery_data_pipeline"
    )
    run_checkpoint_function = next(
        node
        for node in ast.walk(pipeline_function)
        if isinstance(node, ast.FunctionDef) and node.name == "run_dbt_checkpoint"
    )
    cleanup_handler = next(
        node
        for node in ast.walk(run_checkpoint_function)
        if isinstance(node, ast.ExceptHandler)
        and isinstance(node.type, ast.Name)
        and node.type.id == "DbtRemoteCleanupError"
    )

    assert any(
        isinstance(node, ast.Raise)
        and isinstance(node.exc, ast.Call)
        and isinstance(node.exc.func, ast.Name)
        and node.exc.func.id == "AirflowFailException"
        for node in ast.walk(cleanup_handler)
    )

    task_decorator = next(
        decorator
        for decorator in run_checkpoint_function.decorator_list
        if isinstance(decorator, ast.Call)
        and isinstance(decorator.func, ast.Name)
        and decorator.func.id == "task"
    )
    execution_timeout = next(
        keyword.value
        for keyword in task_decorator.keywords
        if keyword.arg == "execution_timeout"
    )
    assert isinstance(execution_timeout, ast.Call)
    assert isinstance(execution_timeout.func, ast.Name)
    assert execution_timeout.func.id == "timedelta"
    timeout_minutes = next(
        keyword.value
        for keyword in execution_timeout.keywords
        if keyword.arg == "minutes"
    )
    assert isinstance(timeout_minutes, ast.Constant)
    assert timeout_minutes.value == 125

    retry_count = next(
        keyword.value
        for keyword in task_decorator.keywords
        if keyword.arg == "retries"
    )
    assert isinstance(retry_count, ast.Constant)
    assert retry_count.value == 1

    pool_name = next(
        keyword.value
        for keyword in task_decorator.keywords
        if keyword.arg == "pool"
    )
    assert isinstance(pool_name, ast.Constant)
    assert pool_name.value == "iceberg_writer"

    checkpoint_assignments: list[tuple[str, str, str]] = []
    for node in pipeline_function.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
            continue
        outer_call = node.value
        if not isinstance(outer_call.func, ast.Call):
            continue
        override_call = outer_call.func
        if (
            not isinstance(override_call.func, ast.Attribute)
            or override_call.func.attr != "override"
            or not isinstance(override_call.func.value, ast.Name)
            or override_call.func.value.id != "run_dbt_checkpoint"
        ):
            continue
        assignment_target = node.targets[0]
        assert isinstance(assignment_target, ast.Name)
        task_id = next(
            keyword.value
            for keyword in override_call.keywords
            if keyword.arg == "task_id"
        )
        assert isinstance(task_id, ast.Constant)
        step_name = outer_call.args[2]
        assert isinstance(step_name, ast.Constant)
        checkpoint_assignments.append(
            (assignment_target.id, str(task_id.value), str(step_name.value))
        )

    assert checkpoint_assignments == [
        (
            "prepare_loaded_data",
            "prepare_and_test_loaded_data_with_dbt",
            "prepare_and_test_loaded_data",
        ),
        (
            "prepare_delivery_calculations",
            "prepare_and_test_delivery_calculations_with_dbt",
            "prepare_and_test_delivery_calculations",
        ),
        (
            "build_current_fact",
            "build_current_delivery_fact_with_dbt",
            "build_current_delivery_fact",
        ),
        (
            "build_history_fact",
            "build_delivery_history_fact_with_dbt",
            "build_delivery_history_fact",
        ),
        (
            "build_dimensions",
            "build_dimension_tables_with_dbt",
            "build_dimension_tables",
        ),
        (
            "test_complete_mart",
            "test_complete_dimensional_mart_with_dbt",
            "test_complete_dimensional_mart",
        ),
    ]

    task_function_count = sum(
        1
        for node in pipeline_function.body
        if isinstance(node, ast.FunctionDef)
        and any(
            isinstance(decorator, ast.Call)
            and isinstance(decorator.func, ast.Name)
            and decorator.func.id == "task"
            for decorator in node.decorator_list
        )
    )
    # Seven source/control task definitions are instantiated once. The generic
    # dbt definition is instantiated through six named overrides.
    assert task_function_count - 1 + len(checkpoint_assignments) == 13

    def flattened_dependencies(node: ast.AST) -> list[str]:
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift):
            return flattened_dependencies(node.left) + flattened_dependencies(
                node.right
            )
        if isinstance(node, ast.Name):
            return [node.id]
        return []

    dependency_chains = [
        flattened_dependencies(node.value)
        for node in pipeline_function.body
        if isinstance(node, ast.Expr)
    ]
    assert [name for name, _task_id, _step_name in checkpoint_assignments] in (
        dependency_chains
    )
