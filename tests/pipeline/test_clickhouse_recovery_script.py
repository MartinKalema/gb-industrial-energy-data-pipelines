from __future__ import annotations

import subprocess
from pathlib import Path


def _script() -> Path:
    return (
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "rebuild_clickhouse_serving_copy.sh"
    )


def test_rebuild_script_is_non_destructive_and_requires_confirmation() -> None:
    script = _script()
    source = script.read_text()

    result = subprocess.run(
        [
            str(script),
            "--start-date",
            "2026-08-26",
            "--end-date",
            "2026-08-26",
            "--generation-time-utc",
            "2026-08-28T12:00:00Z",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "without --confirm-rebuild" in result.stderr
    assert "docker volume rm" not in source
    assert "down -v" not in source
    assert "DROP DATABASE" not in source


def test_rebuild_script_validates_bounds_before_starting_docker() -> None:
    result = subprocess.run(
        [
            str(_script()),
            "--start-date",
            "2026-08-26",
            "--end-date",
            "2026-10-01",
            "--generation-time-utc",
            "2026-08-28T12:00:00Z",
            "--confirm-rebuild",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "cannot exceed 31" in result.stderr
    assert "Starting the batch services" not in result.stdout
