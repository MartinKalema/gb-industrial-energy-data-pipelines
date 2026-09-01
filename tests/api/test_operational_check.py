from __future__ import annotations

import pytest

from apps.api.operational_check import ProbeResult, run_operational_check


def test_operational_check_passes_and_emits_bounded_capacity_evidence() -> None:
    def successful_probe(_url: str, _timeout: float) -> ProbeResult:
        return ProbeResult(
            successful=True,
            status_code=200,
            duration_ms=12.5,
        )

    report = run_operational_check(
        url="http://127.0.0.1:8000/health/ready?internal_filter=hidden",
        request_count=20,
        concurrency=4,
        timeout_seconds=2,
        maximum_p95_ms=100,
        maximum_error_rate=0,
        probe=successful_probe,
    )

    assert report["passed"] is True
    assert report["target"] == "http://127.0.0.1:8000/health/ready"
    assert "internal_filter" not in str(report)
    assert report["successful_request_count"] == 20
    assert report["failed_request_count"] == 0
    assert report["latency_ms"]["p95"] == 12.5
    assert report["status_codes"] == {"200": 20}
    assert report["error_types"] == {}


def test_operational_check_exits_as_failed_evidence_when_thresholds_fail() -> None:
    def failed_probe(_url: str, _timeout: float) -> ProbeResult:
        return ProbeResult(
            successful=False,
            status_code=503,
            duration_ms=250.0,
            error_type="http_error",
        )

    report = run_operational_check(
        url="http://127.0.0.1:8000/health/ready",
        request_count=5,
        concurrency=1,
        timeout_seconds=2,
        maximum_p95_ms=100,
        maximum_error_rate=0,
        probe=failed_probe,
    )

    assert report["passed"] is False
    assert report["error_rate"] == 1.0
    assert report["latency_ms"]["p95"] == 250.0
    assert report["status_codes"] == {"503": 5}
    assert report["error_types"] == {"http_error": 5}


@pytest.mark.parametrize(
    "overrides",
    [
        {"request_count": 0},
        {"request_count": 33, "concurrency": 33},
        {"maximum_error_rate": 1.1},
        {"url": "file:///etc/passwd"},
        {"url": "http://user:secret@127.0.0.1/health/ready"},
    ],
)
def test_operational_check_rejects_unsafe_or_unbounded_inputs(
    overrides: dict[str, object],
) -> None:
    arguments: dict[str, object] = {
        "url": "http://127.0.0.1:8000/health/ready",
        "request_count": 1,
        "concurrency": 1,
        "timeout_seconds": 2,
        "maximum_p95_ms": 100,
        "maximum_error_rate": 0,
    }
    arguments.update(overrides)

    with pytest.raises(ValueError):
        run_operational_check(**arguments)  # type: ignore[arg-type]
