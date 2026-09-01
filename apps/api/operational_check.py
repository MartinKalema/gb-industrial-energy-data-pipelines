"""Alert-ready API probe and small, bounded local load check."""

from __future__ import annotations

import argparse
import json
import math
import re
import time
from collections import Counter
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


@dataclass(frozen=True, slots=True)
class ProbeResult:
    successful: bool
    status_code: int | None
    duration_ms: float
    error_type: str | None = None


def _percentile(values: Sequence[float], percentile: int) -> float:
    """Return the nearest-rank percentile for a non-empty bounded sample."""

    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(1, math.ceil((percentile / 100) * len(ordered)))
    return round(ordered[rank - 1], 3)


def probe_once(
    url: str,
    timeout_seconds: float,
    *,
    expected_json_status: str = "ready",
    request_headers: dict[str, str] | None = None,
) -> ProbeResult:
    started = time.perf_counter()
    status_code: int | None = None
    try:
        headers = {
            "Accept": "application/json",
            "User-Agent": "industrial-energy-operational-check/1.0",
        }
        headers.update(request_headers or {})
        request = Request(
            url,
            headers=headers,
        )
        with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
            status_code = int(response.status)
            payload = json.loads(response.read())
        successful = (
            status_code == 200
            and isinstance(payload, dict)
            and (
                expected_json_status == "any"
                or payload.get("status") == expected_json_status
            )
        )
        error_type = None if successful else "unexpected_response"
    except HTTPError as error:
        status_code = int(error.code)
        successful = False
        error_type = "http_error"
    except URLError:
        successful = False
        error_type = "connection_error"
    except TimeoutError:
        successful = False
        error_type = "timeout"
    except (json.JSONDecodeError, OSError, ValueError):
        successful = False
        error_type = "invalid_response"
    return ProbeResult(
        successful=successful,
        status_code=status_code,
        duration_ms=round((time.perf_counter() - started) * 1_000, 3),
        error_type=error_type,
    )


def run_operational_check(
    *,
    url: str,
    request_count: int,
    concurrency: int,
    timeout_seconds: float,
    maximum_p95_ms: float,
    maximum_error_rate: float,
    expected_json_status: str = "ready",
    request_headers: dict[str, str] | None = None,
    probe: Callable[[str, float], ProbeResult] | None = None,
) -> dict[str, Any]:
    if request_count < 1 or request_count > 1_000:
        raise ValueError("request_count must be between 1 and 1000")
    if concurrency < 1 or concurrency > min(32, request_count):
        raise ValueError("concurrency must be between 1 and min(32, request_count)")
    if timeout_seconds <= 0 or maximum_p95_ms <= 0:
        raise ValueError("timeouts and latency limits must be greater than zero")
    if not 0 <= maximum_error_rate <= 1:
        raise ValueError("maximum_error_rate must be between zero and one")

    split_url = urlsplit(url)
    if (
        split_url.scheme not in {"http", "https"}
        or not split_url.hostname
        or split_url.username is not None
        or split_url.password is not None
    ):
        raise ValueError("url must be an HTTP(S) URL without embedded credentials")

    if probe is None:
        def configured_probe(target: str, timeout: float) -> ProbeResult:
            return probe_once(
                target,
                timeout,
                expected_json_status=expected_json_status,
                request_headers=request_headers,
            )

        probe = configured_probe

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        results = list(
            executor.map(
                lambda _position: probe(url, timeout_seconds),
                range(request_count),
            )
        )
    elapsed_ms = round((time.perf_counter() - started) * 1_000, 3)

    successful_count = sum(result.successful for result in results)
    failed_count = request_count - successful_count
    error_rate = failed_count / request_count
    durations = [result.duration_ms for result in results]
    p95_ms = _percentile(durations, 95)
    passed = error_rate <= maximum_error_rate and p95_ms <= maximum_p95_ms
    error_types = Counter(
        result.error_type for result in results if result.error_type is not None
    )
    status_codes = Counter(
        str(result.status_code)
        for result in results
        if result.status_code is not None
    )

    return {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "passed": passed,
        "target": f"{split_url.scheme}://{split_url.netloc}{split_url.path}",
        "request_count": request_count,
        "concurrency": concurrency,
        "successful_request_count": successful_count,
        "failed_request_count": failed_count,
        "error_rate": round(error_rate, 6),
        "elapsed_ms": elapsed_ms,
        "requests_per_second": round(
            request_count / (elapsed_ms / 1_000), 3
        )
        if elapsed_ms > 0
        else 0.0,
        "latency_ms": {
            "minimum": round(min(durations), 3),
            "p50": _percentile(durations, 50),
            "p95": p95_ms,
            "p99": _percentile(durations, 99),
            "maximum": round(max(durations), 3),
        },
        "thresholds": {
            "maximum_p95_ms": maximum_p95_ms,
            "maximum_error_rate": maximum_error_rate,
        },
        "status_codes": dict(sorted(status_codes.items())),
        "error_types": dict(sorted(error_types.items())),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Probe an API health endpoint and exit non-zero when availability or "
            "latency limits fail"
        )
    )
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:8000/health/ready",
    )
    parser.add_argument("--requests", type=int, default=1)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    parser.add_argument("--max-p95-ms", type=float, default=2_000.0)
    parser.add_argument("--max-error-rate", type=float, default=0.0)
    parser.add_argument("--expected-json-status", default="ready")
    parser.add_argument(
        "--demo-actor",
        help="Optional local demo actor header; never use this as production auth",
    )
    parser.add_argument("--output", type=Path)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.demo_actor is not None and re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", args.demo_actor
    ) is None:
        _parser().error("--demo-actor has an invalid format")
    try:
        report = run_operational_check(
            url=args.url,
            request_count=args.requests,
            concurrency=args.concurrency,
            timeout_seconds=args.timeout_seconds,
            maximum_p95_ms=args.max_p95_ms,
            maximum_error_rate=args.max_error_rate,
            expected_json_status=args.expected_json_status,
            request_headers=(
                {"X-Demo-Actor": args.demo_actor}
                if args.demo_actor is not None
                else None
            ),
        )
    except ValueError as error:
        _parser().error(str(error))

    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output is not None:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
