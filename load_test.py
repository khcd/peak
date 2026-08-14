#!/usr/bin/env python3
"""Send a paced, mixed-size load to the peak ingest endpoint.

The default run sends 100 valid planar-tenant events over 30 seconds. It deliberately mixes
single-event requests with requests of up to 200 events so the server's size and time-based
batching paths both get exercised.

This uses only the Python standard library:

    INGEST_TOKEN=<planar secret> python3 load_test.py

Use --dry-run or a shorter --total/--duration pair before running the test.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import platform
import random
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_TOTAL = 100
DEFAULT_DURATION = 30.0
DEFAULT_MAX_BATCH = 200
DEFAULT_TIMEOUT = 15.0

# Deliberately produces both one-event and full-size requests, with a few non-full batches to
# ensure the endpoint handles ordinary client batch sizes too. The final batch is clipped to the
# exact requested total.
BATCH_PATTERN = (1, 200, 1, 200, 17, 200, 1, 200, 1, 200)


class LoadTestError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:8081/v2/events",
        help="full ingest URL (default: %(default)s)",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Bearer token; defaults to INGEST_TOKEN",
    )
    parser.add_argument("--total", type=int, default=DEFAULT_TOTAL, help="events to send")
    parser.add_argument(
        "--duration",
        type=float,
        default=DEFAULT_DURATION,
        help="target duration in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--max-batch",
        type=int,
        default=DEFAULT_MAX_BATCH,
        help="maximum events per HTTP request (default: %(default)s)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help="per-request timeout in seconds (default: %(default)s)",
    )
    parser.add_argument("--seed", type=int, default=20260813, help="event mix RNG seed")
    parser.add_argument(
        "--gzip",
        action="store_true",
        help="gzip request bodies to exercise the compressed ingest path",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the request plan without contacting the ingest service",
    )
    args = parser.parse_args()
    if args.total <= 0:
        parser.error("--total must be positive")
    if args.duration <= 0:
        parser.error("--duration must be positive")
    if not 1 <= args.max_batch <= DEFAULT_MAX_BATCH:
        parser.error(f"--max-batch must be between 1 and {DEFAULT_MAX_BATCH}")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if not args.dry_run and not (args.token or os.environ.get("INGEST_TOKEN")):
        parser.error("provide --token or INGEST_TOKEN")
    return args


def planned_batch_sizes(total: int, max_batch: int) -> list[int]:
    full = min(max_batch, DEFAULT_MAX_BATCH)
    sizes: list[int] = []
    remaining = total
    index = 0
    while remaining:
        requested = BATCH_PATTERN[index % len(BATCH_PATTERN)]
        requested = min(full, requested)
        size = min(requested, remaining)
        sizes.append(size)
        remaining -= size
        index += 1
    return sizes


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def make_event(rng: random.Random, install_ids: list[str], service_version: str) -> dict[str, Any]:
    event_name = rng.choices(
        (
            "session_start",
            "live_ping",
            "session_end",
            "feature_used",
            "generation_requested",
            "generation_completed",
            "model_loaded",
        ),
        weights=(20, 10, 10, 20, 15, 15, 10),
        k=1,
    )[0]

    attributes: dict[str, Any]
    if event_name == "session_end":
        attributes = {"duration_ms": rng.randrange(100, 600_000)}
    elif event_name == "feature_used":
        attributes = {"feature": rng.choice(("canvas", "export", "upscale", "batch"))}
    elif event_name == "generation_requested":
        attributes = {
            "backend": rng.choice(("sdcpp", "diffusers")),
            "model": rng.choice(("sdxl", "flux", "controlnet")),
            "steps": rng.randrange(1, 80),
            "width": rng.choice((512, 768, 1024)),
            "height": rng.choice((512, 768, 1024)),
            "sampler": rng.choice(("euler", "ddim", "dpmpp")),
        }
    elif event_name == "generation_completed":
        success = rng.random() >= 0.05
        attributes = {
            "duration_ms": rng.randrange(100, 600_000),
            "success": success,
            "error_kind": None if success else rng.choice(("oom", "model_load", "other")),
            "backend": rng.choice(("sdcpp", "diffusers")),
        }
    elif event_name == "model_loaded":
        attributes = {
            "model": {
                "model": rng.choice(("sdxl", "flux", "controlnet")),
                "load_ms": rng.randrange(10, 30_000),
                "size_mb": rng.randrange(100, 8_000),
            }
        }
    else:
        attributes = {}

    return {
        "event_id": str(uuid.uuid4()),
        "event_name": event_name,
        "schema_version": 1,
        "occurred_at": utc_timestamp(),
        "subject": {"kind": "install", "id": rng.choice(install_ids)},
        "session_id": str(uuid.uuid4()),
        "resource": {
            "service_name": "planar-load-test",
            "service_version": service_version,
            "platform": "python",
            "platform_version": platform.python_version(),
        },
        "attributes": attributes,
    }


def send_batch(
    url: str,
    token: str,
    events: list[dict[str, Any]],
    timeout: float,
    use_gzip: bool,
) -> tuple[int, float]:
    body = json.dumps(events, separators=(",", ":")).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if use_gzip:
        body = gzip.compress(body)
        headers["Content-Encoding"] = "gzip"

    request = Request(url, data=body, headers=headers, method="POST")
    started = time.monotonic()
    try:
        with urlopen(request, timeout=timeout) as response:
            status = response.status
            response_body = response.read()
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")[:500]
        raise LoadTestError(f"HTTP {error.code}: {detail}") from error
    except URLError as error:
        raise LoadTestError(f"request failed: {error.reason}") from error
    elapsed = time.monotonic() - started

    if status != 200:
        raise LoadTestError(f"HTTP {status}: {response_body[:500].decode('utf-8', errors='replace')}")
    try:
        result = json.loads(response_body)
    except json.JSONDecodeError as error:
        raise LoadTestError(f"response was not JSON: {response_body[:200]!r}") from error
    accepted = result.get("accepted")
    rejected = result.get("rejected")
    if accepted != len(events) or rejected:
        raise LoadTestError(
            f"unexpected ingest response: accepted={accepted}, rejected={rejected!r}, "
            f"sent={len(events)}"
        )
    return accepted, elapsed


def print_plan(sizes: list[int], duration: float, max_batch: int) -> None:
    counts = Counter(sizes)
    print(
        "plan: "
        f"{sum(sizes):,} events in {len(sizes):,} requests over {duration:g}s; "
        f"single={counts.get(1, 0):,}, full={counts.get(max_batch, 0):,}, "
        f"other={sum(count for size, count in counts.items() if size not in (1, max_batch)):,}"
    )


def run(args: argparse.Namespace) -> int:
    token = args.token or os.environ.get("INGEST_TOKEN", "")
    sizes = planned_batch_sizes(args.total, args.max_batch)
    print_plan(sizes, args.duration, args.max_batch)
    if args.dry_run:
        return 0

    rng = random.Random(args.seed)
    install_ids = [str(uuid.uuid4()) for _ in range(100)]
    service_version = f"load-test-{Path(sys.argv[0]).stem}"
    sent = 0
    request_count = 0
    accepted = 0
    failures = 0
    latency_total = 0.0
    started = time.monotonic()

    for size in sizes:
        target = started + (sent / args.total) * args.duration
        delay = target - time.monotonic()
        if delay > 0:
            time.sleep(delay)

        events = [make_event(rng, install_ids, service_version) for _ in range(size)]
        try:
            batch_accepted, latency = send_batch(
                args.url,
                token,
                events,
                args.timeout,
                args.gzip,
            )
        except LoadTestError as error:
            failures += 1
            print(f"request {request_count + 1} failed: {error}", file=sys.stderr)
            if failures >= 10:
                print("aborting after 10 request failures", file=sys.stderr)
                return 1
            sent += size
            request_count += 1
            continue

        sent += size
        accepted += batch_accepted
        request_count += 1
        latency_total += latency
        if request_count == 1 or request_count % 10 == 0 or sent == args.total:
            elapsed = max(time.monotonic() - started, 0.001)
            print(
                f"progress: {sent:,}/{args.total:,} events, "
                f"{request_count:,} requests, elapsed={elapsed:.1f}s, "
                f"event_rate={sent / elapsed:.1f}/s, last_batch={size}"
            )

    # The last request represents the final part of the target interval. Wait out the remainder
    # so the reported run really covers the requested five-minute wall-clock window.
    remaining = started + args.duration - time.monotonic()
    if remaining > 0:
        time.sleep(remaining)
    elapsed = max(time.monotonic() - started, 0.001)
    print(
        f"complete: sent={sent:,}, accepted={accepted:,}, failures={failures}, "
        f"elapsed={elapsed:.1f}s, event_rate={accepted / elapsed:.2f}/s, "
        f"request_rate={request_count / elapsed:.2f}/s, "
        f"mean_successful_request_latency="
        f"{(latency_total / max(request_count - failures, 1)) * 1000:.1f}ms"
    )
    return 0 if sent == args.total and accepted == args.total and failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
