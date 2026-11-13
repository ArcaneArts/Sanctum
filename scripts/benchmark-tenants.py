"""Measure concurrent hosted operations among mostly idle tenants."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import re
import runpy
import subprocess
import tempfile
import time
import uuid


MCP_CHECK = runpy.run_path(str(Path(__file__).with_name("check-local-mcp.py")))
COLLECTION = "notes"


def timing(samples):
    ordered = sorted(samples)
    return {
        "count": len(ordered),
        "p50_ms": round(ordered[(len(ordered) - 1) // 2], 2),
        "p95_ms": round(ordered[(len(ordered) * 95 + 99) // 100 - 1], 2),
        "max_ms": round(ordered[-1], 2),
    }


def measure(config, args, scratch):
    hosted_process = MCP_CHECK["hosted_process"]
    with hosted_process(args.bin_dir / "sanctum-server", config) as addresses:
        endpoint = f"http://{addresses['grpc']}"

        def cli(tenant, *command):
            result = subprocess.run(
                [args.bin_dir / "sanctum-cli", "--endpoint", endpoint, "--tenant", tenant, *command],
                capture_output=True, text=True, timeout=90,
            )
            if result.returncode:
                raise RuntimeError(f"{tenant} {command[0]}: {result.stderr.strip()}")
            return result.stdout.strip()

        def tenant(index):
            return f"tenant-{index:05d}"

        def create(index):
            return cli(tenant(index), "create", COLLECTION)

        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=args.setup_concurrency) as pool:
            assert all(result == COLLECTION for result in pool.map(create, range(args.tenants)))
        setup_seconds = time.monotonic() - started

        def write(index, revision, source=None, expected=None):
            path = scratch / f"{tenant(index)}.txt"
            marker = f"marker{index:05d}revision{revision:02d}"
            lines = (f"{line:06d} {marker} calibration report for {tenant(index)}.\n"
                     for line in range(args.lines))
            with path.open("w", encoding="utf-8") as file:
                file.writelines(lines)
            command = ["upload", COLLECTION, path, "--background", "--request-id", str(uuid.uuid4())]
            if source:
                command.extend(["--replace-source", source, "--expected-revision", expected])
            started = time.monotonic()
            accepted = cli(tenant(index), *command)
            accepted_at = time.monotonic()
            operation = accepted.split()[0]
            while True:
                status = cli(tenant(index), "job", COLLECTION, operation)
                if "INGESTION_STATE_TEXT_READY" in status:
                    break
                if "INGESTION_STATE_FAILED" in status or time.monotonic() - started > 120:
                    raise RuntimeError(status)
                time.sleep(.1)
            return marker, (accepted_at - started) * 1000, (time.monotonic() - started) * 1000

        def search(index, marker):
            started = time.monotonic()
            result = cli(tenant(index), "search", COLLECTION, marker, "--mode", "lexical", "--limit", "1")
            elapsed = (time.monotonic() - started) * 1000
            assert marker in result, (index, marker, result)
            source, revision, *_ = result.splitlines()[0].split()
            return source, revision, elapsed

        def active(index):
            marker, _, _ = write(index, 0)
            source, revision, first_search = search(index, marker)
            warm = [search(index, marker)[2] for _ in range(args.searches)]
            writes = []
            for number in range(1, args.updates + 1):
                previous = marker
                marker, accept, publish = write(index, number, source, revision)
                current_source, revision, _ = search(index, marker)
                assert current_source == source
                assert not cli(tenant(index), "search", COLLECTION, previous, "--mode", "lexical", "--limit", "1")
                writes.append((accept, publish))
            return first_search, warm, writes, marker

        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=args.active) as pool:
            runs = list(pool.map(active, range(args.active)))
        workload_seconds = time.monotonic() - started
        idle = args.tenants - 1
        assert cli(tenant(idle), "list") == COLLECTION
        assert not cli(tenant(idle), "search", COLLECTION, runs[0][3], "--mode", "lexical")
        for index, run in enumerate(runs):
            other = (index + 1) % args.active
            assert not cli(tenant(other), "search", COLLECTION, run[3], "--mode", "lexical")

        started = time.monotonic()
        usage = cli(tenant(0), "usage", "--cluster")
        usage_ms = (time.monotonic() - started) * 1000
        match = re.match(r"(\d+) bytes in (\d+) files", usage)
        assert match, usage
        return {
            "tenants": args.tenants,
            "active_tenants": args.active,
            "setup_concurrency": args.setup_concurrency,
            "source_lines": args.lines,
            "catalog_setup_seconds": round(setup_seconds, 3),
            "concurrent_workload_seconds": round(workload_seconds, 3),
            "first_search_after_publication": timing([run[0] for run in runs]),
            "warm_search": timing([sample for run in runs for sample in run[1]]),
            "replacement_accept": timing([accept for run in runs for accept, _ in run[2]]),
            "replacement_text_ready": timing([publish for run in runs for _, publish in run[2]]),
            "cluster_bytes": int(match[1]),
            "cluster_objects": int(match[2]),
            "cluster_usage_ms": round(usage_ms, 2),
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", default=Path("target/release"), type=Path)
    parser.add_argument("--tenants", default=1000, type=int)
    parser.add_argument("--active", default=4, type=int)
    parser.add_argument("--setup-concurrency", default=8, type=int)
    parser.add_argument("--lines", default=16000, type=int)
    parser.add_argument("--searches", default=8, type=int)
    parser.add_argument("--updates", default=2, type=int)
    args = parser.parse_args()
    if not 2 <= args.active < args.tenants or min(args.setup_concurrency, args.lines, args.searches, args.updates) < 1:
        parser.error("choose at least two active tenants, one idle tenant, and positive workload counts")
    args.bin_dir = args.bin_dir.resolve()
    config = json.loads(os.environ["SANCTUM_CONFIG_JSON"])
    if not config["object_storage"].get("prefix"):
        parser.error("use a dedicated object storage prefix")
    config.update(grpc_listen="127.0.0.1:0", standalone_object_worker=True, embedding=None)
    with tempfile.TemporaryDirectory(prefix="sanctum-tenant-benchmark-") as temp:
        print(json.dumps(measure(config, args, Path(temp)), indent=2))


if __name__ == "__main__":
    main()
