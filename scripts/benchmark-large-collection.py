"""Measure lexical search and a small edit in a multi-GiB MinIO collection."""

import argparse
import json
import os
from pathlib import Path
import re
import runpy
import subprocess
import tempfile
import time
import uuid


SCRIPTS = Path(__file__).parent
MCP_CHECK = runpy.run_path(str(SCRIPTS / "check-local-mcp.py"))
INGESTION = runpy.run_path(str(SCRIPTS / "benchmark-ingestion.py"))
COLLECTION = "large-collection"


def tail(marker):
    return f"\n{marker} café 🦀. Verified from the end of the source.\n".encode()


def measure(config, args, scratch):
    size = args.mib * INGESTION["MIB"]
    original_marker = "spectrocalibrationoriginal"
    edited_marker = "spectrocalibrationrevision"
    assert len(tail(original_marker)) == len(tail(edited_marker))

    def cli(addresses, *command):
        result = subprocess.run(
            [args.bin_dir / "sanctum-cli", "--endpoint", f"http://{addresses['grpc']}", *command],
            capture_output=True, text=True, timeout=args.timeout,
        )
        if result.returncode:
            raise RuntimeError(f"{command[0]}: {result.stderr.strip()}")
        return result.stdout.rstrip("\n")

    def upload(addresses, path, *replacement):
        operation = str(uuid.uuid4())
        started = time.monotonic()
        reply = cli(addresses, "upload", COLLECTION, path, "--background", "--request-id", operation, *replacement)
        accepted_at = time.monotonic()
        assert reply.startswith(f"{operation} INGESTION_STATE_ACCEPTED "), reply
        while True:
            status = cli(addresses, "job", COLLECTION, operation)
            now = time.monotonic()
            if f"INGESTION_STATE_TEXT_READY {size}/{size} bytes" in status:
                return {"accept_seconds": round(accepted_at - started, 3),
                        "text_ready_seconds": round(now - started, 3)}
            if now - started >= args.timeout or "INGESTION_STATE_FAILED" in status:
                raise RuntimeError(status)
            time.sleep(.2)

    def search(addresses, marker):
        started = time.monotonic()
        result = cli(addresses, "search", COLLECTION, marker, "--mode", "lexical", "--limit", "1")
        elapsed = (time.monotonic() - started) * 1000
        reference, excerpt = result.split("\n", 1)
        source, revision, ordinal, interval, _ = reference.split(maxsplit=4)
        start, end = map(int, interval.removeprefix("[").removesuffix(")").split(".."))
        assert marker in excerpt and 0 < start < end == size, (start, end, excerpt)
        return source, revision, int(ordinal), start, end, round(elapsed, 2)

    def usage(addresses):
        result = cli(addresses, "usage", COLLECTION)
        match = re.match(r"(\d+) bytes in (\d+) files", result)
        assert match, result
        return {"bytes": int(match[1]), "objects": int(match[2])}

    server = args.bin_dir / "sanctum-server"
    config = config | {"standalone_object_worker": True}
    last_path = scratch / f"observations-{args.sources - 1:03d}.txt"
    sources = []
    with MCP_CHECK["hosted_process"](server, config) as addresses:
        cli(addresses, "create", COLLECTION)
        for index in range(args.sources):
            path = scratch / f"observations-{index:03d}.txt"
            marker = original_marker if path == last_path else f"spectrocalibrationsource{index:03d}"
            INGESTION["make_input"](path, size, seed=20261113 + index, tail=tail(marker))
            result = upload(addresses, path)
            sources.append(result)
            print(f"Published source {index + 1}/{args.sources} in {result['text_ready_seconds']:.3f}s", flush=True)
            if path != last_path:
                path.unlink()
        before = usage(addresses)

    with MCP_CHECK["hosted_process"](server, config) as addresses:
        source, revision, _, _, _, cold = search(addresses, original_marker)
        warm = [search(addresses, original_marker)[-1] for _ in range(3)]
        with last_path.open("r+b") as file:
            file.seek(size - len(tail(original_marker)))
            file.write(tail(edited_marker))
        replacement = upload(addresses, last_path, "--replace-source", source, "--expected-revision", revision)
        current, next_revision, ordinal, start, end, _ = search(addresses, edited_marker)
        assert current == source and next_revision != revision
        assert not cli(addresses, "search", COLLECTION, original_marker, "--mode", "lexical", "--limit", "1")
        rpc = MCP_CHECK["Rpc"](MCP_CHECK["Http"](f"http://{addresses['mcp']}/mcp"))
        rpc.initialize()
        fetched = rpc.tool("fetch_chunk", collection=COLLECTION, source_id=current,
                           revision_id=next_revision, ordinal=ordinal)
        assert (fetched["start_byte"], fetched["end_byte"]) == (start, end)
        with last_path.open("rb") as file:
            file.seek(start)
            assert fetched["excerpt"].encode() == file.read(end - start)
        after = usage(addresses)

    changed_bytes = sum(left != right for left, right in zip(tail(original_marker), tail(edited_marker)))
    return {
        "source_count": args.sources,
        "source_bytes": size * args.sources,
        "initial_publications": sources,
        "cold_search_ms": cold,
        "warm_search_ms": warm,
        "replacement": replacement,
        "replacement_changed_bytes": changed_bytes,
        "verified_tail_range": [start, end],
        "published_generation": fetched["database_generation"],
        "usage_before": before,
        "usage_after": after,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", default=Path("target/release"), type=Path)
    parser.add_argument("--sources", default=12, type=int)
    parser.add_argument("--mib", default=200, type=int)
    parser.add_argument("--timeout", default=600, type=int)
    args = parser.parse_args()
    if min(args.sources, args.mib, args.timeout) < 1:
        parser.error("source count, input size, and timeout must be positive")
    args.bin_dir = args.bin_dir.resolve()
    config = json.loads(os.environ["SANCTUM_CONFIG_JSON"])
    if not config["object_storage"].get("prefix"):
        parser.error("use a dedicated object storage prefix")
    config.update(grpc_listen="127.0.0.1:0", embedding=None)
    with tempfile.TemporaryDirectory(prefix="sanctum-collection-benchmark-") as temp:
        print(json.dumps(measure(config, args, Path(temp)), indent=2))


if __name__ == "__main__":
    main()
