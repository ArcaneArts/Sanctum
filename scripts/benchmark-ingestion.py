"""Measure file upload, lexical publication, and exact MCP fetch through real servers."""

import argparse
import json
import os
from pathlib import Path
import random
import re
import runpy
import subprocess
import tempfile
import threading
import time
import uuid


# Reuse the Streamable HTTP client used by the repository integration check.
MCP_CHECK = runpy.run_path(str(Path(__file__).with_name("check-local-mcp.py")))

MIB = 1024 * 1024
COLLECTION = "capacity"
MARKER = "spectrocalibration"
TAIL = f"\n{MARKER} café 🦀. Verified from the end of the source.\n".encode()
WORDS = (
    "sample", "ridge", "calibration", "sensor", "temperature", "quartz", "grain", "crystal",
    "reference", "measurement", "analysis", "elevation", "airflow", "humidity", "spectrum",
    "operator", "revision", "instrument", "reading", "comparison", "assembly", "contour",
    "observation", "distance", "report", "baseline", "signal", "pressure", "archive", "record",
    "settings", "transport", "material", "document", "frequency", "resolution", "validation",
)


def make_input(path, size, *, seed=20261113, tail=TAIL):
    rng = random.Random(seed)
    with path.open("wb") as file:
        remaining = size - len(tail)
        i = 0
        while remaining:
            lines = []
            for _ in range(256):
                lines.append(f"{i:08d} café: {' '.join(rng.choices(WORDS, k=32))}.\n")
                i += 1
            block = "".join(lines).encode()
            if len(block) > remaining:
                text = block[:remaining].decode(errors="ignore").encode()
                block = text + b" " * (remaining - len(text))
            file.write(block)
            remaining -= len(block)
        file.write(tail)
    assert path.stat().st_size == size


def measure(bin_dir, config, path, timeout):
    server = bin_dir / "sanctum-server"
    cli = bin_dir / "sanctum-cli"
    env = os.environ | {"SANCTUM_CONFIG_JSON": json.dumps(config), "SANCTUM_MANAGED_STDIO": "1", "NO_COLOR": "1"}
    with MCP_CHECK["child"]([server], env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True) as process:
        addresses = json.loads(process.stdout.readline())
        command = [cli, "--endpoint", f"http://{addresses['grpc']}"]

        def call(*args):
            return subprocess.check_output([*command, *args], text=True, timeout=timeout)

        stop = threading.Event()
        samples = []

        def sample_rss():
            while not stop.is_set():
                result = subprocess.run(["ps", "-o", "rss=", "-p", str(process.pid)],
                                        text=True, capture_output=True, timeout=5)
                if result.returncode == 0 and result.stdout.strip():
                    samples.append(int(result.stdout.strip()) * 1024)
                stop.wait(0.2)

        call("create", COLLECTION)
        sampler = threading.Thread(target=sample_rss)
        sampler.start()
        try:
            size = path.stat().st_size
            op = str(uuid.uuid4())
            started = time.monotonic()
            accepted = call("upload", COLLECTION, path, "--background", "--request-id", op)
            accepted_at = time.monotonic()
            operation, state, *_ = accepted.split()
            assert state == "INGESTION_STATE_ACCEPTED", accepted
            if config.get("object_storage"):
                assert operation == op, accepted
            while True:
                job = call("job", COLLECTION, operation)
                now = time.monotonic()
                if f"INGESTION_STATE_TEXT_READY {size}/{size} bytes" in job:
                    break
                assert now - started < timeout and "INGESTION_STATE_FAILED" not in job, job
                time.sleep(0.2)
            ready_at = time.monotonic()
        finally:
            stop.set()
            sampler.join()
        rpc = MCP_CHECK["Rpc"](MCP_CHECK["Http"](f"http://{addresses['mcp']}/mcp"))
        rpc.initialize()
        search = rpc.tool("search", collection=COLLECTION, query=MARKER, mode="lexical")
        hit = search["hits"][0]
        assert size - 8192 < hit["start_byte"] < hit["end_byte"] == size, hit
        ref = {key: hit[key] for key in ("tenant", "collection", "source_id", "revision_id", "ordinal")}
        fetch_started = time.monotonic()
        chunk = rpc.tool("fetch_chunk", **ref)
        fetch_ms = (time.monotonic() - fetch_started) * 1000
        assert chunk["start_byte"] == hit["start_byte"] and chunk["end_byte"] == size
        with path.open("rb") as source:
            source.seek(chunk["start_byte"])
            expected = source.read(size - chunk["start_byte"])
        assert chunk["excerpt"].encode() == expected and TAIL in expected
        usage = call("usage", COLLECTION)
        match = re.search(r"(\d+) bytes in (\d+) files", usage)
        assert match, usage
        result = {
            "input_bytes": size,
            "upload_accept_seconds": round(accepted_at - started, 3),
            "accepted_to_text_ready_seconds": round(ready_at - accepted_at, 3),
            "total_seconds": round(ready_at - started, 3),
            "peak_server_rss_bytes": max(samples),
            "rss_samples": len(samples),
            "end_chunk_start": chunk["start_byte"],
            "end_chunk_bytes": len(expected),
            "end_chunk_ordinal": chunk["ordinal"],
            "warm_fetch_ms": round(fetch_ms, 3),
            "collection_bytes": int(match[1]),
            "collection_files": int(match[2]),
        }
        process.stdin.close()
        assert process.wait(timeout=30) == 0
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", default="target/release", type=Path)
    parser.add_argument("--hosted", action="store_true", help="read the object backend from SANCTUM_CONFIG_JSON")
    parser.add_argument("--mib", default=200, type=int)
    parser.add_argument("--timeout", default=600, type=int)
    args = parser.parse_args()
    if args.mib < 1 or args.timeout < 1:
        parser.error("size and timeout must be positive")
    with tempfile.TemporaryDirectory(prefix="sanctum-ingestion-benchmark-") as temp:
        root = Path(temp)
        path = root / "observations.txt"
        make_input(path, args.mib * MIB)
        if args.hosted:
            config = json.loads(os.environ["SANCTUM_CONFIG_JSON"])
            if not config["object_storage"].get("prefix"):
                parser.error("use a dedicated object storage prefix")
            config["standalone_object_worker"] = True
        else:
            config = {"version": 1, "storage_root": str(root / "storage")}
        config.update(grpc_listen="127.0.0.1:0", mcp_listen="127.0.0.1:0", embedding=None)
        print(json.dumps(measure(args.bin_dir.resolve(), config, path, args.timeout), indent=2))


if __name__ == "__main__":
    main()
