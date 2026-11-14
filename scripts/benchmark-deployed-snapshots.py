"""Seed hosted collections and measure concurrent searches against published snapshots."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import runpy
import sys
import tempfile
from threading import Barrier
import time
from urllib.parse import urlsplit
from uuid import UUID, uuid5

import grpc
from sanctum.v1 import sanctum_pb2 as pb


SCRIPTS = Path(__file__).parent
DEPLOYMENT = runpy.run_path(str(SCRIPTS / "check-hosted-deployment.py"))
INGESTION = runpy.run_path(str(SCRIPTS / "benchmark-ingestion.py"))
MIB = INGESTION["MIB"]
FRAME = 64 * 1024
TAIL_BYTES = 16 * 1024 + 4  # Largest canonical chunk plus UTF-8 alignment.
RPC_TIMEOUT = 120


def upload_frames(scope, operation, path):
    yield pb.UploadTextFrame(header=pb.UploadTextHeader(
        scope=scope, name=path.name, request_id=str(operation),
        external=pb.ExternalSource(namespace="capacity", id=str(operation)),
    ))
    with path.open("rb") as source:
        while data := source.read(FRAME):
            yield pb.UploadTextFrame(data=data)
    yield pb.UploadTextFrame(end=pb.UploadTextEnd())


def snapshot(client, scope):
    opened = client.OpenDatabaseSession(pb.DatabaseTarget(
        tenant=scope.tenant, collection=scope.collection,
    ), timeout=RPC_TIMEOUT)
    client.CloseDatabaseSession(opened.session, timeout=RPC_TIMEOUT)
    if not opened.snapshot_generation:
        raise RuntimeError("the benchmark needs an object-backed database")
    return opened.bytes, opened.snapshot_generation


def ingestion(client, scope, operation):
    try:
        return client.GetIngestionJob(pb.IngestionJobRequest(
            scope=scope, operation_id=str(operation),
        ), timeout=RPC_TIMEOUT)
    except grpc.RpcError as error:
        if error.code() != grpc.StatusCode.NOT_FOUND:
            raise
        return None


def source_operation(args, scope, number):
    return uuid5(args.run_id, f"{scope.collection}-{number}-{args.source_mib}")


def seed_collection(client, args, scope, scratch):
    scratch.mkdir()
    client.CreateCollection(pb.CreateCollectionRequest(
        scope=scope, request_id=str(uuid5(args.run_id, scope.collection)),
    ), timeout=RPC_TIMEOUT)
    source_bytes = args.source_mib * MIB
    # Resume from the last accepted source. Earlier jobs share the latest
    # snapshot size, so checking it for each would misidentify the final source.
    resumed = None
    first = 1
    for number in range(1, args.max_sources + 1):
        job = ingestion(client, scope, source_operation(args, scope, number))
        if job is None:
            break
        first, resumed = number, job
    for number in range(first, args.max_sources + 1):
        operation = source_operation(args, scope, number)
        marker = f"sanctumcapacity{operation.hex}"
        tail = f"\n{marker} café 🦀. End of the capacity fixture.\n".encode()
        path = scratch / f"observations-{number:03d}.txt"
        accepted = resumed
        resumed = None
        if accepted is None:
            INGESTION["make_input"](path, source_bytes, seed=operation.int, tail=tail)
            accepted = client.SubmitText(upload_frames(scope, operation, path), timeout=600)
        if accepted.operation_id != str(operation) or accepted.total_bytes != source_bytes:
            raise RuntimeError("the deployment acknowledged a different fixture")
        job = accepted
        if job.state != pb.INGESTION_STATE_TEXT_READY:
            job, _ = DEPLOYMENT["wait_for_text"](client, scope, operation, args.wait_seconds)
        database_bytes, generation = snapshot(client, scope)
        print(f"{scope.collection}: {number} sources, {database_bytes:,} database bytes",
              file=sys.stderr, flush=True)
        if database_bytes >= args.target_mib * MIB:
            if not path.exists():
                INGESTION["make_input"](path, source_bytes, seed=operation.int, tail=tail)
            with path.open("rb") as reader:
                tail_start = max(0, source_bytes - TAIL_BYTES)
                reader.seek(tail_start)
                # Align the independently generated range to a UTF-8 boundary.
                expected_tail = reader.read().lstrip(bytes(range(128, 192)))
            return {
                "tenant": scope.tenant, "collection": scope.collection, "query": marker,
                "source_id": job.source_id, "revision_id": job.revision_id,
                "operation": str(operation), "source_bytes": source_bytes,
                "source_count": number,
                "tail": expected_tail.decode(), "tail_start": source_bytes - len(expected_tail),
                "snapshot_bytes": database_bytes, "snapshot_generation": generation,
            }
        path.unlink(missing_ok=True)
    raise RuntimeError(f"{scope.collection} did not reach the target size with the requested sources")


def seed(client, args):
    tenant = f"capacity-{args.run_id.hex[:20]}"
    fixtures = []
    with tempfile.TemporaryDirectory(prefix="sanctum-capacity-") as directory:
        for number in range(1, args.collections + 1):
            collection = f"collection-{number}"
            scope = pb.Scope(tenant=tenant, collection=collection)
            fixtures.append(seed_collection(client, args, scope, Path(directory) / collection))
    return {"run_id": str(args.run_id), "targets": fixtures}


def verify_hit(hit, fixture, scope):
    reference = hit.chunk
    if (reference.scope != scope or reference.source_id != fixture["source_id"]
            or reference.revision_id != fixture["revision_id"]
            or hit.external.namespace != "capacity"
            or hit.external.id != fixture["operation"]):
        raise RuntimeError(f"search returned different source provenance in {scope.collection}")
    if hit.database_generation != fixture["snapshot_generation"]:
        raise RuntimeError(f"snapshot generation changed during the run in {scope.collection}")
    if not fixture["tail_start"] <= hit.start_byte < hit.end_byte <= fixture["source_bytes"]:
        raise RuntimeError(f"invalid canonical range in {scope.collection}")
    start = hit.start_byte - fixture["tail_start"]
    end = hit.end_byte - fixture["tail_start"]
    if hit.excerpt.encode() != fixture["tail"].encode()[start:end]:
        raise RuntimeError(f"incorrect source bytes in {scope.collection}")
    if fixture["query"] not in hit.excerpt:
        raise RuntimeError(f"search excerpt omitted the fixture marker in {scope.collection}")


def search(client, fixture, barrier):
    scope = pb.Scope(tenant=fixture["tenant"], collection=fixture["collection"])
    request = pb.SearchTextRequest(
        scope=scope, mode=pb.SEARCH_MODE_LEXICAL, query=fixture["query"], limit=1,
    )
    barrier.wait(timeout=RPC_TIMEOUT)
    started = DEPLOYMENT["utc_now"]()
    reply, duration = DEPLOYMENT["time_call"](lambda: client.SearchText(request, timeout=RPC_TIMEOUT))
    finished = DEPLOYMENT["utc_now"]()
    if len(reply.hits) != 1 or reply.mode != pb.SEARCH_MODE_LEXICAL:
        raise RuntimeError(f"expected one lexical result in {scope.collection}")
    hit = reply.hits[0]
    verify_hit(hit, fixture, scope)
    return hit.chunk, {
        "tenant": scope.tenant, "collection": scope.collection,
        "snapshot_bytes": fixture["snapshot_bytes"], "snapshot_generation": hit.database_generation,
        "start_byte": hit.start_byte, "end_byte": hit.end_byte,
        "started_at": started, "finished_at": finished, "elapsed_ms": duration,
    }


def measure(client, args):
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    fixtures = manifest["targets"]
    if args.collection:
        fixtures = [fixture for fixture in fixtures if fixture["collection"] in args.collection]
        if {fixture["collection"] for fixture in fixtures} != set(args.collection):
            raise ValueError("one or more selected collections are missing from the manifest")
    if not fixtures:
        raise ValueError("the seed manifest contains no collections")
    passes = []
    with ThreadPoolExecutor(max_workers=len(fixtures)) as pool:
        for number in range(1, args.samples + 1):
            barrier = Barrier(len(fixtures))
            pending = [pool.submit(search, client, fixture, barrier) for fixture in fixtures]
            results = [future.result() for future in pending]
            passes.append([sample for _, sample in results])
            print(f"Completed search pass {number}/{args.samples}", file=sys.stderr, flush=True)
            if number < args.samples and args.interval_seconds:
                time.sleep(args.interval_seconds)
    # Keep additional requests outside the timing passes.
    for fixture, (chunk, sample) in zip(fixtures, results, strict=True):
        fetched = client.FetchChunk(pb.FetchChunkRequest(chunk=chunk), timeout=RPC_TIMEOUT)
        verify_hit(fetched, fixture, chunk.scope)
        if (fetched.chunk, fetched.start_byte, fetched.end_byte) != (chunk, sample["start_byte"], sample["end_byte"]):
            raise RuntimeError(f"fetch returned different provenance in {chunk.scope.collection}")
    return {"run_id": manifest["run_id"], "searches": passes}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", required=True)
    commands = parser.add_subparsers(dest="phase", required=True)
    prepare = commands.add_parser("seed", help="publish fixtures through the existing gRPC API")
    prepare.add_argument("--run-id", required=True, type=UUID)
    prepare.add_argument("--collections", type=int, default=1)
    prepare.add_argument("--target-mib", type=int, default=1024)
    prepare.add_argument("--source-mib", type=int, default=200)
    prepare.add_argument("--max-sources", type=int, default=40, help="maximum uploads per collection")
    prepare.add_argument("--wait-seconds", type=int, default=600)
    timing = commands.add_parser("measure", help="search the seed manifest over gRPC")
    timing.add_argument("manifest", type=Path)
    timing.add_argument("--collection", action="append", help="measure only the named collection")
    timing.add_argument("--samples", type=int, default=4)
    timing.add_argument("--interval-seconds", type=int, default=0, help="pause between search passes")
    args = parser.parse_args()
    url = args.api_url.rstrip("/")
    endpoint = urlsplit(url)
    if endpoint.path or endpoint.query or endpoint.fragment or endpoint.username:
        parser.error("--api-url must be a service origin")
    if args.phase == "seed" and min(args.collections, args.target_mib, args.source_mib,
                                    args.max_sources, args.wait_seconds) < 1:
        parser.error("seed sizes, counts, and wait duration must be positive")
    if args.phase == "measure" and (args.samples < 1 or args.interval_seconds < 0):
        parser.error("use a positive sample count and a nonnegative interval")
    with DEPLOYMENT["connect"](url, os.environ.get(DEPLOYMENT["ENDPOINT_TOKEN"])) as client:
        report = seed(client, args) if args.phase == "seed" else measure(client, args)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
