"""Verify upload and retrieval through an existing Sanctum deployment."""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import runpy
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import grpc
from sanctum.v1 import sanctum_pb2 as pb
from sanctum.v1 import sanctum_pb2_grpc as rpc


MCP = runpy.run_path(str(Path(__file__).with_name("check-local-mcp.py")))
ENDPOINT_TOKEN = "SANCTUM_GRPC_ID_TOKEN"
COLLECTION = "deployment-check"
RPC_TIMEOUT = 20


def fixture(operation, mib):
    """Generate an identical byte stream on each replay and cap every frame."""
    marker = f"sanctumcheck{operation.hex}"
    tail = f"\n{marker} café ❄️\n".encode()
    padding = mib * 1024 * 1024 - len(tail) if mib else 0
    if padding < 0:
        raise ValueError("fixture is smaller than its final line")
    line = b"Hosted Sanctum deployment validation fixture.\n"
    block = (line * (65536 // len(line) + 1))[:65536]
    return marker, tail, padding, block


def frames(scope, operation, data):
    _, tail, padding, block = data
    yield pb.UploadTextFrame(header=pb.UploadTextHeader(
        scope=scope, name="deployment-check.txt", request_id=str(operation),
        external=pb.ExternalSource(namespace="deployment-check", id=str(operation)),
    ))
    while padding:
        part = block[:min(padding, len(block))]
        yield pb.UploadTextFrame(data=part)
        padding -= len(part)
    yield pb.UploadTextFrame(data=tail)
    yield pb.UploadTextFrame(end=pb.UploadTextEnd())


@contextmanager
def connect(api_url, token):
    endpoint = urlsplit(api_url)
    if endpoint.scheme == "https" and token:
        credentials = grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(), grpc.access_token_call_credentials(token),
        )
        with grpc.secure_channel(endpoint.netloc, credentials) as channel:
            yield rpc.SanctumStub(channel)
    elif endpoint.scheme == "http" and not token and endpoint.hostname in ("localhost", "127.0.0.1", "::1"):
        with grpc.insecure_channel(endpoint.netloc) as channel:
            yield rpc.SanctumStub(channel)
    else:
        raise ValueError(f"use an HTTPS origin with {ENDPOINT_TOKEN}, or unauthenticated loopback HTTP")


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def time_call(call):
    started = time.monotonic()
    result = call()
    return result, round((time.monotonic() - started) * 1000, 2)


def check_iam(api_url):
    # Exercise the actual service boundary using the same route as the authorized check.
    transport = MCP["Http"](api_url + "/mcp")
    request = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
    try:
        with transport.post(request) as reply:
            raise RuntimeError(f"unauthenticated MCP request returned HTTP {reply.status}")
    except HTTPError as error:
        with error:
            if error.code not in (401, 403):
                raise
            unauthenticated = error.code
    with grpc.secure_channel(urlsplit(api_url).netloc, grpc.ssl_channel_credentials()) as channel:
        try:
            rpc.SanctumStub(channel).GetRuntimeInfo(pb.Empty(), timeout=RPC_TIMEOUT)
        except grpc.RpcError as error:
            if error.code() not in (grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.PERMISSION_DENIED):
                raise
            grpc_denied = error.code().name
        else:
            raise RuntimeError("unauthenticated gRPC request was accepted")
    return {"mcp_unauthenticated_status": unauthenticated, "grpc_unauthenticated_status": grpc_denied}


def submit(client, scope, operation, data):
    try:
        client.CreateCollection(pb.CreateCollectionRequest(scope=scope), timeout=RPC_TIMEOUT)
    except grpc.RpcError as error:
        if error.code() != grpc.StatusCode.ALREADY_EXISTS:
            raise
    accepted, elapsed = time_call(lambda: client.SubmitText(
        frames(scope, operation, data), timeout=600,
    ))
    expected = len(data[1]) + data[2]
    if accepted.operation_id != str(operation) or accepted.total_bytes != expected:
        raise RuntimeError("accepted upload does not match the fixture")
    return accepted, elapsed


def wait_for_text(client, scope, operation, timeout):
    request = pb.IngestionJobRequest(scope=scope, operation_id=str(operation))
    deadline = time.monotonic() + timeout
    states = []
    while True:
        job = client.GetIngestionJob(request, timeout=RPC_TIMEOUT)
        state = job.state
        if not states or states[-1] != state:
            states.append(state)
        if state == pb.INGESTION_STATE_TEXT_READY:
            return job, [pb.IngestionState.Name(value) for value in states]
        if state in (pb.INGESTION_STATE_FAILED, pb.INGESTION_STATE_CANCELED):
            raise RuntimeError(f"operation {operation}: {job.error_code} {job.error_message}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"operation {operation} is still {pb.IngestionState.Name(state)}")
        time.sleep(1)


def range_bytes(start, end, data):
    _, tail, padding, block = data
    expected = bytearray()
    while start < min(end, padding):
        offset = start % len(block)
        size = min(padding - start, end - start, len(block) - offset)
        expected.extend(block[offset:offset + size])
        start += size
    if start < end:
        expected.extend(tail[start - padding:end - padding])
    return bytes(expected)


def expected_excerpt(reference, start, end, excerpt, scope, job, data):
    if (reference.scope, reference.source_id, reference.revision_id) != (scope, job.source_id, job.revision_id):
        raise RuntimeError("search returned a different source revision")
    _, tail, padding, _ = data
    if not (0 <= start < end <= padding + len(tail)):
        raise RuntimeError("search returned a range outside the fixture")
    expected = range_bytes(start, end, data)
    if excerpt.encode() != expected:
        raise RuntimeError("search returned incorrect source bytes")
    return expected


def mcp_client(api_url, token):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    client = MCP["Rpc"](MCP["Http"](api_url + "/mcp", headers))
    client.initialize()
    return client


def verify_retrieval(client, api_url, token, scope, job, data):
    marker = data[0]
    request = pb.SearchTextRequest(scope=scope, mode=pb.SEARCH_MODE_LEXICAL, query=marker, limit=1)
    search, first_ms = time_call(lambda: client.SearchText(request, timeout=RPC_TIMEOUT))
    if len(search.hits) != 1:
        raise RuntimeError("gRPC search did not find the fixture")
    hit = search.hits[0]
    reference = hit.chunk
    expected = expected_excerpt(reference, hit.start_byte, hit.end_byte, hit.excerpt, scope, job, data)
    if hit.external.id != job.operation_id or not hit.database_generation:
        raise RuntimeError("gRPC search omitted fixture provenance")
    repeat, repeat_ms = time_call(lambda: client.SearchText(request, timeout=RPC_TIMEOUT))
    if len(repeat.hits) != 1 or repeat.hits[0].chunk != reference:
        raise RuntimeError("repeated search returned a different reference")
    fetched = client.FetchChunk(pb.FetchChunkRequest(chunk=reference), timeout=RPC_TIMEOUT)
    start, end = hit.start_byte, hit.end_byte
    if (fetched.start_byte, fetched.end_byte, fetched.excerpt.encode()) != (start, end, expected):
        raise RuntimeError("gRPC returned incorrect source bytes")

    mcp = mcp_client(api_url, token)
    target = {"tenant": scope.tenant, "collection": scope.collection}
    result = mcp.tool("search", **target, query=marker, mode="lexical", limit=1)
    matches = result["hits"]
    if len(matches) != 1 or matches[0]["source_id"] != reference.source_id:
        raise RuntimeError("MCP search did not find the gRPC source")
    exact = mcp.tool(
        "fetch_chunk", **target, source_id=reference.source_id,
        revision_id=reference.revision_id, ordinal=reference.ordinal,
    )
    if (exact["start_byte"], exact["end_byte"], exact["excerpt"].encode()) != (start, end, expected):
        raise RuntimeError("MCP returned different range bytes")
    return {
        "database_generation": hit.database_generation,
        "source_id": reference.source_id,
        "revision_id": reference.revision_id,
        "first_search_ms": first_ms,
        "repeat_search_ms": repeat_ms,
        "verified_range_bytes": len(expected),
    }


def wait_for_vectors(client, scope, job, timeout):
    request = pb.EmbeddingStatusRequest(scope=scope, source_id=job.source_id, revision_id=job.revision_id)
    deadline = time.monotonic() + timeout
    while True:
        status = client.GetEmbeddingStatus(request, timeout=RPC_TIMEOUT)
        coverage = status.coverage
        if not coverage.profile_id:
            raise RuntimeError("embedding is disabled for this API")
        if 0 < coverage.total_chunks == coverage.embedded_chunks and coverage.failed_chunks == 0:
            return coverage
        if coverage.failed_chunks > status.retrying_chunks:
            raise RuntimeError(f"document embedding failed: {status.sample_error}")
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"document embeddings reached {coverage.embedded_chunks}/{coverage.total_chunks}; "
                f"retrying {status.retrying_chunks}; last provider error: {status.sample_error}"
            )
        time.sleep(1)


def verify_semantic(client, api_url, token, scope, job, data, dimensions, timeout):
    coverage, wait_ms = time_call(lambda: wait_for_vectors(client, scope, job, timeout))
    runtime = client.GetRuntimeInfo(pb.Empty(), timeout=RPC_TIMEOUT)
    if runtime.embedding_profile_id != coverage.profile_id or runtime.embedding_dimensions != dimensions:
        raise RuntimeError(
            f"expected the stored profile with {dimensions} dimensions; "
            f"API reports {runtime.embedding_dimensions} dimensions, profile {runtime.embedding_profile_id}"
        )

    # This query does not use the fixture's unique lexical marker.
    query = "A validation document uploaded to a hosted knowledge service"
    request = pb.SearchTextRequest(scope=scope, mode=pb.SEARCH_MODE_SEMANTIC, query=query, limit=1)
    search, grpc_ms = time_call(lambda: client.SearchText(request, timeout=RPC_TIMEOUT))
    if (search.mode != pb.SEARCH_MODE_SEMANTIC or search.degraded_reason
            or search.embedding_coverage != coverage or len(search.hits) != 1):
        raise RuntimeError("gRPC did not return a complete semantic result")
    hit = search.hits[0]
    if (not hit.HasField("score") or not math.isfinite(hit.score)
            or hit.embedding_state != pb.CHUNK_EMBEDDING_STATE_READY or not hit.database_generation):
        raise RuntimeError("gRPC semantic hit omitted vector or snapshot information")
    expected = expected_excerpt(hit.chunk, hit.start_byte, hit.end_byte, hit.excerpt, scope, job, data)

    mcp = mcp_client(api_url, token)
    result, mcp_ms = time_call(lambda: mcp.tool(
        "search", tenant=scope.tenant, collection=scope.collection, query=query, mode="semantic", limit=1,
    ))
    mcp_coverage = result["embedding_coverage"]
    if (result["mode"] != "semantic" or result["degraded_reason"] or len(result["hits"]) != 1
            or mcp_coverage != {
                "profile_id": coverage.profile_id, "total_chunks": coverage.total_chunks,
                "embedded_chunks": coverage.embedded_chunks, "failed_chunks": 0,
            }):
        raise RuntimeError("MCP did not return a complete semantic result")
    match = result["hits"][0]
    if (match["embedding_state"] != "ready" or not match.get("database_generation")
            or match.get("score") is None or not math.isfinite(match["score"])):
        raise RuntimeError("MCP semantic hit omitted vector or snapshot information")
    reference = pb.ChunkRef(
        scope=pb.Scope(tenant=match["tenant"], collection=match["collection"]),
        source_id=match["source_id"], revision_id=match["revision_id"], ordinal=match["ordinal"],
    )
    mcp_excerpt = expected_excerpt(reference, match["start_byte"], match["end_byte"], match["excerpt"], scope, job, data)
    return {
        "profile_id": coverage.profile_id, "dimensions": dimensions,
        "embedded_chunks": coverage.embedded_chunks, "wait_for_vectors_ms": wait_ms,
        "grpc_search_ms": grpc_ms, "mcp_search_ms": mcp_ms,
        "grpc_verified_range_bytes": len(expected), "mcp_verified_range_bytes": len(mcp_excerpt),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--operation", type=UUID, default=None)
    parser.add_argument("--mib", type=int, default=0, help="generate an exact-size UTF-8 upload")
    parser.add_argument("--phase", choices=["all", "submit", "verify"], default="all")
    parser.add_argument("--wait-seconds", type=int, default=180)
    parser.add_argument("--embedding-dimensions", type=int, help="also verify embeddings and strict semantic retrieval")
    parser.add_argument("--expect-private", action="store_true", help="verify unauthenticated IAM denial over HTTP and gRPC")
    args = parser.parse_args()
    if args.phase != "all" and args.operation is None:
        parser.error("--operation is required when splitting submit and verify")
    if args.mib < 0 or args.wait_seconds < 1:
        parser.error("size and wait duration must be valid")
    if args.embedding_dimensions is not None and args.embedding_dimensions < 1:
        parser.error("--embedding-dimensions must be positive")
    operation = args.operation or uuid4()
    endpoint = urlsplit(args.api_url)
    if endpoint.path not in ("", "/") or endpoint.query or endpoint.fragment or endpoint.username:
        parser.error("--api-url must be a service origin")
    if args.expect_private and endpoint.scheme != "https":
        parser.error("--expect-private requires HTTPS")
    api_url = args.api_url.rstrip("/")
    scope = pb.Scope(tenant=f"verify-{operation.hex[:20]}", collection=COLLECTION)
    data = fixture(operation, args.mib)
    token = os.environ.get(ENDPOINT_TOKEN)
    report = {
        "started_at": utc_now(), "operation": str(operation), "tenant": scope.tenant,
        "collection": scope.collection, "bytes": len(data[1]) + data[2], "phase": args.phase,
        "dispatch_shard": operation.bytes[0] % 16,
    }
    if args.expect_private and not token:
        parser.error(f"--expect-private requires {ENDPOINT_TOKEN}")
    if args.expect_private:
        report.update(check_iam(api_url))
    with connect(api_url, token) as client:
        if args.phase != "verify":
            accepted, elapsed = submit(client, scope, operation, data)
            report.update(acceptance_ms=elapsed, submission_state=pb.IngestionState.Name(accepted.state))
        if args.phase != "submit":
            result, elapsed = time_call(lambda: wait_for_text(client, scope, operation, args.wait_seconds))
            job, states = result
            report.update(wait_to_text_ready_ms=elapsed, observed_states=states)
            report.update(verify_retrieval(client, api_url, token, scope, job, data))
            if args.embedding_dimensions is not None:
                report["semantic"] = verify_semantic(
                    client, api_url, token, scope, job, data, args.embedding_dimensions, args.wait_seconds,
                )
    report["finished_at"] = utc_now()
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
