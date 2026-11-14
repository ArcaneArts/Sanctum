"""Measure concurrent object-backed semantic search with real Ollama vectors."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import runpy
import statistics
from threading import Barrier
import time

import grpc
from sanctum.v1 import sanctum_pb2 as pb
from sanctum.v1 import sanctum_pb2_grpc as rpc


SCRIPTS = Path(__file__).resolve().parent
MCP = runpy.run_path(str(SCRIPTS / "check-local-mcp.py"))
RETRIEVAL = runpy.run_path(str(SCRIPTS / "benchmark-retrieval.py"))
SCOPES = [
    pb.Scope(tenant="vector-benchmark", collection="argument-a"),
    pb.Scope(tenant="vector-benchmark", collection="argument-b"),
]


def read_cases(args):
    corpus, queries, qrels = RETRIEVAL["read_dataset"](args.dataset)
    judged = [(qid, qrels[qid]) for qid in queries if qrels[qid] in corpus]
    documents = list(corpus)
    cases = []
    for index, scope in enumerate(SCOPES):
        pairs = judged[index * args.queries:(index + 1) * args.queries]
        if len(pairs) != args.queries:
            raise ValueError("dataset has too few judged queries")
        selected = dict.fromkeys(did for _, did in pairs)
        start = index * args.documents
        for did in documents[start:start + args.documents]:
            if len(selected) >= args.documents:
                break
            selected[did] = None
        if len(selected) != args.documents:
            raise ValueError("dataset has too few documents")
        bodies = {
            did: ((corpus[did]["title"] + "\n" if corpus[did]["title"] else "")
                  + corpus[did]["text"]).encode()
            for did in selected
        }
        workload = [(qid, RETRIEVAL["query_text"](queries[qid]["text"]), did) for qid, did in pairs]
        cases.append((scope, bodies, workload))
    return cases


def ingest(client, cases, timeout):
    summary = []
    for scope, bodies, _ in cases:
        client.CreateCollection(pb.CreateCollectionRequest(scope=scope), timeout=15)
        started = time.monotonic()
        chunks = 0
        for number, (did, body) in enumerate(bodies.items(), start=1):
            revision = RETRIEVAL["upload"](client, scope, did, body)
            chunks += revision.chunks
            if number % 64 == 0:
                print(f"{scope.collection}: {number}/{len(bodies)} documents", flush=True)
        lexical = time.monotonic() - started
        RETRIEVAL["wait_for_embeddings"](client, scope, chunks, timeout)
        session = client.OpenDatabaseSession(pb.DatabaseTarget(
            tenant=scope.tenant, collection=scope.collection,
        ), timeout=15)
        client.CloseDatabaseSession(session.session, timeout=15)
        summary.append({
            "collection": scope.collection,
            "documents": len(bodies),
            "chunks": chunks,
            "source_bytes": sum(map(len, bodies.values())),
            "snapshot_bytes": session.bytes,
            "text_ready_seconds": round(lexical, 3),
            "fully_embedded_seconds": round(time.monotonic() - started, 3),
        })
    return summary


def search(client, scope, bodies, case, mode, chunks):
    qid, query, judged = case
    started = time.monotonic()
    result = client.SearchText(pb.SearchTextRequest(
        scope=scope, query=query, limit=20, mode=mode,
    ), timeout=45)
    duration = (time.monotonic() - started) * 1000
    if result.mode != mode or result.embedding_coverage.embedded_chunks != chunks:
        raise RuntimeError(f"incomplete {scope.collection} response for {qid}")
    ranked = list(dict.fromkeys(hit.external.id for hit in result.hits if hit.external.id != qid))[:10]
    for hit in result.hits:
        if hit.chunk.scope != scope or not hit.database_generation:
            raise RuntimeError("missing or crossed collection provenance")
        expected = bodies[hit.external.id][hit.start_byte:hit.end_byte]
        if hit.excerpt.encode() != expected:
            raise RuntimeError("incorrect UTF-8 excerpt")
    return duration, judged in ranked


def scenario(client, cases, summary, mode, concurrent):
    barrier = Barrier(len(cases)) if concurrent else None

    def run(case, info):
        scope, bodies, workload = case
        if barrier is not None:
            barrier.wait(timeout=10)
        return [search(client, scope, bodies, q, mode, info["chunks"]) for q in workload]

    if concurrent:
        with ThreadPoolExecutor(max_workers=len(cases)) as pool:
            jobs = [pool.submit(run, case, info) for case, info in zip(cases, summary)]
            results = [job.result() for job in jobs]
    else:
        results = [run(case, info) for case, info in zip(cases, summary)]
    latencies = sorted(ms for group in results for ms, _ in group)
    return {
        "queries": len(latencies),
        "first_per_collection_ms": [round(group[0][0], 2) for group in results],
        "p50_ms": round(statistics.median(latencies), 2),
        "p95_ms": round(latencies[(len(latencies) * 95 + 99) // 100 - 1], 2),
        "recall_at_10": round(statistics.mean(found for group in results for _, found in group), 5),
    }


def run(args, config, cases, endpoint):
    config = config | {
        "grpc_listen": "127.0.0.1:0",
        "standalone_object_worker": True,
        "hosted_snapshot_page_deltas": True,
        "chunking": {"window_bytes": RETRIEVAL["WINDOW"], "overlap_bytes": RETRIEVAL["OVERLAP"]},
        "embedding": {
            "provider": "ollama", "endpoint": endpoint,
            "profile": {
                "model": args.model, "revision": args.revision, "dimensions": args.dimensions,
                "document_prefix": RETRIEVAL["DOCUMENT"], "query_prefix": RETRIEVAL["QUERY"],
            },
        },
    }
    server = args.bin_dir.resolve() / "sanctum-server"
    process = MCP["hosted_process"]
    with process(server, config) as api, grpc.insecure_channel(api["grpc"]) as channel:
        summary = ingest(rpc.SanctumStub(channel), cases, args.timeout)
    config["standalone_object_worker"] = False
    with process(server, config) as api, grpc.insecure_channel(api["grpc"]) as channel:
        client = rpc.SanctumStub(channel)
        cold = scenario(client, cases, summary, pb.SEARCH_MODE_SEMANTIC, True)
        semantic_serial = scenario(client, cases, summary, pb.SEARCH_MODE_SEMANTIC, False)
        semantic_parallel = scenario(client, cases, summary, pb.SEARCH_MODE_SEMANTIC, True)
        hybrid_parallel = scenario(client, cases, summary, pb.SEARCH_MODE_HYBRID, True)
    return {
        "collections": summary,
        "semantic_after_restart": cold,
        "warm_semantic_serial": semantic_serial,
        "warm_semantic_parallel": semantic_parallel,
        "warm_hybrid_parallel": hybrid_parallel,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path, help="extracted BEIR ArguAna directory")
    parser.add_argument("--bin-dir", type=Path, default=Path("target/release"))
    parser.add_argument("--ollama", default="http://127.0.0.1:11434")
    parser.add_argument("--model", default="embeddinggemma-2:740m")
    parser.add_argument("--revision", required=True)
    parser.add_argument("--dimensions", type=int, default=768)
    parser.add_argument("--documents", type=int, default=128, help="documents per collection")
    parser.add_argument("--queries", type=int, default=24, help="queries per collection")
    parser.add_argument("--timeout", type=int, default=180, help="embedding wait in seconds")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 1 <= args.queries <= args.documents or min(args.dimensions, args.timeout) < 1:
        parser.error("use positive limits and at least as many documents as queries")
    config = json.loads(os.environ["SANCTUM_CONFIG_JSON"])
    if not config["object_storage"].get("prefix"):
        parser.error("use a dedicated object storage prefix")
    cases = read_cases(args)
    with RETRIEVAL["count_provider"](args.ollama.rstrip("/"), timeout=28) as (endpoint, counts):
        result = run(args, config, cases, endpoint)
        result["provider"] = dict(counts())
    result["model"] = args.model
    result["revision"] = args.revision
    result["dimensions"] = args.dimensions
    ids = [qid for _, _, queries in cases for qid, _, _ in queries]
    result["query_ids_sha256"] = hashlib.sha256("\n".join(ids).encode()).hexdigest()
    report = json.dumps(result, indent=2)
    print(report)
    if args.output:
        args.output.write_text(report + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
