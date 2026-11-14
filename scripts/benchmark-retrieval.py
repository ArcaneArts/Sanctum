"""Measure BEIR ArguAna retrieval and replacement reuse through gRPC and Ollama."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
import csv
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
import runpy
import statistics
import subprocess
import tempfile
import threading
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

import grpc
from sanctum.v1 import sanctum_pb2 as pb, sanctum_pb2_grpc as rpc


MANAGED = runpy.run_path(str(Path(__file__).with_name("check-local-mcp.py")))["child"]
DOCUMENT = "title: none | text: "
QUERY = "task: search result | query: "
SCOPE = pb.Scope(tenant="default", collection="arguana")
REUSE_SCOPE = pb.Scope(tenant="default", collection="replacement")
MODES = {
    "lexical": pb.SEARCH_MODE_LEXICAL,
    "semantic": pb.SEARCH_MODE_SEMANTIC,
    "hybrid": pb.SEARCH_MODE_HYBRID,
}
K = 10
WINDOW = 4096
OVERLAP = 400


def read_jsonl(path):
    with path.open(encoding="utf-8") as stream:
        return {row["_id"]: row for row in map(json.loads, stream)}


def read_dataset(path):
    corpus = read_jsonl(path / "corpus.jsonl")
    queries = read_jsonl(path / "queries.jsonl")
    with (path / "qrels" / "test.tsv").open(encoding="utf-8", newline="") as stream:
        qrels = {}
        for qid, did, score in list(csv.reader(stream, delimiter="\t"))[1:]:
            if score != "1" or qid in qrels or qid not in queries or qid == did:
                raise ValueError("Expected one distinct, relevant ArguAna document per query")
            qrels[qid] = did
    if qrels.keys() != queries.keys():
        raise ValueError("Every ArguAna query must have a relevance judgment")
    return corpus, queries, qrels


@contextmanager
def count_provider(ollama, timeout):
    """Forward embedding requests to the real Ollama server and count inputs."""
    counts = Counter()
    lock = threading.Lock()

    def snapshot():
        with lock:
            return counts.copy()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            if self.path != "/api/embed":
                self.send_error(404)
                return
            body = self.rfile.read(int(self.headers["Content-Length"]))
            inputs = json.loads(body)["input"]
            with lock:
                counts.update("query" if text.startswith(QUERY) else "document" for text in inputs)
                counts["requests"] += 1
            request = Request(f"{ollama}/api/embed", data=body, headers={"Content-Type": "application/json"})
            try:
                with urlopen(request, timeout=timeout) as upstream:
                    status, data = upstream.status, upstream.read()
            except HTTPError as error:
                with error:
                    status, data = error.code, error.read()
            except OSError:
                self.send_error(502, "Ollama upstream could not be reached")
                return
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass  # Sanctum can cancel an outstanding request at shutdown.

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as proxy:
        worker = threading.Thread(target=proxy.serve_forever)
        worker.start()
        try:
            yield f"http://127.0.0.1:{proxy.server_port}", snapshot
        finally:
            proxy.shutdown()
            worker.join()


def upload(client, scope, doc_id, body, previous=None, *, record_busy=None):
    header = pb.UploadTextHeader(
        scope=scope, name=f"{doc_id}.txt",
        external=pb.ExternalSource(namespace="benchmark", id=doc_id), request_id=str(uuid4()),
    )
    if previous is not None:
        header.replacement.expected_revision_id = previous.revision_id
    frames = (
        pb.UploadTextFrame(header=header),
        pb.UploadTextFrame(data=body),
        pb.UploadTextFrame(end=pb.UploadTextEnd()),
    )
    deadline = time.monotonic() + 90
    job = retry_busy(lambda: client.SubmitText(iter(frames), timeout=30), deadline, record_busy)
    request = pb.IngestionJobRequest(scope=scope, operation_id=job.operation_id)
    while True:
        if job.state == pb.INGESTION_STATE_TEXT_READY:
            return job.text_revision
        if job.state in (pb.INGESTION_STATE_FAILED, pb.INGESTION_STATE_CANCELED):
            raise RuntimeError(f"{doc_id}: {job}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Waiting for {doc_id}: {job}")
        time.sleep(0.1)
        job = retry_busy(lambda: client.GetIngestionJob(request, timeout=15), deadline, record_busy)


def retry_busy(call, deadline, record_busy):
    while True:
        try:
            return call()
        except grpc.RpcError as error:
            if (error.code() != grpc.StatusCode.UNAVAILABLE
                    or error.details() != "database is busy" or time.monotonic() >= deadline):
                raise
            if record_busy is not None:
                record_busy()
            time.sleep(0.2)


def wait_for_embeddings(client, scope, chunks, timeout):
    deadline = time.monotonic() + timeout
    next_report = time.monotonic() + 15
    request = pb.EmbeddingStatusRequest(scope=scope)
    while True:
        status = client.GetEmbeddingStatus(request, timeout=15)
        coverage = status.coverage
        if coverage.failed_chunks > status.retrying_chunks:
            raise RuntimeError(f"Permanent embedding failure: {status}")
        if coverage.total_chunks == coverage.embedded_chunks == chunks:
            return
        now = time.monotonic()
        if now >= deadline:
            raise TimeoutError(f"Waiting for {chunks} current embeddings: {status}")
        if now >= next_report:
            print(f"{scope.collection}: {coverage.embedded_chunks}/{chunks} chunks embedded", flush=True)
            next_report = now + 15
        time.sleep(0.5)


def ingest(client, corpus, workers, timeout, embeddings):
    client.CreateCollection(pb.CreateCollectionRequest(scope=SCOPE), timeout=15)
    busy_retries = 0
    retry_lock = threading.Lock()

    def record_busy():
        nonlocal busy_retries
        with retry_lock:
            busy_retries += 1

    def document(item):
        doc_id, row = item
        text = (row["title"] + "\n" if row["title"] else "") + row["text"]
        return upload(client, SCOPE, doc_id, text.encode(), record_busy=record_busy).chunks

    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        chunks = 0
        for count, result in enumerate(pool.map(document, corpus.items()), start=1):
            chunks += result
            if count % 1000 == 0:
                print(f"Published {count}/{len(corpus)} documents", flush=True)
    lexical_ready = time.monotonic()
    if embeddings:
        wait_for_embeddings(client, SCOPE, chunks, timeout)
    return chunks, round(lexical_ready - started, 3), round(time.monotonic() - started, 3), busy_retries


def query_text(text):
    raw = text.encode()
    if len(raw) <= 4096:
        return text
    return raw[:4096].decode(errors="ignore").rsplit(maxsplit=1)[0]


def evaluate(client, queries, qrels, chunks, lexical_fallback):
    modes = {"hybrid_without_embeddings": pb.SEARCH_MODE_HYBRID} if lexical_fallback else MODES
    scores = {name: [] for name in modes}
    latency = {name: [] for name in modes}
    for number, (qid, row) in enumerate(queries.items(), start=1):
        query = query_text(row["text"])
        for name, mode in modes.items():
            started = time.monotonic()
            response = client.SearchText(pb.SearchTextRequest(
                scope=SCOPE, mode=mode, query=query, limit=30,
            ), timeout=45)
            latency[name].append((time.monotonic() - started) * 1000)
            expected_mode = pb.SEARCH_MODE_LEXICAL if lexical_fallback else mode
            reason = "embedding_disabled" if lexical_fallback else ""
            expected_embedded = 0 if lexical_fallback else chunks
            if (response.mode != expected_mode or response.degraded_reason != reason
                    or response.embedding_coverage.embedded_chunks != expected_embedded):
                raise RuntimeError(f"Incomplete {name} result for {qid}")
            ranked = list(dict.fromkeys(hit.external.id for hit in response.hits if hit.external.id != qid))
            top = ranked[:K]
            rank = top.index(qrels[qid]) + 1 if qrels[qid] in top else 0
            scores[name].append((rank, len(top)))
        if number % 100 == 0:
            print(f"Evaluated {number}/{len(queries)} queries", flush=True)

    return {
        name: {
            "recall_at_10": round(statistics.mean(rank > 0 for rank, _ in ranks), 5),
            "ndcg_at_10": round(statistics.mean(1 / math.log2(rank + 1) if rank else 0 for rank, _ in ranks), 5),
            "queries_with_results": sum(size > 0 for _, size in ranks),
            "queries_with_ten_results": sum(size == K for _, size in ranks),
            "grpc_p50_ms": round(statistics.median(latency[name]), 2),
            "grpc_p95_ms": round(sorted(latency[name])[math.ceil(len(ranks) * .95) - 1], 2),
        }
        for name, ranks in scores.items()
    }


def reuse(client, counts, timeout):
    client.CreateCollection(pb.CreateCollectionRequest(scope=REUSE_SCOPE), timeout=15)
    # No newlines or multibyte boundaries. Put the edit outside the overlap
    # between adjacent windows, and ensure each window has unique text.
    body = b"".join(f" Calibration entry {number:04d} for sensor drift.".encode() for number in range(1000))
    offset = (WINDOW - OVERLAP) * 4 + 1400
    modified = body[:offset] + b"reviewed" + body[offset + len(b"reviewed"):]
    before = counts()
    started = time.monotonic()
    original = upload(client, REUSE_SCOPE, "calibration", body)
    wait_for_embeddings(client, REUSE_SCOPE, original.chunks, timeout)
    first_ready = time.monotonic()
    first = counts() - before
    replacement = upload(client, REUSE_SCOPE, "calibration", modified, previous=original)
    wait_for_embeddings(client, REUSE_SCOPE, replacement.chunks, timeout)
    second_ready = time.monotonic()
    second = counts() - before - first
    if (original.source_id != replacement.source_id or original.revision_id == replacement.revision_id
            or original.chunks != replacement.chunks or first["document"] != original.chunks
            or second["document"] != 1 or second["query"]):
        raise RuntimeError(f"Expected one changed embedding input, got {first=} and {second=}")
    return {
        "source_bytes": len(body),
        "source_chunks": original.chunks,
        "initial_provider_inputs": first["document"],
        "replacement_provider_inputs": second["document"],
        "provider_inputs_avoided": original.chunks - second["document"],
        "initial_provider_requests": first["requests"],
        "replacement_provider_requests": second["requests"],
        "initial_to_fully_embedded_seconds": round(first_ready - started, 3),
        "replacement_to_fully_embedded_seconds": round(second_ready - first_ready, 3),
    }


def benchmark(args, root, corpus, queries, qrels, endpoint, counts):
    embeddings = not args.lexical_fallback
    config = {
        "version": 1,
        "storage_root": str(root),
        "grpc_listen": "127.0.0.1:0",
        "mcp_listen": None,
        "local_worker_concurrency": args.workers,
        "chunking": {"window_bytes": WINDOW, "overlap_bytes": OVERLAP},
        "embedding": {
            "provider": "ollama", "endpoint": endpoint,
            "profile": {
                "model": args.model, "revision": args.revision, "dimensions": args.dimensions,
                "document_prefix": DOCUMENT, "query_prefix": QUERY,
            },
        },
    }
    if args.lexical_fallback:
        del config["embedding"]
    env = os.environ | {
        "SANCTUM_CONFIG_JSON": json.dumps(config), "SANCTUM_MANAGED_STDIO": "1", "NO_COLOR": "1",
    }
    with MANAGED([args.bin_dir.resolve() / "sanctum-server"], env=env, stdin=subprocess.PIPE,
                 stdout=subprocess.PIPE, text=True) as server:
        addresses = json.loads(server.stdout.readline())
        with grpc.insecure_channel(addresses["grpc"]) as channel:
            client = rpc.SanctumStub(channel)
            chunks, lexical_seconds, embedding_seconds, busy_retries = ingest(
                client, corpus, args.workers, args.timeout, embeddings,
            )
            corpus_provider = counts()
            print(f"{chunks} current chunks in {embedding_seconds} s: {dict(corpus_provider)}", flush=True)
            reuse_result = reuse(client, counts, args.timeout) if embeddings else None
            if reuse_result:
                print(f"Replacement: {reuse_result}", flush=True)
            retrieval = evaluate(client, queries, qrels, chunks, args.lexical_fallback)
        server.stdin.close()
        if server.wait(timeout=30) != 0:
            raise RuntimeError("Sanctum did not shut down cleanly")
    database = root / SCOPE.tenant / SCOPE.collection / "meta.db"
    return {
        "model": args.model if embeddings else None,
        "revision": args.revision if embeddings else None,
        "dimensions": args.dimensions if embeddings else None,
        "documents": len(corpus), "queries": len(queries),
        "queries_missing_judged_document": sum(qrels[qid] not in corpus for qid in queries),
        "truncated_queries": sum(len(row["text"].encode()) > 4096 for row in queries.values()),
        "chunks": chunks, "meta_db_bytes_after_shutdown": database.stat().st_size,
        "ingestion_busy_retries": busy_retries,
        "lexical_index_seconds": lexical_seconds,
        "fully_embedded_seconds": embedding_seconds if embeddings else None,
        "corpus_provider_inputs": corpus_provider["document"],
        "corpus_provider_requests": corpus_provider["requests"],
        "retrieval": retrieval, "replacement": reuse_result,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path, help="directory containing extracted BEIR ArguAna files")
    parser.add_argument("--bin-dir", default=Path("target/release"), type=Path)
    parser.add_argument("--ollama", default="http://127.0.0.1:11434")
    parser.add_argument("--model", default="embeddinggemma-2:740m")
    parser.add_argument("--revision", help="installed Ollama model revision or digest")
    parser.add_argument("--lexical-fallback", action="store_true", help="measure hybrid with embeddings disabled")
    parser.add_argument("--dimensions", type=int, default=768)
    parser.add_argument("--workers", type=int, choices=range(1, 33), default=8, metavar="1-32")
    parser.add_argument("--queries", type=int, help="optional first N queries for a smoke run")
    parser.add_argument("--timeout", type=int, default=1800, help="per-collection embedding wait in seconds")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.queries is not None and args.queries < 1:
        parser.error("--queries must be positive")
    if not args.lexical_fallback and not args.revision:
        parser.error("--revision is required when embeddings are enabled")
    corpus, queries, qrels = read_dataset(args.dataset)
    if args.queries is not None:
        queries = {qid: queries[qid] for qid in list(queries)[:args.queries]}
    with tempfile.TemporaryDirectory(prefix="sanctum-retrieval-benchmark-") as tmp:
        proxy = (nullcontext((None, Counter)) if args.lexical_fallback
                 else count_provider(args.ollama.rstrip("/"), timeout=28))
        with proxy as (endpoint, counts):
            result = benchmark(args, Path(tmp), corpus, queries, qrels, endpoint, counts)
    result["query_provider_inputs"] = counts()["query"]
    result["query_ids_sha256"] = hashlib.sha256("\n".join(queries).encode()).hexdigest()
    report = json.dumps(result, indent=2)
    print(report)
    if args.output:
        args.output.write_text(report + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
