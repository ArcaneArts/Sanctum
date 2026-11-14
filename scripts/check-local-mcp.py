"""Exercise the built Sanctum binaries across MCP HTTP, MCP stdio and gRPC."""

import contextlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
import tempfile
import time
import urllib.error
import urllib.request
import uuid


class Rpc:
    def __init__(self, exchange):
        self.exchange = exchange
        self.counter = 0

    def request(self, method, params=None):
        self.counter += 1
        request = {"jsonrpc": "2.0", "id": self.counter, "method": method}
        if params is not None:
            request["params"] = params
        response = self.exchange(request)
        assert response["id"] == self.counter, response
        assert "error" not in response, response
        return response["result"]

    def initialize(self):
        result = self.request("initialize", {
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "clientInfo": {"name": "sanctum-mcp-check", "version": "1"},
        })
        assert result["protocolVersion"] == "2025-11-25", result
        self.exchange({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def tool(self, tool_name, **arguments):
        result = self.request("tools/call", {"name": tool_name, "arguments": arguments})
        assert not result.get("isError"), result
        return result["structuredContent"]


class Http:
    def __init__(self, endpoint, headers=None):
        self.endpoint = endpoint
        self.headers = headers or {}
        self.session = None

    def post(self, message, **extra_headers):
        headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
        if message["method"] != "initialize":
            headers["MCP-Protocol-Version"] = "2025-11-25"
        if self.session:
            headers["Mcp-Session-Id"] = self.session
        request = urllib.request.Request(self.endpoint, json.dumps(message).encode(),
                                         headers=headers | self.headers | extra_headers)
        return urllib.request.urlopen(request, timeout=10)

    def __call__(self, message):
        with self.post(message) as response:
            self.session = response.headers.get("Mcp-Session-Id") or self.session
            if response.status == 202:
                return None
            if response.headers.get_content_type() == "application/json":
                return json.load(response)
            for line in response:
                if line.startswith(b"data: ") and line[6:].strip():
                    return json.loads(line[6:])
        raise AssertionError("MCP HTTP response ended without a JSON-RPC message")


@contextlib.contextmanager
def child(command, **kwargs):
    process = subprocess.Popen(command, **kwargs)
    try:
        yield process
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise


READ_ONLY_TOOLS = {
    "list_collections", "ingestion_status", "search", "search_collections",
    "scan_pending_text", "fetch_chunk", "fetch_context", "get_metadata",
}
WRITE_TOOLS = {"create_collection", "ingest_text", "mutate_metadata"}


def check_tool_annotations(tools):
    hints = {tool["name"]: tool.get("annotations", {}).get("readOnlyHint") for tool in tools}
    assert hints.keys() == READ_ONLY_TOOLS | WRITE_TOOLS, hints
    assert {name for name, hint in hints.items() if hint is True} == READ_ONLY_TOOLS, hints


def check(bin_dir):
    suffix = ".exe" if os.name == "nt" else ""
    server_bin = bin_dir / f"sanctum-server{suffix}"
    cli_bin = bin_dir / f"sanctum-cli{suffix}"
    with tempfile.TemporaryDirectory(prefix="sanctum-mcp-") as directory:
        path = Path(directory)
        config = json.dumps({
            "version": 1,
            "storage_root": str(path / "data"),
            "grpc_listen": "127.0.0.1:0",
            "mcp_listen": "127.0.0.1:0",
        })
        log_path = path / "server.log"
        env = os.environ | {
            "SANCTUM_CONFIG_JSON": config, "SANCTUM_MANAGED_STDIO": "1", "NO_COLOR": "1",
        }
        with log_path.open("w") as log, child(
            [server_bin], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log,
            text=True, env=env,
        ) as server:
            timer = threading.Timer(10, server.kill)
            timer.start()
            try:
                addresses = json.loads(server.stdout.readline())
            except json.JSONDecodeError as error:
                raise RuntimeError(log_path.read_text()) from error
            finally:
                timer.cancel()
            endpoint = f"http://{addresses['mcp']}/mcp"

            transport = Http(endpoint)
            initial = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
            for header, value in [("Origin", "https://untrusted.example"), ("Host", "untrusted.example")]:
                try:
                    transport.post(initial, **{header: value})
                except urllib.error.HTTPError as error:
                    with error:
                        assert error.code == 403, (header, error.code, error.read())
                else:
                    raise AssertionError(f"accepted unexpected {header}")
            http = Rpc(transport)
            http.initialize()
            tools = http.request("tools/list")["tools"]
            check_tool_annotations(tools)
            http.tool("create_collection", collection="notes")
            http.tool("create_collection", collection="reference")
            first = http.tool("list_collections", limit=1)
            assert first["collections"] == ["notes"], first
            assert http.tool("list_collections", after=first["next_after"])["collections"] == ["reference"]

            content = "Snow crystals near the ridge ❄️ and a quartz sample."
            upload = {"collection": "notes", "name": "field.md", "text": content,
                      "external": {"namespace": "field", "id": "note-1"}, "request_id": str(uuid.uuid4())}
            job = http.tool("ingest_text", **upload)
            replay = http.tool("ingest_text", **upload)
            assert (replay["operation_id"], replay["revision_id"]) == (job["operation_id"], job["revision_id"])
            for _ in range(100):
                status = http.tool("ingestion_status", collection="notes", operation_id=job["operation_id"])
                if status["state"] == "text_ready":
                    break
                assert status["state"] != "failed", status
                time.sleep(.05)
            assert status["state"] == "text_ready", status

            with child(
                [cli_bin, "mcp-stdio", "--mcp-url", endpoint],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
            ) as bridge:
                timer = threading.Timer(30, bridge.kill)
                timer.daemon = True
                timer.start()

                def exchange(message):
                    bridge.stdin.write(json.dumps(message) + "\n")
                    bridge.stdin.flush()
                    if "id" in message:
                        return json.loads(bridge.stdout.readline())

                stdio = Rpc(exchange)
                stdio.initialize()
                assert stdio.request("tools/list")["tools"] == tools
                edits = [{"namespace": "field", "key": "classification", "operation": "set",
                          "value": {"tags": ["sample", None]}}]
                stdio.tool("mutate_metadata", collection="notes", edits=edits)
                result = http.tool("get_metadata", collection="notes")
                assert result["entries"][0]["value"] == {"tags": ["sample", None]}, result
                predicate = {"kind": "test", "level": "collection", "namespace": "field",
                             "key": "classification", "path": ["tags"], "operator": "contains",
                             "operand_json": "null"}
                query = "quartz nonmatchingword"
                strict = http.tool("search", collection="notes", query=query, mode="lexical", metadata_filter=predicate)
                assert not strict["hits"], strict
                result = stdio.tool("search", collection="notes", query=query, metadata_filter=predicate)
                assert result["mode"] == "lexical" and result["degraded_reason"] == "embedding_disabled"
                hit, = result["hits"]
                assert hit["representation"] == "canonical_utf8"
                assert hit["embedding_state"] == "disabled"
                entry, = hit["metadata"]
                assert entry["level"] == "collection" and json.loads(entry["value_json"]) == {"tags": ["sample", None]}
                assert not hit["metadata_omitted"]
                combined = stdio.tool("search_collections", collections=["reference", "notes"], query="quartz")
                assert [hit["collection"] for hit in combined["hits"]] == ["notes"], combined
                reference = {key: hit[key] for key in ("tenant", "collection", "source_id", "revision_id", "ordinal")}
                exact = stdio.tool("fetch_chunk", **reference)
                assert exact["excerpt"] == content
                assert exact["end_byte"] == len(content.encode("utf-8"))
                context = stdio.tool("fetch_context", **reference, before_bytes=8, after_bytes=8)
                assert context["text"] == content and context["hit"] == exact
                missing = stdio.request("tools/call", {"name": "fetch_chunk", "arguments": reference | {"tenant": "other"}})
                assert missing.get("isError"), missing
                bridge.stdin.close()
                assert bridge.wait(timeout=10) == 0
                timer.cancel()
            grpc = subprocess.run(
                [cli_bin, "--endpoint", f"http://{addresses['grpc']}", "search", "notes", "quartz"],
                check=True, capture_output=True, text=True, timeout=10,
            )
            assert content in grpc.stdout, grpc.stdout
            server.stdin.close()
            assert server.wait(timeout=10) == 0

    print("MCP HTTP and stdio shared the gRPC data, with metadata, exact UTF-8 ranges, and tenant scope verified.")


@contextlib.contextmanager
def hosted_process(server, config, cloud_role=None):
    env = os.environ | {"SANCTUM_MANAGED_STDIO": "1", "NO_COLOR": "1"}
    if cloud_role:
        with socket.socket() as port:
            port.bind(("127.0.0.1", 0))
            env["PORT"] = str(port.getsockname()[1])
        config = config | {"cloud_run_role": cloud_role}
    env["SANCTUM_CONFIG_JSON"] = json.dumps(config)
    with child([server], env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True) as process:
        try:
            addresses = json.loads(process.stdout.readline())
            yield {key: address.replace("0.0.0.0:", "127.0.0.1:") if address else None
                   for key, address in addresses.items()}
        finally:
            process.stdin.close()
            assert process.wait(timeout=15) == 0


def check_hosted(bin_dir):
    """Use a caller-supplied dedicated object storage prefix."""
    suffix = ".exe" if os.name == "nt" else ""
    server = bin_dir / f"sanctum-server{suffix}"
    cli = bin_dir / f"sanctum-cli{suffix}"
    config = json.loads(os.environ["SANCTUM_CONFIG_JSON"])
    scope = {"tenant": "hosted-check", "collection": "notes"}
    text = "Quartz and café notes near the ridge ❄️.\n"
    request = str(uuid.uuid4())

    with hosted_process(server, config, "worker") as worker, \
            hosted_process(server, config, "api") as first, \
            hosted_process(server, config, "api") as second:
        assert first["grpc"] == first["mcp"] and first["worker"] is None
        assert second["grpc"] == second["mcp"] and second["worker"] is None
        assert worker["mcp"] is None and worker["grpc"] is None
        api = f"http://{first['grpc']}"
        first_http = Http(f"{api}/mcp")
        second_http = Http(f"http://{second['mcp']}/mcp")
        rpc = Rpc(first_http)
        rpc.initialize()
        assert first_http.session is None
        initial = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
        try:
            first_http.post(initial, Origin="https://untrusted.example")
        except urllib.error.HTTPError as error:
            with error:
                assert error.code == 403, (error.code, error.read())
        else:
            raise AssertionError("hosted MCP accepted a browser Origin")
        check_tool_annotations(rpc.request("tools/list")["tools"])
        rpc.tool("create_collection", **scope)
        rpc.tool("create_collection", tenant=scope["tenant"], collection="reference")
        first_page = rpc.tool("list_collections", tenant=scope["tenant"], limit=1)
        assert first_page["collections"] == ["notes"]
        assert rpc.tool("list_collections", tenant=scope["tenant"],
                        after=first_page["next_after"])["collections"] == ["reference"]
        initial_metadata = {"namespace": "field", "key": "labels", "value": ["original", None]}
        args = scope | {"name": "notes.md", "text": text, "request_id": request,
                        "source_metadata": [initial_metadata],
                        "chunk_metadata": [{"ordinal": 0, "entries": [initial_metadata]}]}
        accepted = rpc.tool("ingest_text", **args)
        assert accepted["state"] == "accepted" and accepted["operation_id"] == request
        rpc.exchange = second_http
        assert rpc.tool("ingest_text", **args) == accepted
        conflict = rpc.request("tools/call", {"name": "ingest_text", "arguments": args | {"text": "other"}})
        assert conflict["isError"], conflict
        job = scope | {"operation_id": request}
        pending = rpc.tool("scan_pending_text", **job, literal="café")
        start = text.encode().index("café".encode())
        assert pending["matches"] == [{"start_byte": start, "end_byte": start + len("café".encode())}]
        assert pending["stop"] == "complete" and pending["scanned_bytes"] == len(text.encode())
        for stage, state in [("register", "registered"), ("prepare", "registered"), ("index", "text_ready")]:
            delivery = urllib.request.Request(f"http://{worker['worker']}/internal/tasks/{stage}-upload",
                                              json.dumps(job).encode(),
                                              {"Content-Type": "application/json"})
            with urllib.request.urlopen(delivery, timeout=30) as response:
                assert response.status == 204
            status = rpc.tool("ingestion_status", **job)
            assert status["state"] == state, status
            if stage == "prepare":
                assert status["preparation"] == {
                    "ready": {"canonical_bytes": len(text.encode()), "chunks": 1}
                }, status
                assert status["processed_bytes"] == len(text.encode()), status
                result = rpc.tool("search", **scope, query="quartz")
                assert not result["hits"], result
                grpc_job = subprocess.run(
                    [cli, "--endpoint", api, "--tenant", scope["tenant"], "job", "notes", request],
                    check=True, capture_output=True, text=True, timeout=10,
                )
                assert "Awaiting indexing" in grpc_job.stdout, grpc_job.stdout
            else:
                assert status["preparation"] is None, status

        edit = {"namespace": "field", "key": "classification", "operation": "set",
                "value": {"tags": ["sample", None]}}
        rpc.tool("mutate_metadata", **scope, edits=[edit])
        rpc.exchange = first_http
        assert rpc.tool("get_metadata", **scope)["entries"][0]["value"] == edit["value"]
        predicate = {"kind": "test", "level": "collection", "namespace": "field",
                     "key": "classification", "path": ["tags"], "operator": "contains",
                     "operand_json": "null"}
        result = rpc.tool("search_collections", tenant=scope["tenant"], collections=["notes", "reference"],
                          query="quartz", metadata_filter=predicate)
        assert result["mode"] == "lexical" and result["degraded_reason"] == "embedding_disabled"
        hit, = result["hits"]
        entry, = hit["metadata"]
        assert entry["level"] == "collection" and json.loads(entry["value_json"]) == edit["value"]
        assert hit["database_generation"] and not hit["metadata_omitted"]
        reference = {key: hit[key] for key in ("tenant", "collection", "source_id", "revision_id", "ordinal")}
        source_ref = {key: value for key, value in reference.items() if key != "ordinal"}
        assert rpc.tool("get_metadata", **source_ref)["entries"] == [initial_metadata]
        assert rpc.tool("get_metadata", **reference)["entries"] == [initial_metadata]
        exact = rpc.tool("fetch_chunk", **reference)
        context = rpc.tool("fetch_context", **reference)
        assert context["text"] == text and context["hit"] == exact
        rpc.tool("mutate_metadata", **reference, edits=[edit])
        rpc.exchange = second_http
        entries = {entry["key"]: entry["value"] for entry in rpc.tool("get_metadata", **reference)["entries"]}
        assert entries == {edit["key"]: edit["value"], initial_metadata["key"]: initial_metadata["value"]}
        assert exact["excerpt"] == text.encode()[exact["start_byte"]:exact["end_byte"]].decode() == text
        missing = rpc.request("tools/call", {"name": "fetch_chunk",
                                             "arguments": reference | {"tenant": "other"}})
        assert missing["isError"], missing
        grpc = subprocess.run([cli, "--endpoint", api, "--tenant", scope["tenant"], "search", "notes", "quartz"],
                              check=True, capture_output=True, text=True, timeout=10)
        assert text.strip() in grpc.stdout, grpc.stdout

        h2_request = {"jsonrpc": "2.0", "id": 8, "method": "tools/list"}
        response = subprocess.run(
            ["curl", "--http2-prior-knowledge", "--fail", "--silent", "--show-error", "--max-time", "10",
             "-H", "Content-Type: application/json", "-H", "Accept: application/json, text/event-stream",
             "-H", "MCP-Protocol-Version: 2025-11-25", "--data", json.dumps(h2_request), f"{api}/mcp"],
            check=True, capture_output=True, text=True,
        )
        h2_result = json.loads(response.stdout)
        assert h2_result["id"] == 8 and "tools" in h2_result["result"], h2_result

        rtf = r"{\rtf1\ansi Prepared caf\'e9}"
        canonical = "Prepared café".encode()
        rtf_job = rpc.tool("ingest_text", **scope, name="prepared.rtf", text=rtf)
        rtf_operation = scope | {"operation_id": rtf_job["operation_id"]}
        pending = rpc.tool("scan_pending_text", **rtf_operation, literal="café")
        assert pending["stop"] == "extraction_pending" and pending["read_bytes"] == 0, pending
        assert pending["source"]["total_bytes"] == len(rtf.encode()), pending
        for stage in ("register", "prepare"):
            delivery = urllib.request.Request(f"http://{worker['worker']}/internal/tasks/{stage}-upload",
                                              json.dumps(rtf_operation).encode(),
                                              {"Content-Type": "application/json"})
            with urllib.request.urlopen(delivery, timeout=30) as response:
                assert response.status == 204
        pending = rpc.tool("scan_pending_text", **rtf_operation, literal="café")
        assert pending["stop"] == "complete" and pending["read_bytes"] == len(canonical), pending
        assert pending["source"]["total_bytes"] == len(canonical), pending
        assert pending["matches"] == [{"start_byte": canonical.index(b"caf"), "end_byte": len(canonical)}]
        limited = rpc.tool("scan_pending_text", **rtf_operation, literal="café",
                           byte_limit=len(canonical) - 1)
        assert limited["stop"] == "byte_limit" and not limited["matches"], limited
        assert limited["read_bytes"] == len(canonical) - 1, limited
        assert limited["scanned_bytes"] == len(canonical) - len("é".encode()), limited
        assert not rpc.tool("search", **scope, query="prepared")["hits"]
    print("Hosted MCP verified HTTP/1.1, h2c, cross-process requests, and pending RTF byte ranges.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bin_dir", default="target/debug", nargs="?", type=Path)
    parser.add_argument("--hosted", action="store_true", help="read isolated object storage config from SANCTUM_CONFIG_JSON")
    args = parser.parse_args()
    (check_hosted if args.hosted else check)(args.bin_dir.resolve())
