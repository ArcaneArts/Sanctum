"""Exercise the built Sanctum binaries across MCP HTTP, MCP stdio and gRPC."""

import contextlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import threading
import tempfile
import time
import urllib.error
import urllib.request


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
    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.session = None

    def post(self, message, **extra_headers):
        headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
        if self.session:
            headers.update({"Mcp-Session-Id": self.session, "MCP-Protocol-Version": "2025-11-25"})
        request = urllib.request.Request(self.endpoint, json.dumps(message).encode(),
                                         headers=headers | extra_headers)
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
        with log_path.open("w") as log, child(
            [server_bin], stderr=log, env=os.environ | {"SANCTUM_CONFIG_JSON": config, "NO_COLOR": "1"},
        ) as server:
            for _ in range(100):
                logs = log_path.read_text()
                addresses = dict(re.findall(r"Sanctum (MCP|gRPC) listening.*?address=(127\.0\.0\.1:\d+)", logs))
                if len(addresses) == 2:
                    break
                assert server.poll() is None, logs
                time.sleep(.05)
            assert len(addresses) == 2, logs
            endpoint = f"http://{addresses['MCP']}/mcp"

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
            assert len(tools) == 8, tools
            http.tool("create_collection", collection="notes")
            http.tool("create_collection", collection="reference")
            first = http.tool("list_collections", limit=1)
            assert first["collections"] == ["notes"], first
            assert http.tool("list_collections", after=first["next_after"])["collections"] == ["reference"]

            content = "Snow crystals near the ridge ❄️ and a quartz sample."
            job = http.tool("ingest_text", collection="notes", name="field.md", text=content,
                            external={"namespace": "field", "id": "note-1"})
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
                result = stdio.tool("search", collection="notes", query="quartz", metadata_filter=predicate)
                assert result["mode"] == "lexical" and result["degraded_reason"] == "embedding_disabled"
                hit, = result["hits"]
                reference = {key: hit[key] for key in ("tenant", "collection", "source_id", "revision_id", "ordinal")}
                exact = stdio.tool("fetch_chunk", **reference)
                assert exact["excerpt"] == content
                assert exact["end_byte"] == len(content.encode("utf-8"))
                missing = stdio.request("tools/call", {"name": "fetch_chunk", "arguments": reference | {"tenant": "other"}})
                assert missing.get("isError"), missing
                bridge.stdin.close()
                assert bridge.wait(timeout=10) == 0
                timer.cancel()
            grpc = subprocess.run(
                [cli_bin, "--endpoint", f"http://{addresses['gRPC']}", "search", "notes", "quartz"],
                check=True, capture_output=True, text=True, timeout=10,
            )
            assert content in grpc.stdout, grpc.stdout
    print("MCP HTTP and stdio shared the gRPC data, with metadata, exact UTF-8 ranges, and tenant scope verified.")


if __name__ == "__main__":
    import sys

    check(Path(sys.argv[1] if len(sys.argv) > 1 else "target/debug").resolve())
