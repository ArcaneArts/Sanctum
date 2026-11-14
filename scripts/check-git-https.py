"""Check private Git import through a real smart HTTP backend with verified TLS."""

import argparse
import base64
import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import runpy
import secrets
import ssl
import subprocess
import tempfile
import threading
import time
from urllib.parse import urlsplit


MCP = runpy.run_path(str(Path(__file__).with_name("check-local-mcp.py")))
COLLECTION = "notes"
TOKEN_ENV = "SANCTUM_GIT_FIXTURE_TOKEN"
FIRST = "Café notes on rhodonite.\n"
SECOND = "Café notes on citrine.\n"
THIRD = "Café notes on peridot.\n"


def git(path, *args):
    return subprocess.check_output(["git", "-C", path, *args], text=True).strip()


def commit(repo, text):
    (repo / "notes.md").write_text(text, encoding="utf-8")
    git(repo, "add", "notes.md")
    git(repo, "-c", "user.name=Sanctum", "-c", "user.email=test@example.invalid",
        "-c", "commit.gpgsign=false", "commit", "-m", "Git import fixture")
    return git(repo, "rev-parse", "HEAD")


def make_certificate(directory):
    cert, key = directory / "fixture.pem", directory / "fixture-key.pem"
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
         "-subj", "/CN=127.0.0.1", "-addext", "subjectAltName=IP:127.0.0.1",
         "-keyout", key, "-out", cert],
        check=True, capture_output=True,
    )
    return cert, key


@contextlib.contextmanager
def origin(directory, cert, key, authorizations):
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(cert, key)
    requests = []
    fetch_waiting = threading.Event()
    fetch_closed = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def handle(self):
            try:
                super().handle()
            except (BrokenPipeError, ConnectionResetError):
                # Git may abandon the TLS connection without sending an HTTP request.
                pass

        def do_GET(self):
            self.serve_git()

        def do_POST(self):
            self.serve_git()

        def begin_response(self, status, length):
            self.send_response(status)
            self.send_header("Connection", "close")
            self.send_header("Content-Length", str(length))
            self.close_connection = True

        def serve_git(self):
            url = urlsplit(self.path)
            _, scheme, path = url.path.split("/", 2)
            given = self.headers.get("Authorization")
            valid = scheme in authorizations and given == authorizations[scheme]
            requests.append((scheme, self.command, given is not None, valid))
            if scheme == "redirect":
                self.begin_response(302, 0)
                self.send_header("Location", f"{self.server.redirect_to}/basic/{path}?{url.query}")
                self.end_headers()
                return
            if not valid:
                self.begin_response(401, 0)
                self.send_header("WWW-Authenticate", 'Basic realm="Sanctum fixture"')
                self.end_headers()
                return
            if scheme == "stall":
                self.connection.settimeout(12)
                try:
                    fetch_waiting.set()
                    if not self.connection.recv(1):
                        fetch_closed.set()
                except (ConnectionResetError, ssl.SSLEOFError):
                    fetch_closed.set()
                except TimeoutError:
                    pass
                self.close_connection = True
                return
            length = int(self.headers.get("Content-Length", 0))
            env = {
                "PATH": os.environ["PATH"],
                "GIT_PROJECT_ROOT": str(directory),
                "GIT_HTTP_EXPORT_ALL": "1",
                "PATH_INFO": "/" + path,
                "QUERY_STRING": url.query,
                "REQUEST_METHOD": self.command,
                "CONTENT_TYPE": self.headers.get("Content-Type", ""),
                "CONTENT_LENGTH": str(length),
                "HTTP_GIT_PROTOCOL": self.headers.get("Git-Protocol", ""),
            }
            reply = subprocess.check_output(
                ["git", "http-backend"], input=self.rfile.read(length), env=env,
            )
            headers, body = reply.split(b"\r\n\r\n", 1)
            status = 200
            response_headers = []
            for line in headers.decode("ascii").split("\r\n"):
                name, value = line.split(": ", 1)
                if name.lower() == "status":
                    status = int(value.split()[0])
                else:
                    response_headers.append((name, value))
            self.begin_response(status, len(body))
            for name, value in response_headers:
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    def link(server):
        return f"https://127.0.0.1:{server.server_port}"

    with (
        ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server,
        ThreadingHTTPServer(("127.0.0.1", 0), Handler) as other,
    ):
        server.redirect_to = link(other)
        other.redirect_to = link(server)
        server.socket = tls.wrap_socket(server.socket, server_side=True)
        other.socket = tls.wrap_socket(other.socket, server_side=True)
        threads = [threading.Thread(target=entry.serve_forever) for entry in (server, other)]
        for thread in threads:
            thread.start()
        try:
            yield link(server), link(other), requests, fetch_waiting, fetch_closed
        finally:
            server.shutdown()
            other.shutdown()
            for thread in threads:
                thread.join()


@contextlib.contextmanager
def sanctum(bin_dir, root, origins, ca, token):
    config = {
        "version": 1,
        "storage_root": str(root),
        "grpc_listen": "127.0.0.1:0",
        "mcp_listen": "127.0.0.1:0",
        "url_import_origins": origins,
    }
    env = os.environ | {
        "SANCTUM_CONFIG_JSON": json.dumps(config),
        "SANCTUM_MANAGED_STDIO": "1",
        "RUST_LOG": "warn",
        TOKEN_ENV: token,
    }
    env.pop("GIT_SSL_CAINFO", None)
    if ca is not None:
        env["GIT_SSL_CAINFO"] = str(ca)
    with MCP["child"]([bin_dir / "sanctum-server"], env=env, stdin=subprocess.PIPE,
                      stdout=subprocess.PIPE, text=True) as server:
        try:
            addresses = json.loads(server.stdout.readline())
            cli = [bin_dir / "sanctum-cli", "--endpoint", f"http://{addresses['grpc']}"]
            yield cli, f"http://{addresses['mcp']}/mcp", server
        finally:
            if not server.stdin.closed:
                server.stdin.close()
            assert server.wait(timeout=15) == 0


def import_git(cli, url, authorizations, *, error=None):
    reply = subprocess.run(
        [*cli, "import-git", COLLECTION, "--https-url", url, "--ref", "refs/heads/main"],
        capture_output=True, text=True, timeout=20,
    )
    assert all(value not in reply.stdout and value not in reply.stderr for value in authorizations)
    if error:
        assert reply.returncode != 0 and error in reply.stderr, reply.stderr
        return
    assert reply.returncode == 0, reply.stderr
    state, path, operation, commit_id = reply.stdout.rstrip("\n").split("\t")
    assert path == "notes.md", path
    return state, operation, commit_id


def published(cli, mcp, operation, query, expected):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        result = subprocess.check_output([*cli, "job", COLLECTION, operation], text=True, timeout=5)
        if "INGESTION_STATE_TEXT_READY" in result:
            break
        assert "INGESTION_STATE_FAILED" not in result, result
        time.sleep(0.05)
    else:
        raise AssertionError("Git source did not reach text readiness")
    hit, = mcp.tool("search", collection=COLLECTION, query=query, mode="lexical")["hits"]
    ref = {key: hit[key] for key in ("tenant", "collection", "source_id", "revision_id", "ordinal")}
    excerpt = mcp.tool("fetch_chunk", **ref)["excerpt"]
    assert excerpt == expected and hit["excerpt"] == expected
    return hit["source_id"], hit["revision_id"]


def check(bin_dir, directory):
    repo = directory / "repo"
    repo.mkdir()
    git(repo, "init", "-b", "main", "--template=")
    first_commit = commit(repo, FIRST)
    cert, key = make_certificate(directory)
    root = directory / "storage"
    token = secrets.token_urlsafe()
    basic = "Basic " + base64.b64encode(f"fixture:{token}".encode()).decode()
    authorizations = {"basic": basic, "bearer": f"Bearer {token}", "stall": basic}
    with origin(directory, cert, key, authorizations) as fixture:
        url, other, requests, fetch_waiting, fetch_closed = fixture
        secrets_in_headers = (token, basic.removeprefix("Basic "))
        basic_config = {url: {"username": "fixture", "password_env": TOKEN_ENV}}
        with sanctum(bin_dir, root, basic_config, cert, token) as (cli, mcp_url, server):
            subprocess.check_call([*cli, "create", COLLECTION], stdout=subprocess.DEVNULL)
            mcp = MCP["Rpc"](MCP["Http"](mcp_url))
            mcp.initialize()
            remote = url + "/basic/repo/.git"
            state, operation, commit_id = import_git(cli, remote, secrets_in_headers)
            assert (state, commit_id) == ("IMPORT_STATE_QUEUED", first_commit)
            source, revision = published(cli, mcp, operation, "rhodonite", FIRST)
            reused, _, commit_id = import_git(cli, remote, secrets_in_headers)
            assert (reused, commit_id) == ("IMPORT_STATE_UNCHANGED", first_commit)
            second_commit = commit(repo, SECOND)
            state, operation, commit_id = import_git(cli, remote, secrets_in_headers)
            assert (state, commit_id) == ("IMPORT_STATE_QUEUED", second_commit)
            current, replacement = published(cli, mcp, operation, "citrine", SECOND)
            assert current == source and replacement != revision

            start = len(requests)
            import_git(cli, url + "/redirect/repo/.git", secrets_in_headers,
                       error="Git HTTPS fetch failed")
            assert requests[start:] == [("redirect", "GET", True, False)]
            start = len(requests)
            import_git(cli, other + "/basic/repo/.git", secrets_in_headers,
                       error="URL import origin is not allowed")
            assert len(requests) == start

            for stop in ("disconnect", "shutdown"):
                fetch_waiting.clear()
                fetch_closed.clear()
                command = [*cli, "import-git", COLLECTION, "--https-url", url + "/stall/repo/.git"]
                with MCP["child"](command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as client:
                    assert fetch_waiting.wait(5), "Git did not start the HTTPS request"
                    if stop == "disconnect":
                        client.kill()
                        client.wait(timeout=5)
                    else:
                        server.stdin.close()
                        out, error = client.communicate(timeout=5)
                        assert client.returncode != 0 and not out, (out, error)
                        assert "server is shutting down" in error, error
                        assert server.wait(timeout=5) == 0
                    assert fetch_closed.wait(5), f"Git left the HTTPS connection open after {stop}"

        with sanctum(bin_dir, root, {url: None}, cert, token) as (cli, _, _):
            start = len(requests)
            import_git(cli, url + "/basic/repo/.git", secrets_in_headers,
                       error="Git HTTPS fetch failed")
            assert requests[start:] and all(
                scheme == "basic" and not header and not valid
                for scheme, _, header, valid in requests[start:]
            )

        with sanctum(bin_dir, root, basic_config, None, token) as (cli, _, _):
            start = len(requests)
            import_git(cli, url + "/basic/repo/.git", secrets_in_headers,
                       error="Git HTTPS fetch failed")
            assert len(requests) == start

        with sanctum(bin_dir, root, {url: TOKEN_ENV}, cert, token) as (cli, mcp_url, _):
            third_commit = commit(repo, THIRD)
            mcp = MCP["Rpc"](MCP["Http"](mcp_url))
            mcp.initialize()
            state, operation, commit_id = import_git(cli, url + "/bearer/repo/.git", secrets_in_headers)
            assert (state, commit_id) == ("IMPORT_STATE_QUEUED", third_commit)
            published(cli, mcp, operation, "peridot", THIRD)

        for scheme in ("basic", "bearer"):
            assert (scheme, "GET", True, True) in requests
            assert (scheme, "POST", True, True) in requests


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", default=Path("target/debug"), type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="sanctum-git-https-") as temp:
        check(args.bin_dir.resolve(), Path(temp))
    print("Private Git HTTPS imports passed with Basic and Bearer credentials.")
    print("Stalled HTTPS fetches closed on client disconnect and server shutdown.")


if __name__ == "__main__":
    main()
