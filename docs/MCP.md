# Local MCP

The Sanctum server exposes [MCP Streamable HTTP](https://modelcontextprotocol.io/specification/latest/basic/transports) at
`http://127.0.0.1:47551/mcp` by default. It shares the collection catalog,
SQLite databases, ingestion queue, and embedding profile with gRPC.

```sh
SANCTUM_CONFIG_JSON='{"version":1,"storage_root":"./local-data"}' \
  cargo run --locked -p sanctum-server
```

In a client that supports Streamable HTTP, add a server with the URL above.
For a client that launches a stdio subprocess, build `sanctum-cli` and use its
absolute path as the command with `mcp-stdio` as the argument.

```sh
cargo build --locked -p sanctum-cli
```

```json
{
  "command": "/absolute/path/to/Sanctum/target/debug/sanctum-cli",
  "args": ["mcp-stdio", "--mcp-url", "http://127.0.0.1:47551/mcp"]
}
```

The stdio subprocess connects to the running server. The HTTP address is
configurable with `mcp_listen` and `null` disables the listener. The local
listener accepts loopback addresses, checks the HTTP Host, and rejects
requests carrying a browser Origin header. Both transports are for trusted
local clients. Set `tenant` when operating outside the default tenant.

## Tools

| Tool | Parameters and result |
| --- | --- |
| `create_collection` | Supply `collection` and optional `tenant`. Returns the created scope. |
| `list_collections` | Supply optional `tenant`, `after`, and `limit` (1–99, default 25). Pass `next_after` back to get the next page. |
| `ingest_text` | Supply `collection`, `name`, and `text` (up to 128 KiB). Optional `external` has `namespace` and `id`. For replacement, set `replace_source_id` and optionally `expected_revision_id`. Returns a durable operation ID. |
| `ingestion_status` | Supply `collection` and `operation_id`. Reports queue state, processed bytes, attempts, and failure details. |
| `search` | Supply `collection`, `query`, optional `mode` (default hybrid), optional `metadata_filter`, and optional `limit` (1–10, default 5). Returns excerpts and exact source references, with the strategy used and embedding coverage. |
| `fetch_chunk` | Supply `collection`, `source_id`, `revision_id`, and `ordinal` from a hit. Returns the exact UTF-8 excerpt and its half-open byte range. |
| `get_metadata` | Supply `collection` for collection metadata, both `source_id` and `revision_id` for the current source, or those IDs and `ordinal` for a chunk. Returns up to 64 namespaced entries, subject to the shared 512 KiB value limit. |
| `mutate_metadata` | Supply the same target as `get_metadata` plus up to 32 `edits`. Each edit uses `namespace`, `key`, and `operation` (`set`, `patch`, or `remove`). Set and patch also take `value`. |

Call `ingestion_status` with the returned operation ID until it reports
`text_ready` before searching for the newly submitted text. Use the
[gRPC CLI](LOCAL_API.md) for large files and directory imports. MCP HTTP
requests have a 2 MiB JSON body limit.

For example, search for quartz in chunks whose current source metadata
contains the `sample` tag.

```json
{
  "collection": "notes",
  "query": "quartz",
  "mode": "hybrid",
  "metadata_filter": {
    "kind": "test",
    "level": "source",
    "namespace": "field",
    "key": "classification",
    "path": ["tags"],
    "operator": "contains",
    "operand_json": "\"sample\""
  }
}
```

Paths accept literal property names and zero-based array indices. Group
predicates with `{"kind":"all","filters":[...]}` or
`{"kind":"any","filters":[...]}`, or negate one using
`{"kind":"not","filter":{...}}`. Operators and value comparisons follow the
[gRPC metadata filter](LOCAL_API.md#metadata-and-changes).

## Local integration check

```sh
cargo build --locked -p sanctum-server -p sanctum-cli
python3 scripts/check-local-mcp.py
```

The check launches both binaries with a temporary data directory and
ephemeral listener ports. It exercises MCP HTTP and stdio tool discovery,
ingestion, metadata, search, exact byte ranges, and gRPC access to the
same ingested content.
