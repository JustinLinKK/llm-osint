# OSINT MCP Server

Production-grade Model Context Protocol (MCP) server for OSINT tool execution.

Built following the [official MCP tutorial](https://modelcontextprotocol.io/docs/develop/build-server).

## Architecture

This is a **proper MCP server** following the [Model Context Protocol specification](https://modelcontextprotocol.io):

- **Protocol**: JSON-RPC 2.0
- **Transport**: SSE (Server-Sent Events) for HTTP
- **SDK**: `@modelcontextprotocol/sdk`

## Structure

```
src/
├── index.ts              # MCP server entrypoint
├── config.ts             # Environment configuration
├── tools/                # MCP tools (one per file)
│   ├── fetch_url.ts
│   ├── ingest_text.ts
│   └── ingest_graph_entity.ts
│   └── tools_python/      # Python tools used via MCP bridge
└── clients/              # External service clients
    ├── pg.ts             # Postgres connection
    └── minio.ts          # MinIO client
scripts/
└── test-ingest-graph.ts   # MCP graph-ingest test script
└── test-ingest-text.ts    # MCP vector ingest test script
└── test-fetch-url.ts      # MCP fetch URL test script
└── test-python-tools.ts    # MCP Python tool bridge test script
```

## Endpoints

### `GET /health`
Health check endpoint.

**Response:**
```json
{"ok": true}
```

### `GET /sse`
MCP SSE endpoint - clients connect here.

Follows the official MCP SSE transport pattern.

## Tools

### `fetch_url`
Fetches a URL via HTTP GET and stores raw response to MinIO + Postgres.

**Input:**
```json
{
  "runId": "uuid",
  "url": "https://example.com"
}
```

**Output:**
```json
{
  "documentId": "uuid",
  "bucket": "osint-raw",
  "objectKey": "runs/.../raw/html/...",
  "etag": "...",
  "sizeBytes": 1234,
  "contentType": "text/html"
}
```

**Side effects:**
- Stores raw bytes to MinIO
- Inserts document + document_object to Postgres
- Emits `TOOL_CALL_STARTED` and `TOOL_CALL_FINISHED` events
- Logs to `tool_calls` table

### `ingest_text`
Ingests raw text into Postgres + Qdrant (chunk → embed → upsert).

### `ingest_graph_entity`
Ingests graph entities and relationships with evidence. Locations merge by lat/lon with a distance threshold.

**Mitigations applied:**
- **Location**: requires `lat/lon` (or address to geocode) and merges within a distance threshold.
- **Email**: normalized to lowercase before merge (`address_normalized`).
- **Domain**: normalized to lowercase and strips leading `www.`.
- **Article URL**: normalized (lowercased host, hash removed, trailing slash trimmed).
- **Person/Organization name**: normalized (`name_normalized`) used when no stable ID is provided.

## Environment Variables

```bash
MCP_PORT=3001                                      # Server port
DATABASE_URL=postgresql://osint:osint@postgres:5432/osint
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio12345
MINIO_BUCKET=osint-raw
PYTHON_BIN=python3                                 # Optional override
MCP_PYTHON_TOOLS='[]'                              # Optional JSON tool config
X_BEARER_TOKEN=                                    # Optional, X API
BROWSERBASE_API_KEY=                               # Optional, Browserbase
BROWSERBASE_PROJECT_ID=                            # Optional, Browserbase
LINKEDIN_EMAIL=                                    # Optional, LinkedIn scraping
LINKEDIN_PASSWORD=                                 # Optional, LinkedIn scraping
X_TEST_USERNAME=                                   # Optional, test helper
```

See the example environment file in
[apps/mcp-server/src/tools/tools_python/env.example](apps/mcp-server/src/tools/tools_python/env.example).

## Python Tool Bridge (Option B)

You can keep the Node MCP server and call Python tools as subprocesses.
Define tools in `MCP_PYTHON_TOOLS` and provide a Python script path. Each script reads JSON
from stdin and returns JSON on stdout.

Example config:

```bash
MCP_PYTHON_TOOLS='[
  {
    "name": "x_get_user_posts",
    "description": "Fetch X posts via API",
    "scriptPath": "apps/mcp-server/src/tools/tools_python/get_user_posts.py",
    "timeoutMs": 30000
  },
  {
    "name": "x_get_user_posts_browserbase",
    "description": "Fetch X posts via Browserbase",
    "scriptPath": "apps/mcp-server/src/tools/tools_python/get_user_posts_browserbase.py",
    "timeoutMs": 60000
  },
  {
    "name": "linkedin_get_posts_browserbase",
    "description": "Fetch LinkedIn posts via Browserbase",
    "scriptPath": "apps/mcp-server/src/tools/tools_python/get_linkedin_posts_browserbase.py",
    "timeoutMs": 90000
  }
]'
```

Python tool protocol:

- **stdin**: `{ "tool": "extract_entities", "input": { ... } }`
- **stdout**: `{ "ok": true, "result": { ... } }` or `{ "ok": false, "error": "..." }`

Minimal Python wrapper:

```python
import json
import sys

def main():
    payload = json.load(sys.stdin)
    tool = payload.get("tool")
    input_data = payload.get("input", {})

    # TODO: call your actual tool code here.
    result = {"echo": input_data, "tool": tool}

    json.dump({"ok": True, "result": result}, sys.stdout)

if __name__ == "__main__":
    main()
```

## Running

### Development
```bash
yarn dev
```

### Production (Docker)
```bash
docker compose up mcp-server
```

## Test: Graph Ingest Tool

Run the MCP graph-ingest test script (uses stdio transport and inserts a run if needed):

```bash
RUN_ID=<optional-uuid> yarn tsx apps/mcp-server/scripts/test-ingest-graph.ts
```

## Test: Vector Ingest Tool

```bash
RUN_ID=<optional-uuid> yarn tsx apps/mcp-server/scripts/test-ingest-text.ts
```

## Test: Fetch URL Tool

```bash
RUN_ID=<optional-uuid> yarn tsx apps/mcp-server/scripts/test-fetch-url.ts
```

## Test: Python Tool Bridge

Configure a tool in `MCP_PYTHON_TOOLS` and ensure required credentials exist in `.env`.
For the default test, set `X_TEST_USERNAME` (and `X_BEARER_TOKEN` for the API tool).

```bash
RUN_ID=<optional-uuid> yarn tsx apps/mcp-server/scripts/test-python-tools.ts
```

## Client Usage (Python)

```python
from mcp import ClientSession
from mcp.client.sse import sse_client

async with sse_client("http://mcp-server:3001/sse") as (read, write):
    async with ClientSession(read, write) as session:
        # Initialize
        await session.initialize()
        
        # List tools
        tools = await session.list_tools()
        
        # Call tool
        result = await session.call_tool("fetch_url", {
            "runId": "...",
            "url": "https://example.com"
        })
```

## Design Principles

1. **Evidence-first**: Raw bytes always stored in MinIO
2. **Auditable**: Every tool call logged to `tool_calls` table
3. **Observable**: Events emitted to `run_events` for UI streaming
4. **Stateless tools**: Tools are pure functions
5. **MCP compliant**: Follows official specification exactly
