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
├── index.ts              # MCP server (single file, following tutorial pattern)
├── config.ts             # Environment configuration
└── clients/              # External service clients
    ├── pg.ts             # Postgres connection
    └── minio.ts          # MinIO client
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

## Environment Variables

```bash
MCP_PORT=3001                                      # Server port
DATABASE_URL=postgresql://osint:osint@postgres:5432/osint
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio12345
MINIO_BUCKET=osint-raw
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
