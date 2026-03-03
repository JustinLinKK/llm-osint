# MCP Client Integration Guide

This repo now uses **HTTP MCP**, not stdio and not the older `/sse` endpoint shape.

## Transport

- Server URL: `http://localhost:3001/mcp`
- Protocol: JSON-RPC 2.0 over **Streamable HTTP**
- Session model: initialize once, reuse `mcp-session-id`, close with `DELETE`

LangGraph uses the HTTP client implementation in [mcp_client.py](/workspaces/llm-osint/services/agent-langgraph/src/mcp_client.py).

## Node.js Client

```ts
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const transport = new StreamableHTTPClientTransport(
  new URL("http://localhost:3001/mcp")
);

const client = new Client({ name: "example-client", version: "1.0.0" }, { capabilities: {} });
await client.connect(transport);

const result = await client.callTool({
  name: "fetch_url",
  arguments: {
    runId: "550e8400-e29b-41d4-a716-446655440000",
    url: "https://example.com"
  }
});

await client.close();
```

## Python Client

The repo's Python client is an HTTP session wrapper rather than the official async SDK.

```python
from mcp_client import StreamableHttpMcpClient

client = StreamableHttpMcpClient("http://localhost:3001/mcp")
client.start()

result = client.call_tool(
    "fetch_url",
    {
        "runId": "550e8400-e29b-41d4-a716-446655440000",
        "url": "https://example.com",
    },
)

client.close()
```

## Session Flow

1. `initialize`
2. `notifications/initialized`
3. `tools/call`
4. optional additional `tools/call`
5. `DELETE /mcp`

The SDK transport handles this for Node clients. The repo's Python client handles it internally.

## Example Test Script

Use the existing HTTP client script:

```bash
RUN_ID=<optional-uuid> yarn tsx apps/mcp-server/scripts/test-python-tools.ts
```

That script:

- creates a run row if needed
- connects to `MCP_SERVER_URL`
- calls one configured tool
- prints the MCP response payload

## Environment

Typical local values:

```bash
MCP_SERVER_URL=http://localhost:3001/mcp
DATABASE_URL=postgresql://osint:osint@localhost:5432/osint
```

Inside Docker/dev-container networked services, use service DNS names instead of `localhost`.

## Current Repo Reality

- `apps/mcp-server/src/index.ts` serves HTTP MCP
- `services/agent-langgraph/src/mcp_client.py` uses HTTP MCP by default
- older stdio/SSE examples are no longer the primary integration path
