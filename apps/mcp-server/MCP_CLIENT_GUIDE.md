# MCP Client Integration Guide

This guide explains how clients connect to the MCP server in the dev container.

## Architecture Overview

The MCP server uses **stdio transport** (standard input/output), not HTTP:

```
Client Process                    MCP Server Process
     |                                    |
     |  spawn() as child process          |
     |---------------------------------->>|
     |                                    |
     |  stdin/stdout pipes                |
     |<================================>>|
     |                                    |
     |  JSON-RPC 2.0 messages             |
     |<================================>>|
```

## Connection Methods

### 1. Node.js Client (Programmatic)

Use the official `@modelcontextprotocol/sdk` client:

```typescript
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { spawn } from "child_process";

// Spawn the server
const serverProcess = spawn("node", ["dist/index.js"], {
  cwd: "/workspaces/llm-osint/apps/mcp-server",
  stdio: ["pipe", "pipe", "inherit"],
});

// Create transport and client
const transport = new StdioClientTransport({ command: serverProcess });
const client = new Client({ name: "my-client", version: "1.0.0" }, { capabilities: {} });

// Connect
await client.connect(transport);

// Use the client
const tools = await client.listTools();
const result = await client.callTool({
  name: "fetch_url",
  arguments: { runId: "...", url: "..." }
});

// Cleanup
await client.close();
serverProcess.kill();
```

### 2. Python Client (LangGraph Agent)

For the planned Python LangGraph agent:

```python
import subprocess
import json

# Spawn the server
server_process = subprocess.Popen(
    ["node", "dist/index.js"],
    cwd="/workspaces/llm-osint/apps/mcp-server",
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True
)

# Send JSON-RPC request
request = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {
        "name": "fetch_url",
        "arguments": {
            "runId": "550e8400-e29b-41d4-a716-446655440000",
            "url": "https://example.com"
        }
    }
}

server_process.stdin.write(json.dumps(request) + "\n")
server_process.stdin.flush()

# Read response
response = json.loads(server_process.stdout.readline())
print(response)

# Cleanup
server_process.terminate()
```

Or use the official Python MCP SDK:
```bash
pip install mcp
```

### 3. Claude Desktop Configuration

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "osint": {
      "command": "node",
      "args": ["/workspaces/llm-osint/apps/mcp-server/dist/index.js"],
      "env": {
        "DATABASE_URL": "postgresql://osint:osint@postgres:5432/osint",
        "MINIO_ENDPOINT": "http://minio:9000",
        "MINIO_ACCESS_KEY": "minio",
        "MINIO_SECRET_KEY": "minio12345",
        "MINIO_BUCKET": "osint-raw"
      }
    }
  }
}
```

For development mode:
```json
{
  "mcpServers": {
    "osint": {
      "command": "yarn",
      "args": ["--cwd", "/workspaces/llm-osint/apps/mcp-server", "tsx", "src/index.ts"]
    }
  }
}
```

## Testing the Connection

### Run the Example Client

We've included a sample client that demonstrates the full connection flow:

```bash
cd apps/mcp-server

# Make sure infrastructure is running
docker compose -f ../../infra/docker/docker-compose.yml up -d

# Run the example client
yarn example
```

This will:
1. Spawn the MCP server as a child process
2. Connect via stdio transport
3. List available tools
4. Call the `fetch_url` tool with a test URL
5. Display the result
6. Clean up and exit

### Expected Output

```
Starting MCP client example...

Connecting to MCP server...
✓ Connected to MCP server

Fetching available tools...
✓ Available tools:
  - fetch_url: Fetch a URL via HTTP GET and store the raw response to MinIO + Postgres

Calling fetch_url tool with:
  runId: 550e8400-e29b-41d4-a716-446655440000
  url: https://example.com

✓ Tool execution result:
{
  "content": [
    {
      "type": "text",
      "text": "{\"documentId\":\"...\",\"bucket\":\"osint-raw\",\"objectKey\":\"...\",\"etag\":\"...\",\"sizeBytes\":1256,\"contentType\":\"text/html\",\"sourceType\":\"html\",\"sha256\":\"...\"}"
    }
  ]
}

Closing connection...
✓ Connection closed
```

## Environment Variables

The MCP server needs these environment variables to connect to infrastructure:

```bash
DATABASE_URL=postgresql://osint:osint@postgres:5432/osint
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio12345
MINIO_BUCKET=osint-raw
```

In the dev container, these are automatically available when using Docker service names (`postgres`, `minio`, etc.).

## Key Differences from HTTP APIs

| Aspect | HTTP API | MCP Stdio |
|--------|----------|-----------|
| **Connection** | Client makes HTTP requests to server URL | Client spawns server as subprocess |
| **Communication** | Request/response over TCP | JSON-RPC over stdin/stdout pipes |
| **Lifecycle** | Server runs independently | Server lifecycle tied to client |
| **Concurrency** | Multiple clients can connect | One client per server instance |
| **Discovery** | OpenAPI/Swagger specs | MCP protocol `listTools` method |
| **Authentication** | Headers/tokens | Process-level (client spawns server) |

## Integration with LangGraph Agent

When building the Python LangGraph agent (next step), it will:

1. **Spawn the MCP server** when the agent starts
2. **Use MCP tools as LangGraph tools** - each MCP tool becomes a LangGraph tool
3. **Keep the connection alive** during the agent's execution
4. **Clean up** when the agent finishes

Example integration pattern:
```python
from langchain.tools import Tool
import subprocess

class MCPToolWrapper:
    def __init__(self, mcp_server_path):
        self.server = subprocess.Popen([...])
        
    def call_tool(self, tool_name, **kwargs):
        # Send MCP request via stdin
        # Read MCP response from stdout
        return response
        
# Register as LangGraph tools
fetch_url_tool = Tool(
    name="fetch_url",
    func=lambda url, runId: mcp_wrapper.call_tool("fetch_url", url=url, runId=runId),
    description="Fetch a URL and store to evidence repository"
)
```

## Troubleshooting

### "Cannot connect to database"

Make sure PostgreSQL is running:
```bash
docker compose -f infra/docker/docker-compose.yml ps postgres
```

### "Cannot connect to MinIO"

Check MinIO is healthy:
```bash
docker compose -f infra/docker/docker-compose.yml ps minio
curl http://localhost:9000/minio/health/ready
```

### "Server not responding"

Check if the server process is running:
```bash
ps aux | grep "tsx src/index.ts"
```

Kill stuck processes:
```bash
pkill -f "tsx src/index.ts"
```

## Next Steps

1. **Test the example client**: `yarn example`
2. **Build the LangGraph agent** that uses these MCP tools
3. **Add more MCP tools**: `web_search`, `parse_pdf`, etc.
4. **Integrate with Temporal workflow** to orchestrate tool calls

---

For more details on the MCP protocol, see: https://modelcontextprotocol.io
