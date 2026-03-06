# OSINT MCP Server

MCP server for OSINT collection, ingestion, and evidence retrieval.

## Current Architecture

- Protocol: JSON-RPC 2.0
- Transport: **Streamable HTTP**
- Endpoint: `/mcp`
- SDK: `@modelcontextprotocol/sdk`

The server is session-based:

1. client initializes with `POST /mcp`
2. server returns an `mcp-session-id`
3. subsequent tool calls reuse that session id
4. client closes the session with `DELETE /mcp`

Two runtime modes are used in this repo:

- default server on port `3001`
- curated OSINT/Kali preset server on port `3002`

## Structure

```text
src/
  index.ts                    # HTTP MCP entrypoint
  config.ts                   # Env/config loading
  tools/
    fetch_url.ts
    ingest_text.ts
    ingest_graph_entity.ts
    report_query_tools.ts
    python_tools.ts
    tools_python/             # Python wrappers and research integrations
  clients/
    pg.ts
    minio.ts
scripts/
  test-fetch-url.ts
  test-ingest-text.ts
  test-ingest-graph.ts
  test-python-tools.ts
  test-research-python-tools-http.sh
```

## Implemented Tools

Core ingest tools:

- `fetch_url`
- `ingest_text`
- `ingest_graph_entity`
- `ingest_graph_entities`
- `ingest_graph_relations`

Retrieval/query tools:

- `vector_search`
- `vector_get_document`
- `graph_get_entity`
- `graph_neighbors`
- `graph_search_entities`

Python-backed research tools:

- default runtime exposes research-integration tools such as `person_search`, `x_get_user_posts_api`, `linkedin_download_html_ocr`, `google_serp_person_search`, and `arxiv_search_and_download`
- OSINT runtime exposes curated wrappers such as `osint_maigret_username`, `osint_amass_domain`, `osint_whatweb_target`, `osint_exiftool_extract`, and related preset tools
- normal web evidence collection should prefer Tavily-backed tools first (`tavily_research`, `tavily_person_search`, `extract_webpage`, `crawl_webpage`, `map_webpage`); treat `fetch_url` as a fallback utility rather than the default path

## Behavior

`fetch_url`:
- stores raw bytes in MinIO
- writes `documents` + `document_objects`
- logs `tool_calls`
- emits `TOOL_CALL_STARTED` / `TOOL_CALL_FINISHED`

`ingest_text`:
- chunks text
- generates embeddings through OpenRouter
- upserts vectors into Qdrant
- stores chunk rows in Postgres

Graph ingest tools:
- upsert entities/relations into Neo4j
- keep evidence references on nodes/edges
- normalize merge keys for URLs, emails, domains, names, and locations

Query tools:
- let the Stage 2 report graph retrieve evidence from Qdrant, Postgres, and Neo4j

## Environment

```bash
MCP_PORT=3001
DATABASE_URL=postgresql://osint:osint@postgres:5432/osint
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio12345
MINIO_BUCKET=osint-raw
QDRANT_URL=http://qdrant:6333
QDRANT_COLLECTION=osint_chunks
NEO4J_URI=bolt://neo4j:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=neo4jpassword
PYTHON_BIN=python3
MCP_PYTHON_TOOLS=[]
MCP_TOOLSET=default
```

See [env.example](/workspaces/llm-osint/apps/mcp-server/src/tools/tools_python/env.example) for optional provider credentials.

## Running

Development:

```bash
yarn workspace @osint/mcp-server dev
```

Docker:

```bash
docker compose -f infra/docker/docker-compose.yml up -d mcp-server
docker compose -f infra/docker/docker-compose.yml up -d mcp-server-kali
```

## Tests

```bash
RUN_ID=<optional-uuid> yarn tsx apps/mcp-server/scripts/test-fetch-url.ts
RUN_ID=<optional-uuid> yarn tsx apps/mcp-server/scripts/test-ingest-text.ts
RUN_ID=<optional-uuid> yarn tsx apps/mcp-server/scripts/test-ingest-graph.ts
RUN_ID=<optional-uuid> yarn tsx apps/mcp-server/scripts/test-python-tools.ts
```

`test-python-tools.ts` uses the HTTP MCP client transport and is the right shape for the current server.

## Current Status

Implemented and used by the repo today:

- HTTP MCP transport
- LangGraph HTTP client integration
- ingest/vector/graph/query tools
- Python bridge for research and curated OSINT tools

Still pending:

- generic provider-backed `web_search` tool abstraction
- stronger per-tool budgets, allowlists, and rate-limiting
