import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import { registerFetchUrl } from "./tools/fetch_url.js";
import { registerIngestText } from "./tools/ingest_text.js";
import {
  registerIngestGraphEntity,
  registerIngestGraphEntities,
  registerIngestGraphRelations,
} from "./tools/ingest_graph_entity.js";
import { registerReportQueryTools } from "./tools/report_query_tools.js";
import { registerPythonTools } from "./tools/python_tools.js";
import { logger } from "./utils/logger.js";
import http from "node:http";
import { randomUUID } from "node:crypto";

type SessionEntry = {
  server: McpServer;
  transport: StreamableHTTPServerTransport;
};

function createServer(): McpServer {
  const server = new McpServer(
    {
      name: "osint-mcp-server",
      version: "1.0.0",
    },
    {
      capabilities: {
        tools: {},
      },
    }
  );

  const toolset = (process.env.MCP_TOOLSET ?? "default").toLowerCase();
  const isKaliOsintOnly = toolset === "kali-osint";

  registerFetchUrl(server);
  if (!isKaliOsintOnly) {
    registerIngestText(server);
    registerIngestGraphEntity(server);
    registerIngestGraphEntities(server);
    registerIngestGraphRelations(server);
    registerReportQueryTools(server);
  }
  registerPythonTools(server);

  return server;
}

function getHeader(req: http.IncomingMessage, name: string): string | undefined {
  const value = req.headers[name.toLowerCase()];
  if (Array.isArray(value)) {
    return value[0];
  }
  return value;
}

function parseJsonBody(req: http.IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (chunk) => {
      data += chunk;
    });
    req.on("end", () => {
      if (!data) {
        resolve(null);
        return;
      }
      try {
        resolve(JSON.parse(data));
      } catch (error) {
        reject(error);
      }
    });
    req.on("error", reject);
  });
}

// Start server with Streamable HTTP transport
async function main() {
  const port = process.env.MCP_PORT ? Number(process.env.MCP_PORT) : 3001;
  const sessions = new Map<string, SessionEntry>();

  const httpServer = http.createServer(async (req, res) => {
    if (!req.url || !req.url.startsWith("/mcp")) {
      res.writeHead(404, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Not Found" }));
      return;
    }

    const sessionId = getHeader(req, "mcp-session-id");
    let entry = sessionId ? sessions.get(sessionId) : undefined;

    let parsedBody: unknown = undefined;
    if (req.method === "POST") {
      try {
        parsedBody = await parseJsonBody(req);
      } catch (error) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            jsonrpc: "2.0",
            error: {
              code: -32700,
              message: "Parse error: Invalid JSON",
            },
            id: null,
          })
        );
        return;
      }
    }

    if (!entry) {
      const serverInstance = createServer();
      const transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: () => randomUUID(),
        enableJsonResponse: true,
        onsessioninitialized: (newSessionId) => {
          sessions.set(newSessionId, { server: serverInstance, transport });
          logger.info("MCP session initialized", { sessionId: newSessionId });
        },
        onsessionclosed: (closedSessionId) => {
          sessions.delete(closedSessionId);
          logger.info("MCP session closed", { sessionId: closedSessionId });
        },
      });

      await serverInstance.connect(transport);
      entry = { server: serverInstance, transport };

      transport.onclose = () => {
        if (transport.sessionId) {
          sessions.delete(transport.sessionId);
        }
      };
    }

    if (entry.transport && req.method === "POST" && parsedBody !== undefined) {
      const isInitialize = Array.isArray(parsedBody)
        ? parsedBody.some((item) => isInitializeRequest(item))
        : isInitializeRequest(parsedBody);

      if (!isInitialize && !sessionId) {
        logger.warn("MCP request missing session id", { method: req.method });
      }
    }

    await entry.transport.handleRequest(req, res, parsedBody);

    if (!entry.transport.sessionId) {
      await entry.transport.close();
    }
  });

  httpServer.listen(port, "0.0.0.0", () => {
    logger.info("MCP server running", { transport: "streamable-http", port });
  });
}

main().catch((error) => {
  logger.error("Fatal error in main", { error: (error as Error).message });
  process.exit(1);
});
