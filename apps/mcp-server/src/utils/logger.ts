import fs from "node:fs";
import path from "node:path";

const defaultLogPath = path.resolve(process.cwd(), "logs", "mcp-server.log");
const logPath = process.env.MCP_SERVER_LOG_FILE ? path.resolve(process.env.MCP_SERVER_LOG_FILE) : defaultLogPath;

function ensureLogDir() {
  try {
    fs.mkdirSync(path.dirname(logPath), { recursive: true });
  } catch {
    // best-effort
  }
}

function writeLine(line: string) {
  ensureLogDir();
  try {
    fs.appendFileSync(logPath, line + "\n", "utf-8");
  } catch {
    // best-effort
  }
}

function format(level: string, message: string, meta?: Record<string, unknown>) {
  const entry = {
    ts: new Date().toISOString(),
    level,
    message,
    ...(meta ? { meta } : {}),
  };
  return JSON.stringify(entry);
}

export const logger = {
  info(message: string, meta?: Record<string, unknown>) {
    const line = format("info", message, meta);
    console.log(line);
    writeLine(line);
  },
  warn(message: string, meta?: Record<string, unknown>) {
    const line = format("warn", message, meta);
    console.warn(line);
    writeLine(line);
  },
  error(message: string, meta?: Record<string, unknown>) {
    const line = format("error", message, meta);
    console.error(line);
    writeLine(line);
  },
};
