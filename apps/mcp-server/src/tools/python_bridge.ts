import { spawn } from "node:child_process";
import { resolve } from "node:path";

export type PythonToolResult = {
  ok: boolean;
  result: unknown;
  error?: string;
  stdout: string;
  stderr: string;
};

type RunPythonToolParams = {
  pythonBin: string;
  scriptPath: string;
  toolName: string;
  input: Record<string, unknown>;
  timeoutMs?: number;
};

export async function runPythonTool({
  pythonBin,
  scriptPath,
  toolName,
  input,
  timeoutMs = 30000,
}: RunPythonToolParams): Promise<PythonToolResult> {
  const resolvedScript = resolve(scriptPath);
  const payload = JSON.stringify({ tool: toolName, input });

  const child = spawn(pythonBin, [resolvedScript], {
    stdio: ["pipe", "pipe", "pipe"],
    env: process.env,
  });

  let stdout = "";
  let stderr = "";

  child.stdout.on("data", (chunk) => {
    stdout += chunk.toString();
  });

  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  child.stdin.write(payload);
  child.stdin.end();

  const exitCode = await new Promise<number | null>((resolveExit) => {
    let timeoutHandle: NodeJS.Timeout | undefined;

    if (timeoutMs > 0) {
      timeoutHandle = setTimeout(() => {
        child.kill("SIGKILL");
      }, timeoutMs);
    }

    child.on("close", (code) => {
      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
      }
      resolveExit(code);
    });
  });

  if (exitCode !== 0) {
    return {
      ok: false,
      result: null,
      error: `Python exited with code ${exitCode}`,
      stdout: stdout.trim(),
      stderr: stderr.trim(),
    };
  }

  const trimmed = stdout.trim();
  if (!trimmed) {
    return {
      ok: false,
      result: null,
      error: "Python tool returned empty output",
      stdout: "",
      stderr: stderr.trim(),
    };
  }

  try {
    const parsed = JSON.parse(trimmed) as Record<string, unknown>;
    if (typeof parsed === "object" && parsed !== null && "ok" in parsed) {
      const ok = Boolean(parsed.ok);
      return {
        ok,
        result: parsed.result ?? null,
        error: ok ? undefined : (parsed.error as string | undefined) ?? "Python tool failed",
        stdout: trimmed,
        stderr: stderr.trim(),
      };
    }

    return {
      ok: true,
      result: parsed,
      stdout: trimmed,
      stderr: stderr.trim(),
    };
  } catch (error) {
    return {
      ok: false,
      result: null,
      error: `Failed to parse Python JSON output: ${(error as Error).message}`,
      stdout: trimmed,
      stderr: stderr.trim(),
    };
  }
}
