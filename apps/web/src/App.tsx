import { useEffect, useMemo, useState } from "react";
import {
  Button,
  Card,
  CardBody,
  CardHeader,
  Chip,
  ScrollShadow,
  Spinner,
  Tab,
  Tabs,
  Textarea
} from "@heroui/react";

type ViewMode = "chat" | "evidence";
type EvidenceView = "files" | "graph";

type RunSummary = {
  runId: string;
  title?: string | null;
  prompt: string;
  createdAt: string;
  status: string;
  reportStatus?: string | null;
};

type RunEvent = {
  event_id: string;
  run_id: string;
  type: string;
  ts: string;
  payload: Record<string, unknown>;
};

type RunFile = {
  documentId: string;
  sourceUrl: string | null;
  sourceType: string;
  retrievedAt: string;
  title: string | null;
  contentType: string | null;
  object: {
    objectId: string;
    kind: string;
    bucket: string;
    objectKey: string;
    sizeBytes: number | null;
    contentType: string | null;
  } | null;
};

type GraphNode = {
  id: string;
  labels: string[];
  display: string;
};

type GraphEdge = {
  id: string;
  source: string;
  target: string;
  type: string;
};

const RUNS_STORAGE_KEY = "osint-ui-runs";
const API_BASE = "/api";
const FINISHED_RUN_STATUS = new Set(["done", "failed", "cancelled"]);
const FINISHED_REPORT_STATUS = new Set(["ready", "failed"]);

function loadRuns(): RunSummary[] {
  try {
    const raw = localStorage.getItem(RUNS_STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw) as RunSummary[];
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function shortRunId(runId: string): string {
  return runId.split("-")[0] ?? runId;
}

function statusColor(status: string): "primary" | "warning" | "success" | "danger" {
  if (["done", "ready"].includes(status)) return "success";
  if (["failed", "error"].includes(status)) return "danger";
  if (["collecting", "extracting", "mining", "reporting", "draft"].includes(status)) return "warning";
  return "primary";
}

function deriveTitle(prompt: string): string {
  const normalized = prompt.trim().replace(/\s+/g, " ");
  if (!normalized) return "Untitled investigation";
  return normalized.length > 80 ? `${normalized.slice(0, 77)}...` : normalized;
}

function selectExistingRunId(prev: string | null, items: RunSummary[]): string | null {
  if (items.length === 0) return null;
  if (prev && items.some((item) => item.runId === prev)) return prev;
  return items[0]?.runId ?? null;
}

export default function App() {
  const [runs, setRuns] = useState<RunSummary[]>(() => loadRuns());
  const [selectedRunId, setSelectedRunId] = useState<string | null>(runs[0]?.runId ?? null);
  const [events, setEvents] = useState<RunEvent[]>([]);
  const [files, setFiles] = useState<RunFile[]>([]);
  const [graphNodes, setGraphNodes] = useState<GraphNode[]>([]);
  const [graphEdges, setGraphEdges] = useState<GraphEdge[]>([]);
  const [mode, setMode] = useState<ViewMode>("chat");
  const [evidenceView, setEvidenceView] = useState<EvidenceView>("files");
  const [prompt, setPrompt] = useState("");
  const [isStartingRun, setIsStartingRun] = useState(false);
  const [isLoadingRun, setIsLoadingRun] = useState(false);
  const [isLoadingFiles, setIsLoadingFiles] = useState(false);
  const [isLoadingGraph, setIsLoadingGraph] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  useEffect(() => {
    localStorage.setItem(RUNS_STORAGE_KEY, JSON.stringify(runs));
  }, [runs]);

  useEffect(() => {
    let disposed = false;

    const refreshRuns = async () => {
      try {
        const res = await fetch(`${API_BASE}/runs?limit=100&offset=0`);
        if (!res.ok) return;
        const payload = (await res.json()) as {
          items: Array<{
            runId: string;
            title?: string | null;
            prompt: string;
            createdAt: string;
            status: string;
            latestReport: { status: string } | null;
          }>;
        };

        if (disposed) return;
        const items: RunSummary[] = payload.items.map((item) => ({
          runId: item.runId,
          title: item.title ?? null,
          prompt: item.prompt,
          createdAt: item.createdAt,
          status: item.status,
          reportStatus: item.latestReport?.status ?? null
        }));

        setRuns(items);
        setSelectedRunId((prev) => selectExistingRunId(prev, items));
      } catch {
        // Keep local fallback from storage.
      }
    };

    void refreshRuns();
    return () => {
      disposed = true;
    };
  }, []);

  const selectedRun = useMemo(
    () => runs.find((run) => run.runId === selectedRunId) ?? null,
    [runs, selectedRunId]
  );

  const runIsFinished = useMemo(() => {
    if (!selectedRun) return false;
    const runDone = FINISHED_RUN_STATUS.has(selectedRun.status);
    const reportDone = selectedRun.reportStatus ? FINISHED_REPORT_STATUS.has(selectedRun.reportStatus) : false;
    return runDone || reportDone;
  }, [selectedRun]);

  useEffect(() => {
    if (!selectedRunId) return;

    setMode("chat");
    setEvidenceView("files");
    setEvents([]);
    setFiles([]);
    setGraphNodes([]);
    setGraphEdges([]);
    setErrorMessage(null);
    setIsLoadingRun(true);

    let isDisposed = false;
    let polling: number | undefined;

    const loadDetails = async () => {
      try {
        const detailsRes = await fetch(`${API_BASE}/runs/${selectedRunId}`);
        if (detailsRes.status === 404) {
          if (!isDisposed) {
            setRuns((prev) => {
              const next = prev.filter((run) => run.runId !== selectedRunId);
              setSelectedRunId((curr) => (curr === selectedRunId ? (next[0]?.runId ?? null) : curr));
              return next;
            });
            setErrorMessage("Selected run was not found. Switched to the latest available run.");
          }
          return;
        }
        if (!detailsRes.ok) throw new Error(`Failed to load run ${selectedRunId}`);
        const details = (await detailsRes.json()) as {
          run: { title?: string | null; status: string; prompt: string; createdAt: string };
          latestReport: { status: string } | null;
        };

        if (!isDisposed) {
          setRuns((prev) =>
            prev.map((r) =>
              r.runId === selectedRunId
                ? {
                    ...r,
                    title: details.run.title ?? r.title ?? null,
                    prompt: details.run.prompt,
                    createdAt: details.run.createdAt,
                    status: details.run.status,
                    reportStatus: details.latestReport?.status ?? null
                  }
                : r
            )
          );
        }
      } catch (error) {
        if (!isDisposed) {
          setErrorMessage(error instanceof Error ? error.message : "Unknown API error");
        }
      } finally {
        if (!isDisposed) setIsLoadingRun(false);
      }
    };

    void loadDetails();

    const source = new EventSource(`${API_BASE}/runs/${selectedRunId}/events`);
    source.addEventListener("run_event", (msg) => {
      try {
        const data = JSON.parse(msg.data) as RunEvent;
        if (!isDisposed) {
          setEvents((prev) => {
            const exists = prev.some((e) => e.event_id === data.event_id);
            return exists ? prev : [...prev, data];
          });
        }
      } catch {
        // Ignore malformed stream payloads.
      }
    });

    source.onerror = () => {
      if (!isDisposed) {
        setErrorMessage("Event stream disconnected. Retrying automatically.");
      }
    };

    polling = window.setInterval(() => {
      void loadDetails();
    }, 5000);

    return () => {
      isDisposed = true;
      if (polling) window.clearInterval(polling);
      source.close();
    };
  }, [selectedRunId]);

  useEffect(() => {
    if (!selectedRunId || !runIsFinished) return;

    let disposed = false;

    const loadFiles = async () => {
      setIsLoadingFiles(true);
      try {
        const res = await fetch(`${API_BASE}/runs/${selectedRunId}/files?limit=200&offset=0`);
        if (!res.ok) throw new Error("Failed to load files");
        const payload = (await res.json()) as { items: RunFile[] };
        if (!disposed) setFiles(payload.items ?? []);
      } catch (error) {
        if (!disposed) setErrorMessage(error instanceof Error ? error.message : "Failed to load files");
      } finally {
        if (!disposed) setIsLoadingFiles(false);
      }
    };

    const loadGraph = async () => {
      setIsLoadingGraph(true);
      try {
        const res = await fetch(
          `${API_BASE}/runs/${selectedRunId}/graph?nodeLimit=200&nodeOffset=0&edgeLimit=300&edgeOffset=0`
        );
        if (!res.ok) throw new Error("Failed to load graph");
        const payload = (await res.json()) as { nodes: GraphNode[]; edges: GraphEdge[] };
        if (!disposed) {
          setGraphNodes(payload.nodes ?? []);
          setGraphEdges(payload.edges ?? []);
        }
      } catch (error) {
        if (!disposed) setErrorMessage(error instanceof Error ? error.message : "Failed to load graph");
      } finally {
        if (!disposed) setIsLoadingGraph(false);
      }
    };

    void loadFiles();
    void loadGraph();

    return () => {
      disposed = true;
    };
  }, [selectedRunId, runIsFinished]);

  const startRun = async () => {
    if (!prompt.trim()) return;

    setIsStartingRun(true);
    setErrorMessage(null);

    try {
      const res = await fetch(`${API_BASE}/runs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ prompt: prompt.trim() })
      });

      if (!res.ok) throw new Error(`Failed to create run (${res.status})`);
      const data = (await res.json()) as { runId: string };

      const newRun: RunSummary = {
        runId: data.runId,
        title: deriveTitle(prompt),
        prompt: prompt.trim(),
        createdAt: new Date().toISOString(),
        status: "created",
        reportStatus: null
      };

      setRuns((prev) => [newRun, ...prev.filter((run) => run.runId !== newRun.runId)]);
      setSelectedRunId(newRun.runId);
      setPrompt("");
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : "Failed to start run");
    } finally {
      setIsStartingRun(false);
    }
  };

  return (
    <main className="grid-bg h-screen overflow-hidden bg-background text-foreground">
      <div className="h-full bg-gradient-to-br from-slate-950 via-slate-900 to-cyan-950/40">
        <div className="mx-auto flex h-full w-full max-w-[1400px] gap-4 p-4">
          <aside className="hidden h-full min-h-0 w-80 flex-col overflow-hidden rounded-2xl border border-white/10 bg-slate-950/70 p-4 shadow-2xl backdrop-blur lg:flex">
            <div className="mb-4">
              <p className="text-xs uppercase tracking-[0.2em] text-cyan-300/80">Pipeline Runs</p>
              <h1 className="mt-1 text-2xl font-semibold text-cyan-50">Control Room</h1>
            </div>

            <ScrollShadow className="themed-scroll h-full min-h-0 flex-1 pr-2">
              <div className="space-y-2">
                {runs.length === 0 ? (
                  <p className="text-sm text-cyan-100/80">No runs yet. Start one from the prompt composer.</p>
                ) : (
                  runs.map((run) => (
                    <button
                      key={run.runId}
                      type="button"
                      onClick={() => setSelectedRunId(run.runId)}
                      className={`w-full rounded-xl border p-3 text-left transition ${
                        run.runId === selectedRunId
                          ? "border-cyan-400 bg-cyan-500/10"
                          : "border-white/10 bg-slate-900/40 hover:border-white/20"
                      }`}
                    >
                      <div className="flex items-center justify-between gap-2">
                        <p className="font-mono text-sm text-cyan-50">#{shortRunId(run.runId)}</p>
                        <Chip
                          color={statusColor(run.reportStatus ?? run.status)}
                          size="sm"
                          variant="flat"
                          classNames={{ content: "font-medium text-slate-50" }}
                        >
                          {run.reportStatus ?? run.status}
                        </Chip>
                      </div>
                      <p className="mt-2 line-clamp-2 text-sm text-cyan-100">{run.title || deriveTitle(run.prompt)}</p>
                      <p className="mt-1 line-clamp-2 text-xs text-cyan-100/75">{run.prompt}</p>
                    </button>
                  ))
                )}
              </div>
            </ScrollShadow>
          </aside>

          <section className="flex h-full min-h-0 flex-1 flex-col rounded-2xl border border-white/10 bg-slate-950/60 p-4 shadow-2xl backdrop-blur">
            <div className="mb-4 flex items-center justify-between gap-3">
              <div>
                <p className="text-xs uppercase tracking-[0.2em] text-cyan-300/80">Current Session</p>
                <p className="mt-1 text-sm text-cyan-100">{selectedRun?.title || selectedRun?.prompt || "No run selected"}</p>
                <p className="mt-1 font-mono text-xs text-default-600">
                  {selectedRun ? `Run #${shortRunId(selectedRun.runId)}` : "No run selected"}
                </p>
              </div>
              {isLoadingRun ? <Spinner size="sm" color="secondary" /> : null}
            </div>

            <Tabs
              selectedKey={mode}
              onSelectionChange={(key) => setMode(String(key) as ViewMode)}
              color="primary"
              radius="full"
              variant="bordered"
            >
              <Tab key="chat" title="Chat Mode" />
              <Tab key="evidence" title="Evidence" isDisabled={!runIsFinished} />
            </Tabs>

            {!runIsFinished ? (
              <p className="mt-2 text-xs text-default-500">
                File list and graph views unlock when the selected run reaches `done` / `failed` or the report is `ready`.
              </p>
            ) : null}

            <div className="mt-4 min-h-0 flex-1 overflow-hidden">
              {mode === "chat" && (
                <Card className="h-full border border-white/10 bg-slate-950/40" shadow="none">
                  <CardBody className="h-full">
                    <ScrollShadow className="themed-scroll h-full pr-2">
                      <div className="space-y-3">
                        {errorMessage ? (
                          <Card className="border border-danger/30 bg-danger/10" shadow="none">
                            <CardBody className="py-3 text-sm text-danger">{errorMessage}</CardBody>
                          </Card>
                        ) : null}

                        {events.length === 0 ? (
                          <p className="text-sm text-default-500">Run events will stream here in real time.</p>
                        ) : (
                          events.map((event) => (
                            <Card key={event.event_id} className="border border-white/10 bg-slate-900/50" shadow="none">
                              <CardHeader className="flex items-center justify-between pb-0">
                                <p className="font-mono text-xs text-cyan-200">{event.type}</p>
                                <p className="text-xs text-default-500">{new Date(event.ts).toLocaleString()}</p>
                              </CardHeader>
                              <CardBody>
                                <pre className="overflow-x-auto whitespace-pre-wrap break-words text-xs text-default-600">
                                  {JSON.stringify(event.payload, null, 2)}
                                </pre>
                              </CardBody>
                            </Card>
                          ))
                        )}
                      </div>
                    </ScrollShadow>
                  </CardBody>
                </Card>
              )}

              {mode === "evidence" && (
                <Card className="h-full border border-white/10 bg-slate-950/40" shadow="none">
                  <CardHeader className="pb-2">
                    <div className="inline-flex rounded-lg border border-white/15 bg-slate-900/70 p-1">
                      <button
                        type="button"
                        onClick={() => setEvidenceView("files")}
                        className={`rounded-md px-3 py-1.5 text-xs font-medium transition ${
                          evidenceView === "files"
                            ? "bg-cyan-500/20 text-cyan-100 shadow-[inset_0_0_0_1px_rgba(103,232,249,0.45)]"
                            : "text-default-500 hover:text-cyan-100"
                        }`}
                      >
                        File List
                      </button>
                      <button
                        type="button"
                        onClick={() => setEvidenceView("graph")}
                        className={`rounded-md px-3 py-1.5 text-xs font-medium transition ${
                          evidenceView === "graph"
                            ? "bg-cyan-500/20 text-cyan-100 shadow-[inset_0_0_0_1px_rgba(103,232,249,0.45)]"
                            : "text-default-500 hover:text-cyan-100"
                        }`}
                      >
                        Graph DB
                      </button>
                    </div>
                  </CardHeader>
                  <CardBody className="h-full pt-0">
                    {evidenceView === "files" ? (
                      <ScrollShadow className="themed-scroll h-full pr-2">
                        <div className="space-y-3">
                          {isLoadingFiles ? <Spinner size="sm" color="secondary" /> : null}
                          {files.length === 0 ? (
                            <p className="text-sm text-default-500">No file artifacts found for this run.</p>
                          ) : (
                            files.map((file) => (
                              <Card
                                key={`${file.documentId}-${file.object?.objectId ?? "none"}`}
                                className="border border-white/10 bg-slate-900/50"
                                shadow="none"
                              >
                                <CardBody className="text-sm">
                                  <p className="font-medium text-cyan-100">{file.title || `Document ${shortRunId(file.documentId)}`}</p>
                                  <p className="mt-1 font-mono text-xs text-default-500">
                                    {file.object ? `${file.object.bucket}/${file.object.objectKey}` : "No object pointer"}
                                  </p>
                                  <p className="mt-2 text-default-600">Source: {file.sourceUrl ?? "-"}</p>
                                  <div className="mt-2 flex gap-2">
                                    <Chip size="sm" variant="dot" color="default">
                                      {file.sourceType}
                                    </Chip>
                                    <Chip size="sm" variant="flat" color="default">
                                      {file.object?.contentType ?? file.contentType ?? "unknown"}
                                    </Chip>
                                  </div>
                                </CardBody>
                              </Card>
                            ))
                          )}
                        </div>
                      </ScrollShadow>
                    ) : (
                      <>
                        {isLoadingGraph ? <Spinner size="sm" color="secondary" /> : null}
                        {graphNodes.length < 2 ? (
                          <p className="text-sm text-default-500">No graph nodes with this run's evidence pointers yet.</p>
                        ) : (
                          <svg viewBox="0 0 820 440" className="h-full w-full">
                            {graphEdges.map((edge, index) => {
                              const sourceIndex = graphNodes.findIndex((node) => node.id === edge.source);
                              const targetIndex = graphNodes.findIndex((node) => node.id === edge.target);
                              const x1 = 80 + Math.max(sourceIndex, 0) * 70;
                              const y1 = 90 + ((Math.max(sourceIndex, 0) % 2) * 180);
                              const x2 = 80 + Math.max(targetIndex, 0) * 70;
                              const y2 = 90 + ((Math.max(targetIndex, 0) % 2) * 180);

                              return (
                                <g key={edge.id ?? `${edge.source}-${edge.target}-${index}`}>
                                  <line x1={x1} y1={y1} x2={x2} y2={y2} stroke="rgba(34,211,238,0.45)" strokeWidth="2" />
                                  <text x={(x1 + x2) / 2} y={(y1 + y2) / 2 - 6} fill="#94a3b8" fontSize="10">
                                    {edge.type}
                                  </text>
                                </g>
                              );
                            })}

                            {graphNodes.map((node, index) => {
                              const x = 80 + index * 70;
                              const y = 90 + ((index % 2) * 180);
                              return (
                                <g key={node.id}>
                                  <circle cx={x} cy={y} r="24" fill="rgba(6,182,212,0.3)" stroke="rgba(34,211,238,0.9)" />
                                  <text x={x} y={y + 4} fill="#e2e8f0" fontSize="10" textAnchor="middle">
                                    {node.display.slice(0, 12)}
                                  </text>
                                </g>
                              );
                            })}
                          </svg>
                        )}
                      </>
                    )}
                  </CardBody>
                </Card>
              )}
            </div>

            <div className="prompt-composer mt-4 rounded-2xl border border-cyan-400/20 bg-gradient-to-br from-slate-900/90 via-slate-900/75 to-cyan-950/35 p-3 shadow-[0_0_0_1px_rgba(103,232,249,0.08),0_18px_30px_rgba(2,6,23,0.45)]">
              <Textarea
                value={prompt}
                onValueChange={setPrompt}
                label="New Investigation"
                labelPlacement="outside"
                placeholder="Investigate an entity and trace OSINT evidence..."
                disableAutosize
                rows={4}
                variant="bordered"
                className="prompt-textarea"
                classNames={{
                  label: "mb-2 block text-sm font-semibold text-cyan-200",
                  inputWrapper:
                    "rounded-xl border border-cyan-300/35 bg-slate-950 shadow-none outline-none ring-0 data-[focus=true]:border-cyan-200/70 data-[focus=true]:shadow-[0_0_0_2px_rgba(34,211,238,0.18)] data-[focus-visible=true]:shadow-[0_0_0_2px_rgba(34,211,238,0.18)]",
                  innerWrapper: "bg-transparent",
                  input:
                    "resize-none overflow-y-auto bg-transparent text-sm text-cyan-50 placeholder:text-cyan-200/45 outline-none ring-0 focus:outline-none focus:ring-0"
                }}
              />
              <div className="mt-3 flex justify-end">
                <Button
                  isLoading={isStartingRun}
                  onPress={startRun}
                  className="rounded-xl border border-cyan-300/35 bg-gradient-to-r from-cyan-500/20 via-slate-900/60 to-cyan-600/20 px-6 font-medium text-cyan-100 shadow-[inset_0_1px_0_rgba(255,255,255,0.08),0_10px_22px_rgba(8,145,178,0.24)] hover:border-cyan-200/55 hover:text-cyan-50"
                >
                  Start Run
                </Button>
              </div>
            </div>
          </section>
        </div>
      </div>
    </main>
  );
}
