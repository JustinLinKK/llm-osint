import { useEffect, useMemo, useRef, useState } from "react";
import {
  Button,
  Card,
  CardBody,
  CardHeader,
  Chip,
  ScrollShadow,
  Spinner,
  Textarea
} from "@heroui/react";
import type { Core, ElementDefinition, EventObject } from "cytoscape";
import CytoscapeComponent from "react-cytoscapejs";
import { jsPDF } from "jspdf";

type ViewMode = "chat" | "report" | "evidence";
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

type EventChipColor = "default" | "primary" | "secondary" | "success" | "warning" | "danger";

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
  properties?: Record<string, unknown>;
};

const GRAPH_PALETTE = [
  "#1f77b4",
  "#ff7f0e",
  "#2ca02c",
  "#d62728",
  "#9467bd",
  "#8c564b",
  "#e377c2",
  "#7f7f7f",
  "#bcbd22",
  "#17becf",
  "#4c78a8",
  "#f58518"
] as const;

type GraphEdge = {
  id: string;
  source: string;
  target: string;
  type: string;
  display?: string;
  properties?: Record<string, unknown>;
};

type GraphPayload = {
  nodes: GraphNode[];
  edges: GraphEdge[];
  graphRoot?: {
    nodeId: string;
    display?: string | null;
    recommendedLayout?: string;
    recommendedEgoDepth?: number;
  } | null;
};

type GraphGroupName =
  | "Contacts"
  | "Education"
  | "Work"
  | "Languages"
  | "Organizations"
  | "People"
  | "Research"
  | "Technical"
  | "Timeline"
  | "Documents"
  | "Risk"
  | "Other";

const GRAPH_GROUP_ORDER: GraphGroupName[] = [
  "Contacts",
  "Education",
  "Work",
  "Languages",
  "Organizations",
  "People",
  "Research",
  "Technical",
  "Timeline",
  "Documents",
  "Risk",
  "Other"
];

type ReportPayload = {
  reportId: string;
  runId: string;
  status: string;
  createdAt: string;
  markdown: string | null;
  json: {
    reportType?: string;
    qualityOk?: boolean;
    refineRound?: number;
    finalReport?: string;
    evidenceAppendix?: string;
    sectionDrafts?: Array<{
      sectionId: string;
      sectionOrder: number;
      title: string;
      content: string;
      citationKeys: string[];
      createdAt: string;
    }>;
    claimLedger?: Array<{
      claimId: string;
      sectionId: string;
      text: string;
      confidence: number;
      impact: string;
      evidenceKeys: string[];
      conflictFlags: string[];
      createdAt: string;
    }>;
    evidenceRefs?: Array<{
      citationKey: string;
      sectionId: string;
      documentId: string | null;
      snippet: string;
      sourceUrl: string | null;
      score: number | null;
      objectRef: Record<string, unknown>;
      createdAt: string;
    }>;
  } | null;
  citations?: Array<{
    citationKey: string;
    sectionId: string;
    sourceUrl: string | null;
    documentId: string | null;
  }> | null;
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

function classifyEventStage(eventType: string): { label: string; color: EventChipColor } {
  const normalized = eventType.toUpperCase();

  const customLabels: Record<string, { label: string; color: EventChipColor }> = {
    PLANNER_STARTED: { label: "Planning", color: "warning" },
    TOOLS_SELECTED: { label: "Tools Selected", color: "warning" },
    TOOL_WORKER_STARTED: { label: "Tool Running", color: "warning" },
    TOOL_RECEIPT_READY: { label: "Tool Complete", color: "warning" },
    SYNTHESIS_STARTED: { label: "Stage 2: Synthesis", color: "secondary" },
    REPORT_READY: { label: "Stage 2: Ready", color: "secondary" },
    RUN_STARTED: { label: "Run Started", color: "primary" },
    RUN_FINISHED: { label: "Run Finished", color: "success" },
    RUN_FAILED: { label: "Run Failed", color: "danger" }
  };

  const exact = customLabels[normalized];
  if (exact) return exact;

  if (normalized.includes("FAIL") || normalized.includes("ERROR")) {
    return { label: "Error", color: "danger" };
  }

  if (
    normalized.includes("SYNTHESIS") ||
    normalized.includes("REPORT") ||
    normalized.includes("STAGE2") ||
    normalized.includes("STAGE_2")
  ) {
    return { label: "Stage 2", color: "secondary" };
  }

  if (
    normalized.includes("PLANNER") ||
    normalized.includes("TOOL") ||
    normalized.includes("PROCESS") ||
    normalized.includes("CHUNK") ||
    normalized.includes("EMBEDD") ||
    normalized.includes("GRAPH")
  ) {
    return { label: "Stage 1", color: "warning" };
  }

  if (normalized.startsWith("RUN_")) {
    return { label: "Run", color: "primary" };
  }

  return { label: "Event", color: "default" };
}

function buildReportFallback(report: ReportPayload | null): string {
  const sectionDrafts = report?.json?.sectionDrafts ?? [];
  if (!sectionDrafts.length) return "";

  return sectionDrafts
    .map((section) => {
      const title = section.title?.trim() || section.sectionId;
      const content = section.content?.trim() || "No content yet.";
      return `${title}\n\n${content}`;
    })
    .join("\n\n---\n\n");
}

function slugifyFilename(value: string, fallback: string): string {
  const normalized = value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return normalized || fallback;
}

function triggerBlobDownload(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

function graphNodeType(node: GraphNode): string {
  const rawType = node.properties?.type;
  if (typeof rawType === "string" && rawType.trim()) return rawType.trim();
  return node.labels[0] || "Entity";
}

function graphNodeAttributes(node: GraphNode): string[] {
  const raw = node.properties?.attributes;
  if (!Array.isArray(raw)) return [];
  return raw.map((value) => String(value ?? "").trim()).filter(Boolean);
}

function stableGraphColor(label: string): string {
  const key = label.trim().toLowerCase();
  if (!key) return "#9ca3af";
  let hash = 0;
  for (let index = 0; index < key.length; index += 1) {
    hash = (hash * 31 + key.charCodeAt(index)) >>> 0;
  }
  return GRAPH_PALETTE[hash % GRAPH_PALETTE.length] ?? "#9ca3af";
}

function graphRelType(edge: GraphEdge): string {
  return edge.type?.trim() || edge.display?.trim() || "(unknown)";
}

function uniqueSorted(values: Iterable<string>): string[] {
  return Array.from(new Set(Array.from(values).map((value) => value.trim()).filter(Boolean))).sort((a, b) =>
    a.localeCompare(b)
  );
}

function classifyGraphGroup(node: GraphNode, rootNodeId: string): GraphGroupName {
  if (node.id === rootNodeId) return "Other";
  const type = graphNodeType(node);
  const blob = [node.display, ...graphNodeAttributes(node)].join(" | ").toLowerCase();

  if (["ContactPoint", "Email", "Phone", "Handle", "Website", "Domain", "ImageObject", "Location"].includes(type)) {
    return "Contacts";
  }
  if (type === "EducationalCredential") return "Education";
  if (["Experience", "Role", "Occupation"].includes(type)) return "Work";
  if (type === "Language") return "Languages";
  if (type === "Affiliation") {
    return /university|college|school|degree|student|phd|education|credential/.test(blob) ? "Education" : "Organizations";
  }
  if (["Organization", "Institution", "OrganizationProfile"].includes(type)) {
    if (/university|college|school|degree|student|phd|education|credential/.test(blob)) return "Education";
    return "Organizations";
  }
  if (type === "Person") return "People";
  if (["Publication", "Conference", "Award", "Grant", "Patent"].includes(type)) return "Research";
  if (["Repository", "Project", "Topic"].includes(type)) return "Technical";
  if (type === "TimelineEvent") return "Timeline";
  if (type === "Document") return "Documents";
  if (/risk|lawsuit|court|conflict|uncertain|sanction/.test(blob)) return "Risk";
  return "Other";
}

function groupedPresetPositions(
  rootNodeId: string,
  visibleNodeIds: string[],
  groupMembers: Map<GraphGroupName, string[]>
): Map<string, { x: number; y: number }> {
  const positions = new Map<string, { x: number; y: number }>();
  positions.set(rootNodeId, { x: 0, y: 0 });

  const activeGroups = GRAPH_GROUP_ORDER.filter((group) => (groupMembers.get(group)?.length ?? 0) > 0);
  if (!activeGroups.length) return positions;

  const groupRadius = Math.max(230, 170 + activeGroups.length * 22);
  const memberBaseRadius = groupRadius + 170;
  const sectorSpan = (2 * Math.PI) / Math.max(activeGroups.length, 1);

  for (const [groupIndex, group] of activeGroups.entries()) {
    const groupNodeId = `__group__:${group}`;
    const centerAngle = -Math.PI / 2 + sectorSpan * groupIndex;
    positions.set(groupNodeId, {
      x: Math.cos(centerAngle) * groupRadius,
      y: Math.sin(centerAngle) * groupRadius
    });

    const members = (groupMembers.get(group) ?? []).filter((nodeId) => visibleNodeIds.includes(nodeId));
    const sectorWidth = Math.min(sectorSpan * 0.76, Math.PI / 2.6);
    const perRing = 6;
    for (const [memberIndex, nodeId] of members.entries()) {
      const ring = Math.floor(memberIndex / perRing);
      const indexInRing = memberIndex % perRing;
      const remaining = members.length - ring * perRing;
      const countInRing = Math.min(perRing, remaining);
      const ratio = countInRing <= 1 ? 0.5 : indexInRing / (countInRing - 1);
      const angle = centerAngle - sectorWidth / 2 + sectorWidth * ratio;
      const radius = memberBaseRadius + ring * 120;
      positions.set(nodeId, {
        x: Math.cos(angle) * radius,
        y: Math.sin(angle) * radius
      });
    }
  }

  return positions;
}

function buildAdjacency(edges: GraphEdge[]): Map<string, Set<string>> {
  const adjacency = new Map<string, Set<string>>();
  for (const edge of edges) {
    if (!edge.source || !edge.target) continue;
    const sourceLinks = adjacency.get(edge.source) ?? new Set<string>();
    sourceLinks.add(edge.target);
    adjacency.set(edge.source, sourceLinks);
    const targetLinks = adjacency.get(edge.target) ?? new Set<string>();
    targetLinks.add(edge.source);
    adjacency.set(edge.target, targetLinks);
  }
  return adjacency;
}

function computeDegrees(edges: GraphEdge[]): Map<string, number> {
  const degrees = new Map<string, number>();
  for (const edge of edges) {
    if (edge.source) degrees.set(edge.source, (degrees.get(edge.source) ?? 0) + 1);
    if (edge.target) degrees.set(edge.target, (degrees.get(edge.target) ?? 0) + 1);
  }
  return degrees;
}

function bfsNeighborhood(adjacency: Map<string, Set<string>>, root: string, depth: number): Set<string> {
  if (!root || depth < 0) return new Set<string>();
  const visited = new Set<string>([root]);
  const queue: Array<{ nodeId: string; depth: number }> = [{ nodeId: root, depth: 0 }];
  while (queue.length) {
    const current = queue.shift();
    if (!current || current.depth >= depth) continue;
    for (const next of adjacency.get(current.nodeId) ?? []) {
      if (visited.has(next)) continue;
      visited.add(next);
      queue.push({ nodeId: next, depth: current.depth + 1 });
    }
  }
  return visited;
}

function makeGraphStylesheet(showEdgeLabels: boolean, maxDegree: number): Array<Record<string, unknown>> {
  const boundedDegree = Math.max(maxDegree, 1);
  const stylesheet: Array<Record<string, unknown>> = [
    {
      selector: "node",
      style: {
        label: "data(label)",
        "font-size": 10,
        "text-wrap": "wrap",
        "text-max-width": 140,
        "background-color": "data(color)",
        color: "#111827",
        "border-width": 1.2,
        "border-color": "#1f2937",
        width: `mapData(degree, 0, ${boundedDegree}, 14, 54)`,
        height: `mapData(degree, 0, ${boundedDegree}, 14, 54)`
      }
    },
    {
      selector: "node[isRoot = 1]",
      style: {
        "border-width": 3,
        "border-color": "#0f172a",
        "font-size": 12,
        "font-weight": 700,
        width: 72,
        height: 72,
        "z-index": 2
      }
    },
    {
      selector: "node[isGroup = 1]",
      style: {
        shape: "round-rectangle",
        "background-opacity": 0.22,
        "border-width": 2.2,
        "border-color": "#334155",
        "font-size": 13,
        "font-weight": 700,
        width: 110,
        height: 42,
        color: "#0f172a",
        "text-valign": "center",
        "text-halign": "center",
        "z-index": 1
      }
    },
    {
      selector: "edge",
      style: {
        "curve-style": "bezier",
        "line-color": "#9ca3af",
        "target-arrow-color": "#9ca3af",
        "target-arrow-shape": "triangle",
        "arrow-scale": 0.8,
        width: 1.4
      }
    },
    {
      selector: "edge[isGroupEdge = 1]",
      style: {
        "line-style": "dashed",
        "target-arrow-shape": "none",
        "line-color": "#64748b",
        width: 2,
        opacity: 0.6
      }
    },
    {
      selector: "edge[isRootSpoke = 1]",
      style: {
        opacity: 0.38,
        width: 1
      }
    }
  ];

  if (showEdgeLabels) {
    stylesheet.push({
      selector: "edge",
      style: {
        label: "data(label)",
        "font-size": 8,
        "text-wrap": "wrap",
        "text-max-width": 130,
        "text-background-opacity": 0.8,
        "text-background-color": "#ffffff",
        "text-background-padding": 2
      }
    });
  }

  return stylesheet;
}

function formatSelectionPayload(payload: Record<string, unknown>): string {
  return JSON.stringify(payload, null, 2);
}

function buildReportMarkdown(report: ReportPayload | null, fallbackContent: string): string {
  const markdown = report?.markdown?.trim();
  if (markdown) return markdown;
  const finalReport = report?.json?.finalReport?.trim();
  const appendix = report?.json?.evidenceAppendix?.trim();
  const parts = [finalReport, appendix].filter((value): value is string => Boolean(value));
  if (parts.length) return parts.join("\n\n");
  return fallbackContent.trim();
}

export default function App() {
  const cytoscapeRef = useRef<Core | null>(null);
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
  const [isLoadingReport, setIsLoadingReport] = useState(false);
  const [isDownloadingPdf, setIsDownloadingPdf] = useState(false);
  const [isDownloadingGraph, setIsDownloadingGraph] = useState(false);
  const [deletingRunId, setDeletingRunId] = useState<string | null>(null);
  const [report, setReport] = useState<ReportPayload | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [graphSearchText, setGraphSearchText] = useState("");
  const [selectedGraphNodeTypes, setSelectedGraphNodeTypes] = useState<string[]>([]);
  const [selectedGraphRelTypes, setSelectedGraphRelTypes] = useState<string[]>([]);
  const [graphMinDegree, setGraphMinDegree] = useState(0);
  const [graphNodeLimit, setGraphNodeLimit] = useState(60);
  const [graphEgoNode, setGraphEgoNode] = useState("");
  const [graphEgoDepth, setGraphEgoDepth] = useState(1);
  const [graphLayoutName, setGraphLayoutName] = useState("cose");
  const [showGraphEdgeLabels, setShowGraphEdgeLabels] = useState(false);
  const [graphSelectionText, setGraphSelectionText] = useState("Click a node or edge to inspect details.");

  const graphNodeMap = useMemo(
    () => new Map(graphNodes.map((node) => [node.id, node])),
    [graphNodes]
  );

  const sanitizedGraphEdges = useMemo(
    () => graphEdges.filter((edge) => graphNodeMap.has(edge.source) && graphNodeMap.has(edge.target)),
    [graphEdges, graphNodeMap]
  );
  const graphAdjacency = useMemo(() => buildAdjacency(sanitizedGraphEdges), [sanitizedGraphEdges]);
  const graphDegreeByNode = useMemo(() => computeDegrees(sanitizedGraphEdges), [sanitizedGraphEdges]);
  const graphMaxDegree = useMemo(
    () => Math.max(0, ...Array.from(graphDegreeByNode.values())),
    [graphDegreeByNode]
  );
  const graphNodeTypeValues = useMemo(
    () => uniqueSorted(graphNodes.map((node) => graphNodeType(node))),
    [graphNodes]
  );
  const graphRelTypeValues = useMemo(
    () => uniqueSorted(sanitizedGraphEdges.map((edge) => graphRelType(edge))),
    [sanitizedGraphEdges]
  );
  const graphNodeOptions = useMemo(
    () =>
      Array.from(graphNodeMap.values())
        .sort((left, right) => left.display.localeCompare(right.display))
        .map((node) => ({ id: node.id, label: `${node.display} (${node.id})` })),
    [graphNodeMap]
  );
  const graphLimitMin = graphNodes.length >= 10 ? 10 : 1;
  const graphLimitMax = Math.max(graphLimitMin, graphNodes.length || 1);

  useEffect(() => {
    localStorage.setItem(RUNS_STORAGE_KEY, JSON.stringify(runs));
  }, [runs]);

  useEffect(() => {
    setGraphSearchText("");
    setSelectedGraphNodeTypes([]);
    setSelectedGraphRelTypes([]);
    setGraphMinDegree(0);
    setGraphNodeLimit(60);
    setGraphEgoNode("");
    setGraphEgoDepth(1);
    setGraphLayoutName("cose");
    setShowGraphEdgeLabels(false);
    setGraphSelectionText("Click a node or edge to inspect details.");
  }, [selectedRunId]);

  useEffect(() => {
    setGraphNodeLimit((current) => {
      const nextDefault = Math.min(80, graphLimitMax);
      if (current < graphLimitMin) return nextDefault;
      if (current > graphLimitMax) return graphLimitMax;
      return current;
    });
  }, [graphLimitMax, graphLimitMin]);

  const filteredGraph = useMemo(() => {
    const query = graphSearchText.trim().toLowerCase();
    let candidateNodes = new Set(Array.from(graphNodeMap.keys()));

    if (selectedGraphNodeTypes.length) {
      const allowedTypes = new Set(selectedGraphNodeTypes);
      candidateNodes = new Set(
        Array.from(candidateNodes).filter((nodeId) => {
          const node = graphNodeMap.get(nodeId);
          return node ? allowedTypes.has(graphNodeType(node)) : false;
        })
      );
    }

    if (graphMinDegree > 0) {
      candidateNodes = new Set(
        Array.from(candidateNodes).filter((nodeId) => (graphDegreeByNode.get(nodeId) ?? 0) >= graphMinDegree)
      );
    }

    if (query) {
      candidateNodes = new Set(
        Array.from(candidateNodes).filter((nodeId) => {
          const node = graphNodeMap.get(nodeId);
          if (!node) return false;
          const altNames = Array.isArray(node.properties?.alt_names) ? node.properties?.alt_names : [];
          const attributes = Array.isArray(node.properties?.attributes) ? node.properties?.attributes : [];
          const haystack = [
            node.display,
            ...altNames.map((value) => String(value)),
            ...attributes.map((value) => String(value)),
            graphNodeType(node)
          ]
            .join(" | ")
            .toLowerCase();
          return haystack.includes(query);
        })
      );
    }

    if (graphEgoNode) {
      const egoSet = bfsNeighborhood(graphAdjacency, graphEgoNode, graphEgoDepth);
      candidateNodes = new Set(Array.from(candidateNodes).filter((nodeId) => egoSet.has(nodeId)));
    }

    const rankedNodes = Array.from(candidateNodes).sort((left, right) => {
      const degreeDelta = (graphDegreeByNode.get(right) ?? 0) - (graphDegreeByNode.get(left) ?? 0);
      if (degreeDelta !== 0) return degreeDelta;
      const leftNode = graphNodeMap.get(left);
      const rightNode = graphNodeMap.get(right);
      return (leftNode?.display ?? left).localeCompare(rightNode?.display ?? right);
    });
    const limitedNodes = new Set(rankedNodes.slice(0, Math.max(1, graphNodeLimit)));
    const allowedRelTypes = new Set(selectedGraphRelTypes);

    const filteredEdges = sanitizedGraphEdges.filter((edge) => {
      if (!limitedNodes.has(edge.source) || !limitedNodes.has(edge.target)) return false;
      if (allowedRelTypes.size && !allowedRelTypes.has(graphRelType(edge))) return false;
      return true;
    });

    const connectedNodes = new Set<string>();
    for (const edge of filteredEdges) {
      connectedNodes.add(edge.source);
      connectedNodes.add(edge.target);
    }
    const finalNodeIds = new Set([...limitedNodes, ...connectedNodes]);
    const hasGraphRoot = Boolean(graphEgoNode && finalNodeIds.has(graphEgoNode));
    const rootNodeId = hasGraphRoot ? graphEgoNode : "";
    const groupedMode = graphLayoutName === "grouped" && hasGraphRoot;
    const sortedNodeIds = Array.from(finalNodeIds).sort((left, right) =>
      (graphNodeMap.get(left)?.display ?? left).localeCompare(graphNodeMap.get(right)?.display ?? right)
    );

    const groupMembers = new Map<GraphGroupName, string[]>();
    if (groupedMode && rootNodeId) {
      for (const nodeId of sortedNodeIds) {
        if (nodeId === rootNodeId) continue;
        const node = graphNodeMap.get(nodeId);
        if (!node) continue;
        const group = classifyGraphGroup(node, rootNodeId);
        const members = groupMembers.get(group) ?? [];
        members.push(nodeId);
        groupMembers.set(group, members);
      }
    }

    const activeGroups = GRAPH_GROUP_ORDER.filter((group) => (groupMembers.get(group)?.length ?? 0) > 0);
    const groupedPositions =
      groupedMode && rootNodeId ? groupedPresetPositions(rootNodeId, sortedNodeIds, groupMembers) : new Map<string, { x: number; y: number }>();

    const elements: ElementDefinition[] = sortedNodeIds.map((nodeId) => {
      const node = graphNodeMap.get(nodeId);
      const nodeType = node ? graphNodeType(node) : "Entity";
      const data = {
        id: nodeId,
        label: node?.display ?? nodeId,
        type: nodeType,
        degree: graphDegreeByNode.get(nodeId) ?? 0,
        color: stableGraphColor(nodeType),
        isRoot: rootNodeId && nodeId === rootNodeId ? 1 : 0,
        isGroup: 0,
        groupName: groupedMode && node ? classifyGraphGroup(node, rootNodeId) : ""
      };
      const position = groupedPositions.get(nodeId);
      return position ? { data, position } : { data };
    });

    if (groupedMode) {
      for (const group of activeGroups) {
        const groupNodeId = `__group__:${group}`;
        const position = groupedPositions.get(groupNodeId);
        elements.push({
          data: {
            id: groupNodeId,
            label: group,
            type: "Group",
            degree: (groupMembers.get(group)?.length ?? 0) + 1,
            color: stableGraphColor(group),
            isRoot: 0,
            isGroup: 1,
            groupName: group
          },
          ...(position ? { position } : {})
        });
      }
    }

    for (const edge of filteredEdges) {
      elements.push({
        data: {
          id: edge.id || `${edge.source}-${edge.target}-${graphRelType(edge)}`,
          source: edge.source,
          target: edge.target,
          label: edge.display ?? graphRelType(edge),
          rel_type: graphRelType(edge),
          isGroupEdge: 0,
          isRootSpoke: rootNodeId && (edge.source === rootNodeId || edge.target === rootNodeId) ? 1 : 0
        }
      });
    }

    if (groupedMode && rootNodeId) {
      for (const group of activeGroups) {
        const groupNodeId = `__group__:${group}`;
        elements.push({
          data: {
            id: `__group_edge__:${rootNodeId}:${group}`,
            source: rootNodeId,
            target: groupNodeId,
            label: group,
            rel_type: "GROUPS",
            isGroupEdge: 1,
            isRootSpoke: 0
          }
        });
      }
    }

    const layout: Record<string, unknown> = {
      name:
        groupedMode && rootNodeId
          ? "preset"
          : graphLayoutName === "radial"
            ? hasGraphRoot
              ? "breadthfirst"
              : "concentric"
            : graphLayoutName || "cose",
      animate: false,
      fit: true,
      padding: 40
    };
    if (groupedMode && rootNodeId) {
      layout.padding = 72;
    } else if (graphLayoutName === "radial" && hasGraphRoot) {
      layout.roots = `#${graphEgoNode}`;
      layout.circle = true;
      layout.directed = true;
      layout.avoidOverlap = true;
      layout.avoidOverlapPadding = 18;
      layout.spacingFactor = finalNodeIds.size > 40 ? 1.15 : 1.35;
      layout.padding = 56;
    } else if (graphLayoutName === "breadthfirst" && hasGraphRoot) {
      layout.roots = `#${graphEgoNode}`;
      layout.directed = true;
      layout.spacingFactor = 1.1;
    }

    const warnings: string[] = [];
    if (!finalNodeIds.size) warnings.push("No nodes match current filters.");
    else if (candidateNodes.size > graphNodeLimit) warnings.push(`Filtered node set truncated by node limit: ${graphNodeLimit}.`);
    if (graphLayoutName === "grouped" && !hasGraphRoot) warnings.push("Grouped layout requires a root/ego node.");

    return {
      elements,
      layout,
      stylesheet: makeGraphStylesheet(showGraphEdgeLabels, graphMaxDegree),
      stats: groupedMode
        ? `Showing ${finalNodeIds.size} nodes / ${filteredEdges.length} edges with ${activeGroups.length} visual groups (from ${graphNodes.length} nodes / ${sanitizedGraphEdges.length} edges total)`
        : `Showing ${finalNodeIds.size} nodes / ${filteredEdges.length} edges (from ${graphNodes.length} nodes / ${sanitizedGraphEdges.length} edges total)`,
      warning: warnings.join(" ")
    };
  }, [
    graphAdjacency,
    graphDegreeByNode,
    graphEgoDepth,
    graphEgoNode,
    graphLayoutName,
    graphMaxDegree,
    graphMinDegree,
    graphNodeLimit,
    graphNodeMap,
    graphNodes.length,
    graphSearchText,
    sanitizedGraphEdges,
    selectedGraphNodeTypes,
    selectedGraphRelTypes,
    showGraphEdgeLabels
  ]);

  useEffect(() => {
    if (mode !== "evidence" || evidenceView !== "graph" || !cytoscapeRef.current) return;
    const frame = window.requestAnimationFrame(() => {
      cytoscapeRef.current?.resize();
      if (filteredGraph.elements.length > 0) {
        cytoscapeRef.current?.fit(undefined, 28);
      }
      cytoscapeRef.current?.center();
    });
    return () => window.cancelAnimationFrame(frame);
  }, [evidenceView, filteredGraph.elements, filteredGraph.layout, mode]);

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
    setReport(null);
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
        const payload = (await res.json()) as GraphPayload;
        if (!disposed) {
          setGraphNodes(payload.nodes ?? []);
          setGraphEdges(payload.edges ?? []);
          if (payload.graphRoot?.nodeId) {
            setGraphEgoNode(payload.graphRoot.nodeId);
            setGraphEgoDepth(payload.graphRoot.recommendedEgoDepth ?? 2);
            setGraphLayoutName(payload.graphRoot.recommendedLayout ?? "radial");
          }
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

  const deleteRun = async (runId: string) => {
    setDeletingRunId(runId);
    setErrorMessage(null);

    try {
      const res = await fetch(`${API_BASE}/runs/${runId}`, { method: "DELETE" });
      if (!res.ok) {
        throw new Error(`Failed to delete run (${res.status})`);
      }

      setRuns((prev) => {
        const next = prev.filter((run) => run.runId !== runId);
        setSelectedRunId((current) => (current === runId ? (next[0]?.runId ?? null) : current));
        return next;
      });
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : "Failed to delete run");
    } finally {
      setDeletingRunId((current) => (current === runId ? null : current));
    }
  };

  useEffect(() => {
    if (!selectedRunId) return;

    let disposed = false;

    const loadReport = async () => {
      setIsLoadingReport(true);
      try {
        const res = await fetch(`${API_BASE}/runs/${selectedRunId}/report`);
        if (res.status === 404) {
          if (!disposed) setReport(null);
          return;
        }
        if (!res.ok) throw new Error("Failed to load report");
        const payload = (await res.json()) as ReportPayload;
        if (!disposed) setReport(payload);
      } catch (error) {
        if (!disposed) setErrorMessage(error instanceof Error ? error.message : "Failed to load report");
      } finally {
        if (!disposed) setIsLoadingReport(false);
      }
    };

    if (selectedRun?.reportStatus || runIsFinished) {
      void loadReport();
    }

    return () => {
      disposed = true;
    };
  }, [selectedRunId, selectedRun?.reportStatus, runIsFinished]);

  const reportContent =
    report?.json?.finalReport?.trim() ||
    report?.markdown?.trim() ||
    buildReportFallback(report) ||
    report?.json?.evidenceAppendix?.trim() ||
    "";
  const reportMarkdown = buildReportMarkdown(report, reportContent);
  const reportAvailable = Boolean(report?.status || selectedRun?.reportStatus || runIsFinished);
  const reportFilenameBase = slugifyFilename(
    selectedRun?.title || selectedRun?.prompt || selectedRun?.runId || "report",
    selectedRun ? `report-${shortRunId(selectedRun.runId)}` : "report"
  );

  const downloadReportMarkdown = () => {
    if (!reportMarkdown.trim()) return;
    triggerBlobDownload(new Blob([reportMarkdown], { type: "text/markdown;charset=utf-8" }), `${reportFilenameBase}.md`);
  };

  const downloadReportPdf = async () => {
    if (!reportMarkdown.trim()) return;
    setIsDownloadingPdf(true);
    try {
      const doc = new jsPDF({
        orientation: "portrait",
        unit: "pt",
        format: "a4"
      });
      const margin = 48;
      const lineHeight = 18;
      const pageWidth = doc.internal.pageSize.getWidth();
      const pageHeight = doc.internal.pageSize.getHeight();
      const printableWidth = pageWidth - margin * 2;
      const lines = doc.splitTextToSize(reportMarkdown, printableWidth) as string[];
      let cursorY = margin;

      doc.setFillColor(248, 250, 252);
      doc.rect(0, 0, pageWidth, pageHeight, "F");
      doc.setTextColor(15, 23, 42);
      doc.setFont("courier", "normal");
      doc.setFontSize(11);

      for (const line of lines) {
        if (cursorY > pageHeight - margin) {
          doc.addPage();
          doc.setFillColor(248, 250, 252);
          doc.rect(0, 0, pageWidth, pageHeight, "F");
          doc.setTextColor(15, 23, 42);
          doc.setFont("courier", "normal");
          doc.setFontSize(11);
          cursorY = margin;
        }
        doc.text(line || " ", margin, cursorY);
        cursorY += lineHeight;
      }

      doc.save(`${reportFilenameBase}.pdf`);
    } finally {
      setIsDownloadingPdf(false);
    }
  };

  const downloadGraphPng = async () => {
    if (!cytoscapeRef.current || !selectedRun) return;
    setIsDownloadingGraph(true);
    try {
      const dataUrl = cytoscapeRef.current.png({
        full: true,
        bg: "#ffffff",
        scale: 3
      });
      const response = await fetch(dataUrl);
      const pngBlob = await response.blob();
      triggerBlobDownload(pngBlob, `${reportFilenameBase}-graph.png`);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : "Failed to export graph PNG");
    } finally {
      setIsDownloadingGraph(false);
    }
  };

  return (
    <main className="grid-bg h-screen overflow-hidden bg-background text-foreground">
      <div className="h-full bg-gradient-to-br from-slate-950 via-slate-900 to-cyan-950/40">
        <div className="mx-auto flex h-full w-full max-w-[2600px] gap-5 px-5 py-4">
          <aside className="hidden h-full min-h-0 w-[360px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-slate-950/70 p-4 shadow-2xl backdrop-blur lg:flex">
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
                    <div
                      key={run.runId}
                      className={`relative rounded-xl border transition ${
                        run.runId === selectedRunId
                          ? "border-cyan-400 bg-cyan-500/10"
                          : "border-white/10 bg-slate-900/40 hover:border-white/20"
                      }`}
                    >
                      <button
                        type="button"
                        onClick={() => setSelectedRunId(run.runId)}
                        className="w-full rounded-xl p-3 pr-11 text-left"
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
                      <button
                        type="button"
                        aria-label={`Delete run ${shortRunId(run.runId)}`}
                        disabled={deletingRunId === run.runId}
                        onClick={(event) => {
                          event.stopPropagation();
                          void deleteRun(run.runId);
                        }}
                        className="absolute right-2 top-2 flex h-7 w-7 items-center justify-center rounded-full border border-white/10 bg-slate-950/70 text-sm text-cyan-100/80 transition hover:border-danger/50 hover:text-danger disabled:cursor-not-allowed disabled:opacity-50"
                      >
                        {deletingRunId === run.runId ? <Spinner size="sm" color="danger" /> : "×"}
                      </button>
                    </div>
                  ))
                )}
              </div>
            </ScrollShadow>
          </aside>

          <section className="flex h-full min-h-0 flex-1 flex-col rounded-2xl border border-white/10 bg-slate-950/60 p-5 shadow-2xl backdrop-blur">
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

            <div className="inline-flex w-fit rounded-lg border border-white/15 bg-slate-900/70 p-1">
              <button
                type="button"
                onClick={() => setMode("chat")}
                className={`h-10 min-w-[140px] rounded-md px-4 text-sm font-medium tracking-[0.01em] transition ${
                  mode === "chat"
                    ? "bg-cyan-500/20 text-cyan-100 shadow-[inset_0_0_0_1px_rgba(103,232,249,0.45)]"
                    : "text-cyan-100/75 hover:text-cyan-100"
                }`}
              >
                Chat Mode
              </button>
              <button
                type="button"
                onClick={() => setMode("report")}
                disabled={!reportAvailable}
                className={`h-10 min-w-[140px] rounded-md px-4 text-sm font-medium tracking-[0.01em] transition ${
                  mode === "report"
                    ? "bg-cyan-500/20 text-cyan-100 shadow-[inset_0_0_0_1px_rgba(103,232,249,0.45)]"
                    : "text-cyan-100/75 hover:text-cyan-100"
                } disabled:cursor-not-allowed disabled:text-cyan-100/35`}
              >
                Report
              </button>
              <button
                type="button"
                onClick={() => setMode("evidence")}
                disabled={!runIsFinished}
                className={`h-10 min-w-[140px] rounded-md px-4 text-sm font-medium tracking-[0.01em] transition ${
                  mode === "evidence"
                    ? "bg-cyan-500/20 text-cyan-100 shadow-[inset_0_0_0_1px_rgba(103,232,249,0.45)]"
                    : "text-cyan-100/75 hover:text-cyan-100"
                } disabled:cursor-not-allowed disabled:text-cyan-100/35`}
              >
                Evidence
              </button>
            </div>

            {!reportAvailable && !runIsFinished ? (
              <p className="mt-2 text-xs text-default-500">
                Report unlocks after Stage 2 writes a draft or ready snapshot. Evidence unlocks when the run reaches
                `done` / `failed` or the report is `ready`.
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
                          events.map((event) => {
                            const stage = classifyEventStage(event.type);
                            return (
                              <Card key={event.event_id} className="border border-white/10 bg-slate-900/50" shadow="none">
                                <CardHeader className="flex items-center justify-between pb-0">
                                  <div className="flex items-center gap-2">
                                    <p className="font-mono text-xs text-cyan-200">{event.type}</p>
                                    <Chip size="sm" variant="flat" color={stage.color} classNames={{ content: "font-medium" }}>
                                      {stage.label}
                                    </Chip>
                                  </div>
                                  <p className="text-xs text-default-500">{new Date(event.ts).toLocaleString()}</p>
                                </CardHeader>
                                <CardBody>
                                  <pre className="overflow-x-auto whitespace-pre-wrap break-words text-xs text-default-600">
                                    {JSON.stringify(event.payload, null, 2)}
                                  </pre>
                                </CardBody>
                              </Card>
                            );
                          })
                        )}
                      </div>
                    </ScrollShadow>
                  </CardBody>
                </Card>
              )}

              {mode === "report" && (
                <Card className="h-full border border-white/10 bg-slate-950/40" shadow="none">
                  <CardBody className="h-full">
                    <ScrollShadow className="themed-scroll h-full pr-2">
                      <div className="space-y-3">
                        {isLoadingReport ? <Spinner size="sm" color="secondary" /> : null}
                        {!reportContent ? (
                          <p className="text-sm text-default-500">No report snapshot is available for this run yet.</p>
                        ) : (
                          <>
                            <Card className="border border-white/10 bg-slate-900/50" shadow="none">
                              <CardBody className="flex flex-wrap items-center justify-between gap-3 text-sm">
                                <div className="flex flex-wrap gap-2">
                                  <Chip size="sm" color={statusColor(report?.status ?? "draft")} variant="flat">
                                    {report?.status ?? "draft"}
                                  </Chip>
                                  {report?.json?.reportType ? (
                                    <Chip size="sm" variant="flat" color="secondary">
                                      {report.json.reportType}
                                    </Chip>
                                  ) : null}
                                  {typeof report?.json?.qualityOk === "boolean" ? (
                                    <Chip size="sm" variant="flat" color={report.json.qualityOk ? "success" : "warning"}>
                                      {report.json.qualityOk ? "quality ok" : "needs review"}
                                    </Chip>
                                  ) : null}
                                  {typeof report?.json?.refineRound === "number" ? (
                                    <Chip size="sm" variant="flat" color="default">
                                      refine {report.json.refineRound}
                                    </Chip>
                                  ) : null}
                                </div>
                                <div className="flex flex-wrap gap-2">
                                  <Button
                                    size="sm"
                                    variant="bordered"
                                    className="border-cyan-300/30 text-cyan-100"
                                    onPress={downloadReportMarkdown}
                                    isDisabled={!reportMarkdown.trim()}
                                  >
                                    Download Markdown
                                  </Button>
                                  <Button
                                    size="sm"
                                    variant="bordered"
                                    className="border-cyan-300/30 text-cyan-100"
                                    onPress={downloadReportPdf}
                                    isLoading={isDownloadingPdf}
                                    isDisabled={!reportMarkdown.trim()}
                                  >
                                    Download PDF
                                  </Button>
                                </div>
                              </CardBody>
                            </Card>

                            <Card className="border border-white/10 bg-slate-900/50" shadow="none">
                              <CardHeader className="pb-0">
                                <p className="text-sm font-semibold text-cyan-100">
                                  {report?.json?.finalReport?.trim() || report?.markdown?.trim()
                                    ? "Final Report"
                                    : "Draft Sections"}
                                </p>
                              </CardHeader>
                              <CardBody>
                                <pre className="overflow-x-auto whitespace-pre-wrap break-words font-sans text-sm leading-7 text-cyan-50">
                                  {reportContent}
                                </pre>
                              </CardBody>
                            </Card>

                            {report?.json?.evidenceRefs?.length ? (
                              <Card className="border border-white/10 bg-slate-900/50" shadow="none">
                                <CardHeader className="pb-0">
                                  <p className="text-sm font-semibold text-cyan-100">Evidence References</p>
                                </CardHeader>
                                <CardBody className="space-y-3">
                                  {report.json.evidenceRefs.slice(0, 12).map((item) => (
                                    <div key={`${item.citationKey}-${item.createdAt}`} className="rounded-lg border border-white/10 p-3">
                                      <div className="flex flex-wrap gap-2">
                                        <Chip size="sm" variant="flat" color="secondary">
                                          {item.citationKey}
                                        </Chip>
                                        {item.sourceUrl ? (
                                          <Chip size="sm" variant="flat" color="default">
                                            {item.sourceUrl}
                                          </Chip>
                                        ) : null}
                                      </div>
                                      <p className="mt-2 text-sm text-default-200">{item.snippet || "No snippet"}</p>
                                    </div>
                                  ))}
                                </CardBody>
                              </Card>
                            ) : null}

                            {report?.json?.evidenceAppendix?.trim() ? (
                              <Card className="border border-white/10 bg-slate-900/50" shadow="none">
                                <CardHeader className="pb-0">
                                  <p className="text-sm font-semibold text-cyan-100">Evidence Appendix</p>
                                </CardHeader>
                                <CardBody>
                                  <pre className="overflow-x-auto whitespace-pre-wrap break-words font-sans text-sm leading-7 text-default-200">
                                    {report.json.evidenceAppendix}
                                  </pre>
                                </CardBody>
                              </Card>
                            ) : null}
                          </>
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
                                <CardBody className="grid gap-3 text-sm md:grid-cols-[minmax(0,1fr)_auto] md:items-start">
                                  <div className="min-w-0">
                                    <div>
                                      <p className="font-medium text-cyan-100">{file.title || `Document ${shortRunId(file.documentId)}`}</p>
                                      <p className="mt-1 break-all font-mono text-xs text-default-500">
                                        {file.object ? `${file.object.bucket}/${file.object.objectKey}` : "No object pointer"}
                                      </p>
                                    </div>
                                    <p className="mt-2 break-all text-default-600">Source: {file.sourceUrl ?? "-"}</p>
                                  <div className="mt-2 flex flex-wrap gap-2">
                                    <Chip size="sm" variant="dot" color="default">
                                      {file.sourceType}
                                    </Chip>
                                    <Chip size="sm" variant="flat" color="default">
                                      {file.object?.contentType ?? file.contentType ?? "unknown"}
                                    </Chip>
                                  </div>
                                  </div>
                                  <div className="flex justify-start md:justify-end">
                                    <Button
                                      size="sm"
                                      variant="bordered"
                                      className="border-cyan-300/30 text-cyan-100"
                                      as="a"
                                      href={`${API_BASE}/runs/${selectedRunId}/files/${file.documentId}/download`}
                                      isDisabled={!file.object}
                                    >
                                      Download Original
                                    </Button>
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
                        {graphNodes.length === 0 ? (
                          <p className="text-sm text-default-500">No graph nodes with this run's evidence pointers yet.</p>
                        ) : (
                          <div className="grid h-full min-h-0 gap-4 xl:grid-cols-[340px_minmax(0,1.2fr)_320px] 2xl:grid-cols-[360px_minmax(0,1.45fr)_340px]">
                            <Card className="min-h-0 border border-white/10 bg-slate-900/50" shadow="none">
                              <CardBody className="space-y-4 overflow-y-auto text-sm">
                                <div>
                                  <p className="mb-2 font-semibold text-cyan-100">Search</p>
                                  <input
                                    type="text"
                                    value={graphSearchText}
                                    onChange={(event) => setGraphSearchText(event.target.value)}
                                    placeholder="e.g. handle, domain, company"
                                    className="w-full rounded-lg border border-white/10 bg-slate-950/80 px-3 py-2 text-sm text-cyan-50 outline-none placeholder:text-cyan-100/35"
                                  />
                                </div>

                                <div>
                                  <p className="mb-2 font-semibold text-cyan-100">Node Types</p>
                                  <select
                                    multiple
                                    value={selectedGraphNodeTypes}
                                    onChange={(event) =>
                                      setSelectedGraphNodeTypes(Array.from(event.target.selectedOptions, (option) => option.value))
                                    }
                                    className="h-32 w-full rounded-lg border border-white/10 bg-slate-950/80 px-3 py-2 text-sm text-cyan-50"
                                  >
                                    {graphNodeTypeValues.map((type) => (
                                      <option key={type} value={type}>
                                        {type}
                                      </option>
                                    ))}
                                  </select>
                                </div>

                                <div>
                                  <p className="mb-2 font-semibold text-cyan-100">Relation Types</p>
                                  <select
                                    multiple
                                    value={selectedGraphRelTypes}
                                    onChange={(event) =>
                                      setSelectedGraphRelTypes(Array.from(event.target.selectedOptions, (option) => option.value))
                                    }
                                    className="h-32 w-full rounded-lg border border-white/10 bg-slate-950/80 px-3 py-2 text-sm text-cyan-50"
                                  >
                                    {graphRelTypeValues.map((type) => (
                                      <option key={type} value={type}>
                                        {type}
                                      </option>
                                    ))}
                                  </select>
                                </div>

                                <div>
                                  <div className="mb-2 flex items-center justify-between">
                                    <p className="font-semibold text-cyan-100">Minimum Degree</p>
                                    <span className="text-xs text-cyan-100/70">{graphMinDegree}</span>
                                  </div>
                                  <input
                                    type="range"
                                    min={0}
                                    max={Math.max(graphMaxDegree, 1)}
                                    step={1}
                                    value={graphMinDegree}
                                    onChange={(event) => setGraphMinDegree(Number(event.target.value))}
                                    className="w-full"
                                  />
                                </div>

                                <div>
                                  <div className="mb-2 flex items-center justify-between">
                                    <p className="font-semibold text-cyan-100">Node Limit</p>
                                    <span className="text-xs text-cyan-100/70">{graphNodeLimit}</span>
                                  </div>
                                  <input
                                    type="range"
                                    min={graphLimitMin}
                                    max={graphLimitMax}
                                    step={graphLimitMax - graphLimitMin >= 10 ? 10 : 1}
                                    value={Math.min(graphNodeLimit, graphLimitMax)}
                                    onChange={(event) => setGraphNodeLimit(Number(event.target.value))}
                                    className="w-full"
                                  />
                                </div>

                                <div>
                                  <p className="mb-2 font-semibold text-cyan-100">Ego Node</p>
                                  <select
                                    value={graphEgoNode}
                                    onChange={(event) => setGraphEgoNode(event.target.value)}
                                    className="w-full rounded-lg border border-white/10 bg-slate-950/80 px-3 py-2 text-sm text-cyan-50"
                                  >
                                    <option value="">Optional focus node...</option>
                                    {graphNodeOptions.map((option) => (
                                      <option key={option.id} value={option.id}>
                                        {option.label}
                                      </option>
                                    ))}
                                  </select>
                                </div>

                                <div>
                                  <div className="mb-2 flex items-center justify-between">
                                    <p className="font-semibold text-cyan-100">Ego Depth</p>
                                    <span className="text-xs text-cyan-100/70">{graphEgoDepth}</span>
                                  </div>
                                  <input
                                    type="range"
                                    min={0}
                                    max={3}
                                    step={1}
                                    value={graphEgoDepth}
                                    onChange={(event) => setGraphEgoDepth(Number(event.target.value))}
                                    className="w-full"
                                  />
                                </div>

                                <div>
                                  <p className="mb-2 font-semibold text-cyan-100">Layout</p>
                                  <select
                                    value={graphLayoutName}
                                    onChange={(event) => setGraphLayoutName(event.target.value)}
                                    className="w-full rounded-lg border border-white/10 bg-slate-950/80 px-3 py-2 text-sm text-cyan-50"
                                  >
                                    <option value="grouped">Grouped Root</option>
                                    <option value="radial">Radial Root</option>
                                    <option value="cose">COSE (force-directed)</option>
                                    <option value="concentric">Concentric</option>
                                    <option value="breadthfirst">Breadthfirst Tree</option>
                                    <option value="circle">Circle</option>
                                    <option value="grid">Grid</option>
                                  </select>
                                </div>

                                <label className="flex items-center gap-2 text-cyan-100">
                                  <input
                                    type="checkbox"
                                    checked={showGraphEdgeLabels}
                                    onChange={(event) => setShowGraphEdgeLabels(event.target.checked)}
                                  />
                                  Show edge labels
                                </label>
                              </CardBody>
                            </Card>

                            <Card className="min-h-0 border border-white/10 bg-slate-900/50" shadow="none">
                              <CardBody className="flex h-full min-h-0 flex-col gap-3">
                                <div className="flex flex-wrap items-center justify-between gap-3 text-sm">
                                  <div>
                                    <p className="font-semibold text-cyan-100">{filteredGraph.stats}</p>
                                    {filteredGraph.warning ? (
                                      <p className="text-xs text-amber-300">{filteredGraph.warning}</p>
                                    ) : null}
                                  </div>
                                  <Button
                                    size="sm"
                                    variant="bordered"
                                    className="border-cyan-300/30 text-cyan-100"
                                    onPress={downloadGraphPng}
                                    isLoading={isDownloadingGraph}
                                    isDisabled={filteredGraph.elements.length === 0}
                                  >
                                    Download PNG
                                  </Button>
                                </div>
                                <div className="h-[70vh] min-h-[560px] overflow-hidden rounded-xl border border-white/10 bg-white">
                                  <CytoscapeComponent
                                    elements={filteredGraph.elements}
                                    layout={filteredGraph.layout}
                                    stylesheet={filteredGraph.stylesheet}
                                    style={{ width: "100%", height: "100%" }}
                                    cy={(cy: Core) => {
                                      cytoscapeRef.current = cy;
                                      cy.off("tap");
                                      cy.on("tap", "node", (event: EventObject) => {
                                        const nodeId = event.target.id();
                                        const node = graphNodeMap.get(nodeId);
                                        if (event.target.data("isGroup") === 1) {
                                          setGraphSelectionText(
                                            formatSelectionPayload({
                                              element: "group",
                                              id: nodeId,
                                              label: event.target.data("label"),
                                              groupName: event.target.data("groupName"),
                                              memberCount: Math.max(0, Number(event.target.data("degree") || 0) - 1)
                                            })
                                          );
                                          return;
                                        }
                                        setGraphSelectionText(
                                          formatSelectionPayload({
                                            element: "node",
                                            id: nodeId,
                                            display: node?.display ?? nodeId,
                                            type: node ? graphNodeType(node) : null,
                                            labels: node?.labels ?? [],
                                            properties: node?.properties ?? {}
                                          })
                                        );
                                      });
                                      cy.on("tap", "edge", (event: EventObject) => {
                                        const edgeId = event.target.id();
                                        const edge = sanitizedGraphEdges.find((item) => item.id === edgeId);
                                        if (event.target.data("isGroupEdge") === 1 || !edge) {
                                          setGraphSelectionText(
                                            formatSelectionPayload({
                                              element: "edge",
                                              id: edgeId,
                                              source: event.target.source().id(),
                                              target: event.target.target().id(),
                                              relType: event.target.data("rel_type"),
                                              label: event.target.data("label"),
                                              synthetic: true
                                            })
                                          );
                                          return;
                                        }
                                        setGraphSelectionText(
                                          formatSelectionPayload({
                                            element: "edge",
                                            id: edgeId,
                                            source: edge?.source ?? event.target.source().id(),
                                            target: edge?.target ?? event.target.target().id(),
                                            relType: edge ? graphRelType(edge) : event.target.data("rel_type"),
                                            label: edge?.display ?? event.target.data("label"),
                                            properties: edge?.properties ?? {}
                                          })
                                        );
                                      });
                                    }}
                                  />
                                </div>
                              </CardBody>
                            </Card>

                            <Card className="min-h-0 border border-white/10 bg-slate-900/50" shadow="none">
                              <CardBody className="space-y-3 overflow-y-auto text-sm">
                                <div>
                                  <p className="mb-2 font-semibold text-cyan-100">Selection</p>
                                  <pre className="rounded-lg border border-white/10 bg-slate-950/70 p-3 text-xs leading-6 text-cyan-50">
                                    {graphSelectionText}
                                  </pre>
                                </div>

                                <div>
                                  <p className="mb-2 font-semibold text-cyan-100">Type Palette</p>
                                  <div className="space-y-2">
                                    {graphNodeTypeValues.map((type) => (
                                      <div key={type} className="flex items-center gap-3 rounded-lg border border-white/10 bg-slate-950/50 px-3 py-2">
                                        <span
                                          className="inline-block h-3 w-3 rounded-full"
                                          style={{ backgroundColor: stableGraphColor(type) }}
                                        />
                                        <span className="text-cyan-50">{type}</span>
                                      </div>
                                    ))}
                                  </div>
                                </div>
                              </CardBody>
                            </Card>
                          </div>
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
