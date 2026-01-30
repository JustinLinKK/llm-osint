import { z } from "zod";

export const ToolPlanItemSchema = z.object({
  tool: z.string().min(1),
  params: z.record(z.unknown()).default({}),
  rationale: z.string().min(1).optional(),
  stop_conditions: z.array(z.string().min(1)).optional()
});

export const ToolPlanSchema = z.object({
  run_id: z.string().uuid().optional(),
  created_at: z.string().datetime({ offset: true }).optional(),
  items: z.array(ToolPlanItemSchema).min(1)
});

export const ToolResultObjectSchema = z.object({
  document_id: z.string().uuid(),
  object_key: z.string().min(1),
  content_type: z.string().min(1).optional(),
  size_bytes: z.number().int().nonnegative().optional(),
  version_id: z.string().min(1).nullable().optional()
});

export const ToolResultSchema = z.object({
  tool: z.string().min(1),
  ok: z.boolean(),
  message: z.string().min(1).optional(),
  data: z.unknown().optional(),
  documents: z.array(z.string().uuid()).optional(),
  objects: z.array(ToolResultObjectSchema).optional()
});

export const RunEventTypeSchema = z.enum([
  "RUN_CREATED",
  "PLANNER_STARTED",
  "TOOLS_SELECTED",
  "TOOL_CALL_STARTED",
  "TOOL_CALL_FINISHED",
  "PROCESSING_STARTED",
  "CHUNKING_FINISHED",
  "EMBEDDING_FINISHED",
  "GRAPH_FINISHED",
  "SYNTHESIS_STARTED",
  "REPORT_READY",
  "RUN_FAILED"
]);

export const RunEventSchema = z.object({
  run_id: z.string().uuid(),
  type: RunEventTypeSchema,
  ts: z.string().datetime({ offset: true }),
  payload: z.record(z.unknown()).optional()
});

export const CitationSchema = z.object({
  document_id: z.string().uuid(),
  chunk_id: z.string().uuid().optional(),
  object_key: z.string().min(1).optional(),
  source_url: z.string().url().optional(),
  quote: z.string().min(1).optional()
});

export const ReportSchema = z.object({
  run_id: z.string().uuid(),
  report_id: z.string().uuid().optional(),
  created_at: z.string().datetime({ offset: true }).optional(),
  markdown: z.string().min(1),
  json: z.record(z.unknown()).optional(),
  citations: z.array(CitationSchema).optional()
});

export type ToolPlanItem = z.infer<typeof ToolPlanItemSchema>;
export type ToolPlan = z.infer<typeof ToolPlanSchema>;
export type ToolResultObject = z.infer<typeof ToolResultObjectSchema>;
export type ToolResult = z.infer<typeof ToolResultSchema>;
export type RunEventType = z.infer<typeof RunEventTypeSchema>;
export type RunEvent = z.infer<typeof RunEventSchema>;
export type Citation = z.infer<typeof CitationSchema>;
export type Report = z.infer<typeof ReportSchema>;
