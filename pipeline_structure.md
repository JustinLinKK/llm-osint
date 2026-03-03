# Pipeline Structure

## Entry point

The LangGraph pipeline is launched from `services/agent-langgraph/src/run_planner.py`.

- `run_planner(...)` executes Stage 1 collection/planning.
- If `--run-stage2` is passed and Stage 1 returns `next_stage == "stage2"`, `run_report_subgraph(...)` executes Stage 2 report generation.

So the current high-level flow is:

1. Stage 1 planner graph decides what tools to run and iterates until collection coverage is good enough.
2. Each selected tool runs through a separate tool-worker graph that normalizes output and ingests it into vector/graph storage.
3. Stage 2 report graph retrieves the accumulated evidence and turns it into a structured final report.

---

## Stage 1: planner graph

Defined in `services/agent-langgraph/src/planner_graph.py`.

### Nodes

1. `analyze_input`
2. `plan_tools`
3. `explain_plan`
4. `execute_tools`
5. `planner_review_receipts`
6. `decide_stop_or_refine`

### What each node does

#### `analyze_input`

- Extracts seed URLs from the prompt and inputs.
- Initializes crawl frontier state:
  - `seed_urls`
  - `pending_urls`
  - `visited_urls`
  - `allowed_hosts`
- Resets iteration counters, noteboard state, follow-up queues, dedupe stores, and coverage ledger.

#### `plan_tools`

- Builds the next round of tool calls.
- Uses `WORK_PLANNER_SYSTEM_PROMPT` when an LLM is available.
- Injects the current noteboard, rationale, and queued follow-up tasks into the planner context.
- Mixes LLM-selected tools with deterministic heuristics and queued follow-up tasks.
- Prioritizes:
  - target person/org discovery
  - domain/email/username/contact pivots
  - business, academic, archive, and relationship expansion
  - in-scope crawl frontier fetches
- Caps each iteration to `STAGE1_MAX_TOOLS_PER_ITERATION`.

#### `explain_plan`

- Emits the selected tool plan and rationale as an event.
- Mostly converts the plan into an externally visible explanation.

#### `execute_tools`

- Fans out the current `tool_plan`.
- For each plan item, calls `run_tool_worker(...)`.
- Optionally auto-ingests extra graph entities derived from raw tool results.
- Executes in parallel up to `LANGGRAPH_MAX_WORKER`.

#### `planner_review_receipts`

- Merges the latest tool receipts into the full run state.
- Updates:
  - `documents_created`
  - `noteboard`
  - crawl frontier
  - follow-up task queues
  - related entity candidates
  - coverage ledger
- Derives deterministic follow-up tasks from specialized rule modules:
  - academic
  - technical
  - business
  - archive/identity
  - relationship
  - contradiction/consistency
  - secondary-entity depth expansion

This is the main bridge between raw tool execution and the next planning round.

#### `decide_stop_or_refine`

- Increments iteration count.
- Recomputes coverage and related-entity depth.
- Stops Stage 1 when one of these is true:
  - max iterations reached
  - minimum iterations reached and there are no more useful tools/follow-ups
  - minimum iterations reached, coverage is sufficient, related-entity depth is sufficient, and no follow-ups remain
- Sets:
  - `done`
  - `next_stage` (`"stage1"` or `"stage2"`)

### Stage 1 loop condition

`should_continue(...)` returns:

- `END` if `done == True`
- otherwise back to `plan_tools`

So Stage 1 is an iterative research loop with planner memory in the noteboard and receipts.

---

## Tool worker graph

Defined in `services/agent-langgraph/src/tool_worker_graph.py`.

Every Stage 1 tool call runs through this subgraph.

### Nodes

1. `execute_tool`
2. `summarize_tool_result`
3. `vector_ingest_worker`
4. `graph_ingest_worker`
5. `receipt_summarize_worker`
6. `persist_receipt`

### What it does

#### `execute_tool`

- Calls the MCP tool with normalized arguments.
- Stores raw tool output.

#### `summarize_tool_result`

- Normalizes raw tool output into compact plain text.
- Uses a tool-specific summary prompt when available.
- This normalized summary becomes the canonical downstream text for ingestion and receipt generation.

#### `vector_ingest_worker`

- Converts normalized text into `ingest_text` arguments.
- Uses `VECTOR_INGEST_SYSTEM_PROMPT`.
- Stores tool output into the vector store for later retrieval.

#### `graph_ingest_worker`

- Preferred path: use `GRAPH_CONSTRUCTION_SYSTEM_PROMPT` to extract open-domain entities and relations, then batch-ingest them with `ingest_graph_entities` and `ingest_graph_relations`.
- Fallback path: use `GRAPH_INGEST_SYSTEM_PROMPT` to create one conservative anchor entity via `ingest_graph_entity`.

#### `receipt_summarize_worker`

- Uses `WORKER_SUMMARIZE_RECEIPT_SYSTEM_PROMPT`.
- Produces planner-facing deltas:
  - short summary
  - key facts
  - next hints

#### `persist_receipt`

- Merges raw result, normalized summary, graph/vector upserts, and receipt LLM output.
- Stores artifacts, summaries, notes, and tool receipts.
- Returns a `ToolReceipt` to the planner graph.

### Why this subgraph matters

It is effectively the normalization and storage boundary:

- raw tool output becomes normalized evidence text
- normalized evidence text becomes vector + graph data
- planner receives a compact receipt instead of raw tool output

---

## Stage 2: report graph

Defined in `services/agent-langgraph/src/report_graph.py`.

This graph turns Stage 1 evidence into a final report.

### Main path

1. `report_init_node`
2. `build_outline_node`
3. `section_router_node`
4. `process_sections_node`
5. `reduce_sections_node`
6. `final_reflection_node`
7. `quality_gate_node`
8. `finalize_report_node`

There is also a refinement loop through:

- `prepare_section_revisions_node`
- `refine_retrieval_node`

### What each node does

#### `report_init_node`

- Determines report type (`person` or `org`-like logic from helpers).
- Picks primary entities from Stage 1 evidence.
- Initializes report memory, section buffers, quality state, and issue tracking.

#### `build_outline_node`

- Uses `REPORT_OUTLINE_SYSTEM_PROMPT` if LLM is available.
- Otherwise falls back to a helper-generated default outline.
- Produces section tasks with objectives and query hints.

#### `section_router_node`

- Chooses which sections need to be processed next.
- Routes either:
  - missing sections from the outline, or
  - revision targets from the reflection step
- Can inject special sections such as:
  - timeline normalization
  - conflict resolution

#### `process_sections_node`

- Parallel section worker stage.
- For each section:
  - hydrate graph context
  - retrieve vector and graph evidence
  - pack evidence
  - extract claims
  - verify claims
  - draft section text

This is the current active section-processing path. The file still contains individual node implementations for the same steps, but the graph is wired through the batched `process_sections_node`.

#### `reduce_sections_node`

- Deduplicates drafts, claims, evidence, and issues.
- Runs consistency validation.
- Rebuilds report memory.

#### `final_reflection_node`

- Uses `REPORT_SECTION_REFLECTION_SYSTEM_PROMPT`.
- Reviews the report section by section.
- Marks sections as:
  - `ok`
  - `needs_revision`
  - `missing`

#### `quality_gate_node`

- Applies deterministic quality checks:
  - missing required sections
  - unsupported high-impact claims
  - consistency issues
  - coverage completeness
  - section reflection status
- Decides whether to finalize or refine.

#### `prepare_section_revisions_node`

- Keeps only the sections that need more work.
- Carries forward targeted query hints from reflection output.

#### `refine_retrieval_node`

- Broadens retrieval hints when the report is weak because of evidence gaps or contradictions.
- Clears current drafts/claims/evidence for another retrieval pass.

#### `finalize_report_node`

- Assembles the final report and evidence appendix.
- Persists the final report snapshot.
- Emits `REPORT_READY`.

### Stage 2 routing

- If quality is good enough, Stage 2 finalizes.
- If not, it loops through section revision or retrieval refinement until:
  - quality is acceptable, or
  - `max_refine_rounds` is reached

---

## Active system prompts and what they are for

Defined in `services/agent-langgraph/src/system_prompts.py`.

### Stage 1 planner prompt

#### `WORK_PLANNER_SYSTEM_PROMPT`

Used by `planner_graph.plan_tools`.

Purpose:

- Tells the planner how to choose the next tool sequence.
- Enforces public-information OSINT boundaries.
- Pushes the planner toward:
  - reliable, corroborated evidence
  - prerequisite-aware tool ordering
  - broad person-target coverage
  - later-round expansion into relationships, business roles, contact pivots, archives, and history

This is the main strategic prompt for Stage 1.

### Tool-result normalization prompts

These are used by `tool_worker_graph._tool_summary_prompt(...)` during `summarize_tool_result`.

#### `WORKER_TOOL_SUMMARY_SYSTEM_PROMPT`

Generic fallback summarizer for tool output.

Use case:

- Any tool without a more specialized prompt.
- Produces a structured plain-text summary with tool, args, evidence refs, findings, entities, and short raw snippets.

#### `PERSON_SEARCH_TOOL_SUMMARY_SYSTEM_PROMPT`

For `person_search`.

Focus:

- current role/status
- biography/history
- contact signals
- relationship clues

#### `GOOGLE_SERP_PERSON_SEARCH_TOOL_SUMMARY_SYSTEM_PROMPT`

For `google_serp_person_search`, `tavily_person_search`, and `tavily_research`.

Focus:

- what source types were found
- visible bio/contact/relationship clues
- strong pivots such as URLs, institutions, co-authors, emails, phones, domains

#### `ARXIV_TOOL_SUMMARY_SYSTEM_PROMPT`

For `arxiv_search_and_download`.

Focus:

- publication history
- research areas
- co-authors and affiliations
- paper IDs and URLs

#### `GITHUB_TOOL_SUMMARY_SYSTEM_PROMPT`

For `github_identity_search`.

Focus:

- identity anchors
- repository footprint
- org memberships
- public contact/linkage signals

#### `PERSONAL_SITE_TOOL_SUMMARY_SYSTEM_PROMPT`

For `personal_site_search`.

Focus:

- canonical site URL
- contact signals
- linked profiles
- hosting/technology clues

#### `GITLAB_TOOL_SUMMARY_SYSTEM_PROMPT`

For `gitlab_identity_search`.

Focus:

- GitLab identity anchors
- project footprint
- namespace/org membership

#### `PACKAGE_REGISTRY_TOOL_SUMMARY_SYSTEM_PROMPT`

For `package_registry_search`, `npm_author_search`, and `crates_author_search`.

Focus:

- package names and timestamps
- maintainer usernames/emails
- repo URLs for follow-up pivots

#### `WAYBACK_TOOL_SUMMARY_SYSTEM_PROMPT`

For `wayback_fetch_url`.

Focus:

- snapshot coverage
- archive timestamps
- archived URLs

#### `BUSINESS_ROLE_TOOL_SUMMARY_SYSTEM_PROMPT`

For `open_corporates_search`, `company_officer_search`, `company_filing_search`, `sec_person_search`, and `director_disclosure_search`.

Focus:

- company identity and status
- officer/director roles
- filing references and dates
- high-value business pivots

#### `DOMAIN_WHOIS_TOOL_SUMMARY_SYSTEM_PROMPT`

For `domain_whois_search`.

Focus:

- registrar/registrant/domain data
- affiliation clues
- RDAP source URL

#### `ARCHIVE_DIFF_TOOL_SUMMARY_SYSTEM_PROMPT`

For `wayback_domain_timeline_search` and `historical_bio_diff`.

Focus:

- archived-history changes over time
- timestamps
- exact archived URLs

#### `SANCTIONS_TOOL_SUMMARY_SYSTEM_PROMPT`

For `sanctions_watchlist_search`.

Focus:

- whether exact-name matches exist
- matched names/programs/lists
- explicit no-match reporting when applicable

#### `IDENTITY_EXPANSION_TOOL_SUMMARY_SYSTEM_PROMPT`

For:

- `alias_variant_generator`
- `username_permutation_search`
- `cross_platform_profile_resolver`
- `institution_directory_search`
- `email_pattern_inference`
- `contact_page_extractor`
- `reddit_user_search`
- `mastodon_profile_search`
- `substack_author_search`
- `medium_author_search`

Focus:

- alias and username variants
- matched profile URLs
- institution directory fields
- inferred emails/contact pivots

#### `ACADEMIC_IDENTITY_TOOL_SUMMARY_SYSTEM_PROMPT`

For `orcid_search`, `semantic_scholar_search`, and `dblp_author_search`.

Focus:

- identity resolution
- academic IDs
- affiliations and topics
- confidence reasons and evidence URLs

#### `PUBMED_TOOL_SUMMARY_SYSTEM_PROMPT`

For `pubmed_author_search`.

Focus:

- biomedical publication footprint
- PMIDs, journals, dates
- coauthor and institution clues

#### `GRANT_TOOL_SUMMARY_SYSTEM_PROMPT`

For `grant_search_person`.

Focus:

- grant IDs, titles, agencies, institutions, amounts, dates
- PI/co-PI clues
- affiliation confirmation

#### `PATENT_TOOL_SUMMARY_SYSTEM_PROMPT`

Defined but currently not active in routing because patent tool integration is commented out.

#### `CONFERENCE_TOOL_SUMMARY_SYSTEM_PROMPT`

For `conference_profile_search`.

Focus:

- venues, years, titles, URLs
- participation patterns
- collaborator and topic clues

### Ingestion and receipt prompts

#### `VECTOR_INGEST_SYSTEM_PROMPT`

Used by `vector_ingest_worker`.

Purpose:

- turns normalized summary text into `ingest_text` arguments
- preserves source URLs and evidence refs
- controls chunking hints for vector storage

#### `GRAPH_CONSTRUCTION_SYSTEM_PROMPT`

Used by the preferred graph ingestion path.

Purpose:

- converts normalized tool output into open-domain entities and relations
- leaves ID generation and merge logic to the downstream ingest tools

#### `GRAPH_INGEST_SYSTEM_PROMPT`

Used only as a fallback graph-ingest path.

Purpose:

- creates one conservative anchor entity and a few high-confidence relations when batch graph extraction is unavailable or empty

#### `WORKER_SUMMARIZE_RECEIPT_SYSTEM_PROMPT`

Used by `receipt_summarize_worker`.

Purpose:

- converts normalized summary plus graph deltas into planner-friendly receipt output
- emits concise `summary`, `key_facts`, and `next_hints`

### Stage 2 report prompts

#### `REPORT_OUTLINE_SYSTEM_PROMPT`

Used by `build_outline_node`.

Purpose:

- designs the report section list
- pushes for broad coverage, chronology, relationships, contact surface, and uncertainty handling

#### `REPORT_SECTION_CLAIMS_SYSTEM_PROMPT`

Used by section claim extraction.

Purpose:

- converts evidence bundles into atomic, citation-bound claims
- keeps claims evidence-backed and explicit about chronology/conflicts

#### `REPORT_SECTION_DRAFT_SYSTEM_PROMPT`

Used from `report_helpers.draft_section_content(...)`.

Purpose:

- turns verified claims plus evidence into long-form section prose
- preserves inline citation keys
- supports revision passes with `current_content`, `revision_focus`, and `next_step_suggestion`

#### `REPORT_SECTION_REFLECTION_SYSTEM_PROMPT`

Used by `final_reflection_node`.

Purpose:

- reviews section quality
- identifies missing or weak sections
- provides targeted retrieval/rewrite hints

#### `FINAL_REPORT_ASSEMBLY_SYSTEM_PROMPT`

Used during final report assembly.

Purpose:

- merges section drafts into one cohesive, evidence-dense final report
- preserves detail, citations, uncertainty, and contradictions

---

## Deprecated prompts still present in the file

These are commented out or marked legacy:

- `GRAPH_BATCH_INGEST_SYSTEM_PROMPT`
- `GRAPH_RELATIONS_SYSTEM_PROMPT`

These are old graph-ingestion prompt paths kept for rollback/reference, but they are not part of the current active pipeline wiring.

---

## Practical summary

The current pipeline is not a single monolithic graph. It is three connected layers:

1. A Stage 1 planner loop that decides what to collect next.
2. A per-tool worker graph that normalizes and stores each tool result.
3. A Stage 2 report graph that retrieves stored evidence, drafts sections, checks quality, and assembles the final report.

The prompt architecture mirrors that split:

- one strategic planner prompt
- many tool-specific normalization prompts
- ingestion and receipt prompts for turning tool output into stored evidence
- report prompts for outline, claim extraction, drafting, reflection, and final synthesis
