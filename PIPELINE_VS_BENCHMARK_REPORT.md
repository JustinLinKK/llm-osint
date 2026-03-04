# Pipeline vs Benchmark Comparison Report

Date: 2026-03-03

Inputs compared:
- Pipeline output: `pipeline_result.md`
- Benchmark output: `Benchmark.txt` (header includes `260227`)

This report summarizes what is materially different between the two artifacts and proposes a concrete, prioritized plan to close the gap in future pipeline runs.

## Executive Summary

The pipeline report is structurally comprehensive (11 sections) but evidence-light: the narrative relies primarily on LinkedIn + generic search receipts and fails to collect several "hard anchors" that the benchmark uses to make identity and timeline claims with high confidence.

The benchmark wins primarily on:
- **Source diversity and traceability**: 41 unique URLs across 13 domains vs the pipeline's 6 unique URLs across 6 domains.
- **Hard anchors**: OpenReview/Semantic Scholar profiles, institutional thesis PDFs, and a real institutional email (all absent from the pipeline report).
- **Reduced noise**: The pipeline introduces uncorroborated/low-signal leads (MIT/Stony Brook references) that the benchmark avoids or replaces with official docs.

## Quantitative Deltas (Measured)

Baseline diff from `python benchmark_diff.py --pipeline-report pipeline_result.md --benchmark-report Benchmark.txt`:
```json
{
  "section_coverage_diff": {"pipeline_sections": 11, "benchmark_sections": 6, "delta": -5},
  "hard_anchors": {
    "collaborator_clustering": {"pipeline": 0, "benchmark": 1},
    "institutional_email_domains": {"pipeline": 0, "benchmark": 1},
    "official_docs": {"pipeline": 1, "benchmark": 1},
    "stable_profile_ids": {"pipeline": 0, "benchmark": 1}
  },
  "pipeline_urls": 74,
  "benchmark_urls": 95
}
```

Additional measurable signals (extracted from the two files):

| Metric | Pipeline (`pipeline_result.md`) | Benchmark (`Benchmark.txt`) | Why it matters |
|---|---:|---:|---|
| Section headers | 11 | 6 | Pipeline has more structure, but not more grounded facts. |
| URL count | 74 | 95 | Raw "citation volume." |
| Unique URLs | 6 | 41 | Proxy for source diversity and dedupe quality. |
| Unique domains | 6 | 13 | Proxy for coverage breadth beyond a single platform. |
| Emails extracted | `error-lite@duckduckgo.com` | `xinyupi2@illinois.edu` | Benchmark has a real institutional pivot; pipeline has a placeholder artifact. |
| Mentions UCSD / San Diego | No | Yes | Benchmark establishes current affiliation; pipeline misses it entirely. |
| Mentions OpenReview / Semantic Scholar | No | Yes | Stable IDs are key to disambiguation + publication graph expansion. |

Domains present in the benchmark but missing in the pipeline report:
`openreview.net`, `www.semanticscholar.org`, `escholarship.org`, `arxiv.org`, `aclanthology.org`, `www.researchgate.net`, `dl.acm.org`, `www.truepeoplesearch.com`, plus a few supporting PDF/alt subdomains.

Domains present in the pipeline report but not in the benchmark:
`html.duckduckgo.com` and `hi` (tool invocation artifacts), plus incidental sources (`www.kaggle.com`, `www.aies-conference.com`, `github.com`).

## Qualitative Differences (What Changed, Not Just Counts)

### 1) Identity Resolution

Benchmark behavior:
- Resolves the "Frederick Pi" vs "Xinyu Pi" naming split using **stable academic profiles** (OpenReview, Semantic Scholar) and ties that back to LinkedIn.

Pipeline behavior:
- Leaves canonical identity **unresolved** and heavily weights LinkedIn + "person search" receipts. It lacks the stable identifiers that would collapse aliases into one entity.

What to improve:
- Treat "stable IDs" as first-class targets (OpenReview profile ID, Semantic Scholar author ID, ORCID if present) and hard-gate Stage 1 completion on them when the target is an academic/researcher.

### 2) Education / Affiliation Timeline

Benchmark behavior:
- Establishes a clear UIUC -> UCSD path and uses **official documents** (institutional thesis PDFs / eScholarship) and an arXiv PDF to support the timeline and affiliation claims.

Pipeline behavior:
- Mentions UIUC (2018-2023) but fails to discover/confirm UCSD and relies on low-confidence directory search output (including a placeholder email).
- Includes noisy leads (MIT/Stony Brook commencement references) without resolving them through official records.

What to improve:
- Add deterministic pivots: when arXiv IDs exist, fetch arXiv PDF(s) and extract affiliation/email lines; when "UCSD" or similar appears, search institutional thesis repositories and ingest PDFs as Document entities.

### 3) Publications and Collaboration Network

Benchmark behavior:
- Uses Semantic Scholar / ACL Anthology / arXiv URLs to ground publication claims and uses the publication graph to infer a cohesive collaborator cluster.

Pipeline behavior:
- States "12 arXiv preprints" and some coauthor counts, but **does not provide resolvable source URLs** for the publications and does not output any explicit clustering result.

What to improve:
- If the pipeline can produce `coauthor_graph_search` results, it should also:
- Build explicit clusters (even connected components is fine for MVP).
- Cite the upstream source(s) that justify the coauthor graph (Semantic Scholar/OpenReview/arXiv).

### 4) Evidence Quality, Traceability, and Noise

Benchmark behavior:
- Uses many unique URLs, and citations are "reader-usable" (you can click through to validate).

Pipeline behavior:
- Concentrates citations in a long "Evidence Index" and repeats the same LinkedIn URL many times.
- Includes internal tool artifacts (`https://hi`) which should never appear as evidence in a final report.

What to improve:
- Separate "tool receipts" from "human-verifiable sources."
- De-duplicate and filter evidence: only include real URLs with enough context; never include placeholder/tool-call URLs in final reporting.

## Root Causes (Pipeline-Level)

These map to your documented architecture in `pipeline_structure.md`.

1) **Stage 1 is not hard-gated on hard anchors**: the system can stop without collecting stable profile IDs, institutional docs, or an institutional email pivot.

2) **Academic pivot chains are missing or not reliably triggered**: even when publications are detected (arXiv IDs, coauthor graph data), the pipeline output does not show follow-ups into OpenReview/Semantic Scholar/eScholarship.

3) **Normalization loses pivots and leaks internal artifacts**: the evidence index contains tool invocation placeholders (`https://hi`) and over-repeats LinkedIn, implying the ingestion boundary is not producing clean "source objects."

4) **Stage 2 reporting is not graph-driven enough**: the report reads like a narrative summary of receipts rather than a synthesis of typed entities, relations, and anchored documents.

## Improvement Plan (Prioritized, Measurable)

This is consistent with `pipeline_gap_closure_plan.txt`, but tightened to the concrete gaps observed in this run.

### P0 (Same week): Make Missing Anchors Unmissable

1) Add hard stop criteria for academic targets
If the target is "researcher-like" (publications detected, arXiv IDs present, or institution keywords present), Stage 1 must either collect at least one stable profile ID (OpenReview/Semantic Scholar/ORCID) and at least one official doc (institutional PDF or institutional profile page), or exit with an explicit "unresolved anchors" block in the final report.

2) Add deterministic follow-ups when publication signals appear
If arXiv IDs are present, fetch arXiv PDF and extract affiliations/emails. If an institution is present, search for official institutional pages/docs.

Where this likely lands:
- Stage 1 gating: `services/agent-langgraph/src/planner_graph.py`
- Follow-up derivation: `planner_review_receipts` logic (see `pipeline_structure.md`)

### P1 (1-2 weeks): Fix Evidence Hygiene and Citation Usefulness

1) Normalize "source objects" in tool workers
Every tool receipt should emit `source_url` (real URL, not a placeholder), stable IDs when present, and extracted pivot fields (emails, profile IDs, institutions, dates). Filter known placeholder emails like `error-lite@duckduckgo.com`.

Where this likely lands:
- Tool normalization: `services/agent-langgraph/src/tool_worker_graph.py`

2) Stage 2 reporting should cite sources per claim
High-impact claims (identity, current affiliation, degrees) must have at least one direct URL citation in the section text. Evidence index should be deduped and contain only human-verifiable sources (no tool-call JSON, no `https://hi`).

Where this likely lands:
- Report synthesis + quality gate: `services/agent-langgraph/src/report_graph.py`

### P2 (2-4 weeks): Graph-First Synthesis (Benchmark-Level)

1) Promote stable IDs + official docs to first-class graph entities
Entity types: `Profile`, `Document`, `Publication`, `Email`, `Institution`. Relations: `HAS_PROFILE`, `HAS_EMAIL`, `STUDIED_AT`, `AFFILIATED_WITH`, `PUBLISHED`, `COAUTHORED_WITH`.

2) Collaborator clustering as a standard output
Even a simple connected-components clustering over `COAUTHORED_WITH` edges is enough to match the benchmark's "dense network" signal.

3) Timeline section driven by dated facts in the graph
Prefer dates pulled from official docs/profiles/papers.

## Acceptance Criteria (What "Improved" Looks Like)

On this benchmark pair, the pipeline should converge to:
- Unique URLs >= 25 and unique domains >= 10 (order-of-magnitude improvement from current 6/6).
- Presence of at least one stable profile ID domain (`openreview.net`, `semanticscholar.org`, `orcid.org`) when publications are detected.
- At least one institutional email pivot (when present in PDFs) and no placeholder emails in report output.
- No internal tool-call placeholder URLs in the final report (e.g., no `https://hi`).
