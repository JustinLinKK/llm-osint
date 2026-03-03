# Pipeline Comprehensiveness TODO

This backlog captures the gap between the current pipeline and a deeper research baseline with stronger entity resolution, richer academic coverage, and stricter contradiction handling.

## Immediate

- [x] Tighten Stage 1 stop criteria so the planner cannot stop after identity plus one shallow anchor.
- [x] Improve alias resolution so profile merges can be justified by multiple independent signals, not just exact username/bio/site matches.
- [x] Trigger deterministic follow-up when publication or collaborator contradictions appear in planner receipts.
- [x] Expose a first-class `CanonicalIdentity` object in the final report output.
- [x] Expose a first-class `DisambiguationEvidence` array in the final report output.

## Coverage Contracts

- [x] Define a per-claim evidence contract: `claim`, `entity_id`, `source_url`, `source_type`, `timestamp`, `confidence`, `quote_span?`.
- [x] Enforce `no URL => no hard claim` in Stage 2 report synthesis.
- [x] Add a structured `AttemptLog` with query, tools/sources hit, and outcome.
- [x] Add `NotFoundReasons` with enumerated reasons: `private`, `ambiguous_identity`, `not_searched`, `not_publicly_found`, `auth_blocked`.

## Identity Resolution

- [x] Add a planner-side `EntityResolver` stage before academic and relationship pivots.
- [x] Merge aliases only when at least two independent corroborations exist.
- [x] Preserve split-name branding cases as one canonical identity plus aliases, not as unresolved ambiguity.
- [x] Distinguish `low social footprint` from `low public footprint` when academic evidence exists.

## Timeline

- [x] Add a `Timeline` object with dated events and linked evidence for education, roles, publications, and public milestones.
- [x] Add report stop criteria for minimum timeline completeness.
- [ ] Normalize conflicting dates into a chronology section with unresolved points explicitly marked.

## Academic

- [x] Add `PublicationInventory` with title, year, venue, coauthors, and primary links.
- [x] Add `ThesisInventory` with PDF/source link, advisor/committee, and abstract keywords.
- [x] Add `ResearchThemes` derived from abstracts/keywords, not summary prose guesses.
- [x] Add deterministic pivots from academic hits to institution directory and email inference.
- [ ] Add thesis / dissertation / PDF discovery and parsing flow.
- [ ] Add acknowledgement extraction for high-signal academic PDFs.

## Relationships

- [x] Replace the lightweight coauthor count output with a typed collaboration graph.
- [x] Model `Person`, `Institution`, `Paper`, and `Venue` nodes.
- [x] Model `COAUTHOR_OF`, `ADVISED_BY`, `AFFILIATED_WITH`, and `MEMBER_OF_LAB` edges.
- [x] Generate `CoauthorClusters` and representative works automatically from publication metadata.

## Profiles And Activity

- [x] Add a `ProfileIndex` covering LinkedIn, GitHub, Google Scholar, ORCID, ResearchGate, lab pages, and personal sites.
- [x] Capture last-active timestamps when visible.
- [x] Extract key fields per profile: title, affiliation, projects, pinned repos, public links.
- [x] Stop reporting raw 404 checks as findings unless they support a conclusion.

## Consistency And Limits

- [x] Promote contradiction detection from report-time warning to a hard planner gate.
- [x] Add automatic re-search rules for publication, affiliation, and relationship conflicts.
- [x] Split limits into `not searched`, `not public`, `ambiguous identity`, and `access blocked`.
- [x] Make coverage completion depend on structured fields, not regexes over report text.

## Heavy Mode

- [ ] Add optional parallel workers: identity, academic, relationship, timeline, archive.
- [ ] Merge only compressed worker outputs into synthesis.
- [ ] Add per-worker coverage ledgers so one stuck worker does not collapse overall depth.
