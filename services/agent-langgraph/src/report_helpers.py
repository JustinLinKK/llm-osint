from __future__ import annotations

import os
import re
from urllib.parse import urlparse
from typing import TYPE_CHECKING, Any, Dict, List

from logger import get_logger
from report_models import (
    AttemptLogEntryModel,
    ClaimModel,
    CoauthorClusterModel,
    CollaborationGraphEdgeModel,
    CollaborationGraphNodeModel,
    CanonicalIdentityModel,
    ConsistencyIssueModel,
    CoverageItemModel,
    CoverageLedgerModel,
    DisambiguationEvidenceModel,
    DocDeepDiveModel,
    EntityModel,
    EvidenceRefModel,
    NotFoundReasonModel,
    ProfileIndexItemModel,
    PublicationInventoryItemModel,
    ResearchThemeModel,
    ReportMemoryModel,
    ReportState,
    SectionDraftModel,
    SectionTaskModel,
    ThesisInventoryItemModel,
    TimelineEventModel,
)
from system_prompts import FINAL_REPORT_ASSEMBLY_SYSTEM_PROMPT, REPORT_SECTION_DRAFT_SYSTEM_PROMPT
from target_normalization import extract_person_targets

if TYPE_CHECKING:
    from mcp_client import McpClientProtocol
    from openrouter_llm import OpenRouterLLM
    from tool_worker_graph import ToolReceipt

logger = get_logger(__name__)


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def decide_report_type(prompt: str, notes: List[str]) -> str:
    text = " ".join([prompt] + notes).lower()
    person_markers = (
        r"\bperson(?:al)?\b",
        r"\bindividual\b",
        r"\bbiograph(?:y|ical)\b",
        r"\bprofile\b",
        r"\bwho is\b",
    )
    org_markers = (
        r"\binc\b",
        r"\bllc\b",
        r"\bcompany\b",
        r"\borganization\b",
        r"\bcorp(?:oration)?\b",
        r"\bbusiness\b",
        r"\bdomain\b",
        r"\bwebsite\b",
    )

    if any(re.search(pattern, text) for pattern in person_markers):
        return "person"
    if any(re.search(pattern, text) for pattern in org_markers):
        return "org"
    return "person"


def default_outline(report_type: str, primary_entities: List[str]) -> List[SectionTaskModel]:
    core_ids = primary_entities[:4]
    if report_type == "org":
        return [
            SectionTaskModel(
                section_id="org_identity",
                title="Organization identity",
                objective="Legal/brand names, aliases, ownership clues, registration and official properties.",
                required=True,
                entity_ids=core_ids,
                query_hints=["legal name", "registry", "about", "official website"],
            ),
            SectionTaskModel(
                section_id="org_people_and_relations",
                title="People and relationships",
                objective="Leadership, employees, partners, subsidiaries, and notable external relationships.",
                required=True,
                entity_ids=core_ids,
                query_hints=["founder", "CEO", "team", "partners", "subsidiary"],
            ),
            SectionTaskModel(
                section_id="org_presence_and_assets",
                title="Presence and assets",
                objective="Domains, infrastructure, social media accounts, public channels, and digital footprint.",
                required=True,
                entity_ids=core_ids,
                query_hints=["domain", "subdomain", "linkedin", "x.com", "github"],
            ),
            SectionTaskModel(
                section_id="org_activity_and_history",
                title="Activity and history",
                objective="Timeline of announcements, products, incidents, events, hiring, and public milestones.",
                required=True,
                entity_ids=core_ids,
                query_hints=["news", "press release", "launch", "timeline", "history"],
            ),
            SectionTaskModel(
                section_id="org_risks_uncertainty",
                title="Risks and uncertainties",
                objective="Conflicts, unresolved claims, legal/regulatory signals, and confidence caveats.",
                required=True,
                entity_ids=core_ids,
                query_hints=["lawsuit", "violation", "complaint", "controversy", "risk"],
            ),
        ]
    return [
        SectionTaskModel(
            section_id="identity_profile",
            title="Identity profile",
            objective="Name variants, identifiers, known aliases, demographics clues, and core biographic markers.",
            required=True,
            entity_ids=core_ids,
            query_hints=["full name", "alias", "bio", "about", "profile"],
        ),
        SectionTaskModel(
            section_id="biography_history",
            title="Biography and history",
            objective="Educational, academic, employment, publication, and historical milestones with dates and institutions.",
            required=True,
            entity_ids=core_ids,
            query_hints=["education", "academic history", "employment history", "publication", "research", "timeline"],
        ),
        SectionTaskModel(
            section_id="academic_research",
            title="Academic / Research",
            objective="Resolved academic identities, ORCID/DBLP/Semantic Scholar/PubMed profiles, publication patterns, grants, patents, venues, and traceable evidence.",
            required=False,
            entity_ids=core_ids,
            query_hints=["orcid", "semantic scholar", "dblp", "pubmed", "grant", "patent", "conference", "publication count", "evidence url"],
        ),
        SectionTaskModel(
            section_id="code_software_footprint",
            title="Code / Software Footprint",
            objective="Public GitHub and personal-site signals, repository footprint, org memberships, package/container publishing clues, and technical affiliation evidence.",
            required=False,
            entity_ids=core_ids,
            query_hints=["github", "gitlab", "hugging face", "personal site", "repository", "npm", "pypi", "crates", "docker hub"],
        ),
        SectionTaskModel(
            section_id="public_contact_methods",
            title="Public contact methods",
            objective="Publicly available emails, phone numbers, websites, contact pages, directories, and location/contact signals.",
            required=True,
            entity_ids=core_ids,
            query_hints=["address", "location", "city", "email", "phone"],
        ),
        SectionTaskModel(
            section_id="relationships_and_associates",
            title="Relationships and associates",
            objective="Related people, colleagues, co-authors, advisors, employers, collaborators, and publicly visible relationship types.",
            required=True,
            entity_ids=core_ids,
            query_hints=["co-author", "advisor", "colleague", "works at", "lab", "department", "network"],
        ),
        SectionTaskModel(
            section_id="social_accounts_and_interests",
            title="Social accounts and interests",
            objective="Social media accounts, recurring topics, hobbies, interests, affiliations, and public behavior patterns from posts/content.",
            required=True,
            entity_ids=core_ids,
            query_hints=["linkedin", "x.com", "instagram", "hobby", "interest", "posts", "activity", "community"],
        ),
        SectionTaskModel(
            section_id="legal_risk_history",
            title="Legal and risk history",
            objective="Public legal, court, crime, sanction, controversy, conflict, and uncertainty signals with caveats.",
            required=True,
            entity_ids=core_ids,
            query_hints=["crime", "arrest", "court", "lawsuit", "controversy", "conflict", "risk", "uncertain"],
        ),
    ]


def pick_primary_entities(
    mcp_client: McpClientProtocol,
    prompt: str,
    noteboard: List[str],
    receipts: List[ToolReceipt],
) -> List[str]:
    if not _env_flag("STAGE2_ENABLE_GRAPH_ENTITY_SEARCH", False):
        # Keep the default path cheap: use extracted hints directly instead of fuzzy graph scans.
        direct_candidates = dedupe_str_list(
            extract_person_targets(prompt)
            + extract_entity_hints_from_text(prompt)
            + [item for note in noteboard for item in (extract_person_targets(note) + extract_entity_hints_from_text(note))]
        )
        return direct_candidates[:4]

    candidates: List[str] = []
    for receipt in receipts:
        candidates.extend(extract_person_targets(receipt.summary))
        candidates.extend(extract_entity_hints_from_text(receipt.summary))
        for fact in receipt.key_facts:
            for value in fact.values():
                if isinstance(value, str):
                    candidates.extend(extract_person_targets(value))
                    candidates.extend(extract_entity_hints_from_text(value))

    candidates.extend(extract_person_targets(prompt))
    candidates.extend(extract_entity_hints_from_text(prompt))
    for note in noteboard:
        candidates.extend(extract_person_targets(note))
        candidates.extend(extract_entity_hints_from_text(note))
    candidates = dedupe_str_list(candidates)[:6]

    stable_ids: List[str] = []
    for query in candidates:
        result = mcp_client.call_tool("graph_search_entities", {"query": query})
        if not result.ok:
            continue
        entities = pick_list(result.content, ["entities", "results", "items"])
        for entity in entities:
            if not isinstance(entity, dict):
                continue
            entity_id = entity.get("entityId") or entity.get("id")
            if isinstance(entity_id, str) and entity_id:
                stable_ids.append(entity_id)

    return dedupe_str_list(stable_ids or candidates)


def graph_context_signals(mcp_client: McpClientProtocol, entity_ids: List[str]) -> tuple[List[str], List[str], List[str]]:
    if not _env_flag("STAGE2_ENABLE_GRAPH_CONTEXT", False):
        return ([], [], [])

    aliases: List[str] = []
    handles: List[str] = []
    domains: List[str] = []
    for entity_id in entity_ids[:2]:
        result = mcp_client.call_tool(
            "graph_neighbors",
            {"entityId": entity_id, "depth": 1},
        )
        if not result.ok:
            continue
        items = pick_list(result.content, ["neighbors", "entities", "items"])
        for item in items:
            if not isinstance(item, dict):
                continue
            for value in item.values():
                if isinstance(value, str):
                    aliases.extend(extract_aliases(value))
                    handles.extend(extract_handles(value))
                    domains.extend(extract_domains(value))
    return (dedupe_str_list(aliases), dedupe_str_list(handles), dedupe_str_list(domains))


def build_section_queries(task: SectionTaskModel, llm3: OpenRouterLLM | None) -> List[str]:
    base_queries = dedupe_str_list([task.title, task.objective] + task.query_hints + task.entity_ids)
    if llm3 is None:
        return base_queries[:3]

    payload = {
        "section": task.model_dump(),
        "base_queries": base_queries[:8],
        "output_schema": {"queries": ["string"]},
    }
    try:
        parsed = llm3.complete_json(
            "Generate compact OSINT retrieval query variants. Return JSON only.",
            payload,
            temperature=0.1,
            timeout=30,
        )
        queries = parsed.get("queries")
        if isinstance(queries, list):
            return dedupe_str_list([item for item in queries if isinstance(item, str)])[:3]
    except Exception:
        logger.exception("Stage 2 query variant generation failed")
    return base_queries[:3]


def vector_multi_query(
    mcp_client: McpClientProtocol,
    run_id: str,
    queries: List[str],
    k: int,
) -> List[Dict[str, Any]]:
    hits: List[Dict[str, Any]] = []
    for query in queries:
        result = mcp_client.call_tool(
            "vector_search",
            {"runId": run_id, "query": query, "k": k},
        )
        if not result.ok:
            continue
        rows = pick_list(result.content, ["results", "hits", "items"])
        for row in rows:
            if isinstance(row, dict):
                hits.append(row)
    deduped: Dict[str, Dict[str, Any]] = {}
    for row in hits:
        doc_id = str(row.get("document_id") or row.get("documentId") or "")
        snippet = str(row.get("snippet") or row.get("text") or "")
        key = f"{doc_id}|{snippet[:80]}"
        if key not in deduped:
            deduped[key] = row
    return list(deduped.values())


def pack_evidence(section_id: str, rows: List[Dict[str, Any]], k: int) -> List[EvidenceRefModel]:
    sorted_rows = sorted(
        rows,
        key=lambda item: float(item.get("score", 0.0) or 0.0),
        reverse=True,
    )[:k]
    packed: List[EvidenceRefModel] = []
    for idx, row in enumerate(sorted_rows, start=1):
        packed.append(
            EvidenceRefModel(
                citation_key=f"{section_id.upper()}_{idx}",
                section_id=section_id,
                document_id=str(row.get("document_id") or row.get("documentId") or "") or None,
                snippet=str(row.get("snippet") or row.get("text") or "").strip()[:500],
                source_url=pick_str(row, ["sourceUrl", "source_url", "url"]),
                title=pick_str(row, ["title", "document_title", "page_title"]),
                domain=_domain_from_url(pick_str(row, ["sourceUrl", "source_url", "url"])),
                retrieved_at=pick_str(row, ["retrievedAt", "retrieved_at", "created_at"]),
                content_hash=pick_str(row, ["contentHash", "content_hash", "sha256"]),
                source_type=_infer_source_type(row),
                score=float(row.get("score", 0.0) or 0.0),
                object_ref=pick_dict(row, ["objectRef", "object_ref"]),
            )
        )
    return packed


def fallback_claims(task: SectionTaskModel, evidence: List[EvidenceRefModel]) -> List[ClaimModel]:
    claims: List[ClaimModel] = []
    for idx, item in enumerate(evidence[:6], start=1):
        claims.append(
            ClaimModel(
                claim_id=f"{task.section_id}_c{idx}",
                section_id=task.section_id,
                text=f"Observed signal in {task.title}: {item.snippet[:200]}",
                subject_entity_id=(task.entity_ids[0] if task.entity_ids else None),
                predicate=_default_predicate_for_section(task.section_id),
                object=item.snippet[:200],
                confidence=0.5,
                impact="medium",
                evidence_keys=[item.citation_key],
                conflict_flags=[],
            )
        )
    return claims


def draft_section_content(
    task: SectionTaskModel,
    claims: List[ClaimModel],
    evidence: List[EvidenceRefModel],
    llm3: OpenRouterLLM | None,
) -> str:
    if llm3 is None:
        lines = [f"Section: {task.title}", f"Objective: {task.objective}"]
        for claim in claims[:8]:
            refs = ", ".join(claim.evidence_keys) if claim.evidence_keys else "NO_REF"
            lines.append(f"- {claim.text} [{refs}]")
        if not claims and evidence:
            for item in evidence[:5]:
                lines.append(f"- Evidence only: {item.snippet[:180]} [{item.citation_key}]")
        return "\n".join(lines).strip()

    payload = {
        "section": task.model_dump(),
        "claims": [item.model_dump() for item in claims],
        "evidence": [item.model_dump() for item in evidence],
        "output_schema": {"section_text": "string"},
    }
    try:
        parsed = llm3.complete_json(REPORT_SECTION_DRAFT_SYSTEM_PROMPT, payload, temperature=0.2, timeout=45)
        section_text = parsed.get("section_text")
        if isinstance(section_text, str) and section_text.strip():
            return section_text.strip()
    except Exception:
        logger.exception("Stage 2 section drafting failed")
    return draft_section_content(task, claims, evidence, llm3=None)


def latest_draft_per_section(drafts: List[SectionDraftModel], ordered_ids: List[str]) -> List[SectionDraftModel]:
    latest: Dict[str, SectionDraftModel] = {}
    for draft in drafts:
        latest[draft.section_id] = draft
    output: List[SectionDraftModel] = []
    for section_id in ordered_ids:
        if section_id in latest:
            output.append(latest[section_id])
    for section_id, draft in latest.items():
        if section_id not in ordered_ids:
            output.append(draft)
    return output


def dedupe_claims(claims: List[ClaimModel]) -> List[ClaimModel]:
    seen: Dict[str, ClaimModel] = {}
    for claim in claims:
        key = claim.claim_id or f"{claim.section_id}|{claim.text}"
        seen[key] = claim
    return list(seen.values())


def dedupe_evidence(items: List[EvidenceRefModel]) -> List[EvidenceRefModel]:
    seen: Dict[str, EvidenceRefModel] = {}
    for item in items:
        key = f"{item.content_hash or item.document_id}|{item.snippet[:100]}|{item.section_id}"
        seen[key] = item
    return list(seen.values())


def assemble_final_report(state: ReportState, llm3: OpenRouterLLM | None) -> str:
    report_memory = state.get("report_memory")
    drafts = state.get("section_drafts", [])
    if report_memory is None and not drafts:
        return "No report sections generated."
    if report_memory is None:
        report_memory = build_report_memory(
            question=state.get("prompt", ""),
            report_type=state.get("report_type", "person"),
            primary_entities=state.get("primary_entities", []),
            stage1_receipts=state.get("stage1_receipts", []),
            claims=state.get("claim_ledger", []),
            evidence=state.get("evidence_refs", []),
            section_issues=state.get("section_issues", []),
            section_drafts=drafts,
            latest_observation="",
        )

    ordered = _deterministic_report_text(report_memory)
    if llm3 is None:
        return ordered

    payload = {
        "report_type": state.get("report_type", "person"),
        "sections": [item.model_dump() for item in drafts],
        "quality_issues": state.get("section_issues", []),
        "report_memory": report_memory.model_dump(),
        "output_schema": {"report_text": "string"},
    }
    try:
        parsed = llm3.complete_json(
            FINAL_REPORT_ASSEMBLY_SYSTEM_PROMPT,
            payload,
            temperature=0.2,
            timeout=45,
        )
        report_text = parsed.get("report_text")
        if (
            isinstance(report_text, str)
            and report_text.strip()
            and "Findings" in report_text
            and "Evidence Index" in report_text
            and "Limits" in report_text
        ):
            return report_text.strip()
    except Exception:
        logger.exception("Final report assembly failed")
    return ordered


def assemble_evidence_appendix(items: List[EvidenceRefModel]) -> str:
    lines = ["Evidence Index"]
    for index, item in enumerate(items, start=1):
        domain = item.domain or _domain_from_url(item.source_url) or "-"
        src = item.source_url or "-"
        retrieved = item.retrieved_at or "unknown-date"
        lines.append(
            f"{index}. [{item.citation_key}] {domain} | {retrieved} | {src} | {item.snippet[:160]}"
        )
    return "\n".join(lines)


def build_report_memory(
    *,
    question: str,
    report_type: str,
    primary_entities: List[str],
    stage1_receipts: List[ToolReceipt],
    claims: List[ClaimModel],
    evidence: List[EvidenceRefModel],
    section_issues: List[str],
    section_drafts: List[SectionDraftModel],
    latest_observation: str,
) -> ReportMemoryModel:
    structured = _derive_structured_outputs(
        report_type=report_type,
        primary_entities=primary_entities,
        stage1_receipts=stage1_receipts,
        claims=claims,
        evidence=evidence,
        section_drafts=section_drafts,
    )
    entities = _build_entities(report_type, primary_entities, claims, evidence)
    coverage = build_coverage_ledger(report_type, claims, evidence, section_drafts, section_issues, structured)
    consistency_issues = run_consistency_validator(section_drafts, claims, evidence)
    limits = build_limits(section_issues, coverage, consistency_issues, structured["not_found_reasons"])
    open_questions = build_open_questions(coverage, consistency_issues)
    return ReportMemoryModel(
        question=question,
        entities=entities,
        claims=dedupe_claims([claim for claim in claims if claim.evidence_keys and claim.source_url]),
        evidence=dedupe_evidence(evidence),
        coverage=coverage,
        consistency_issues=consistency_issues,
        open_questions=open_questions,
        limits=limits,
        latest_observation=latest_observation[:500],
        step_count=max(len(section_drafts), len(claims), len(evidence)),
        canonical_identity=structured["canonical_identity"],
        disambiguation_evidence=structured["disambiguation_evidence"],
        attempt_log=structured["attempt_log"],
        not_found_reasons=structured["not_found_reasons"],
        timeline=structured["timeline"],
        publication_inventory=structured["publication_inventory"],
        thesis_inventory=structured["thesis_inventory"],
        research_themes=structured["research_themes"],
        collaboration_graph_nodes=structured["collaboration_graph_nodes"],
        collaboration_graph_edges=structured["collaboration_graph_edges"],
        coauthor_clusters=structured["coauthor_clusters"],
        profile_index=structured["profile_index"],
        doc_deep_dives=structured["doc_deep_dives"],
    )


def build_coverage_ledger(
    report_type: str,
    claims: List[ClaimModel],
    evidence: List[EvidenceRefModel],
    section_drafts: List[SectionDraftModel],
    section_issues: List[str],
    structured: Dict[str, Any] | None = None,
) -> CoverageLedgerModel:
    structured = structured or {}
    section_map = {draft.section_id: draft for draft in section_drafts}
    claim_sections = {claim.section_id for claim in claims if claim.evidence_keys}
    evidence_urls = [item.source_url or "" for item in evidence]
    evidence_blob = " ".join(
        [
            *[claim.text for claim in claims],
            *[draft.content for draft in section_drafts],
            *[item.snippet for item in evidence],
            *evidence_urls,
        ]
    ).lower()
    canonical_identity: CanonicalIdentityModel = structured.get("canonical_identity", CanonicalIdentityModel())
    timeline: List[TimelineEventModel] = structured.get("timeline", [])
    publication_inventory: List[PublicationInventoryItemModel] = structured.get("publication_inventory", [])
    thesis_inventory: List[ThesisInventoryItemModel] = structured.get("thesis_inventory", [])
    collaboration_graph_edges: List[CollaborationGraphEdgeModel] = structured.get("collaboration_graph_edges", [])
    profile_index: List[ProfileIndexItemModel] = structured.get("profile_index", [])
    doc_deep_dives: List[DocDeepDiveModel] = structured.get("doc_deep_dives", [])
    not_found_reasons: List[NotFoundReasonModel] = structured.get("not_found_reasons", [])

    def item(resolved: bool, confidence: float, note: str) -> CoverageItemModel:
        return CoverageItemModel(resolved=resolved, confidence=confidence if resolved else min(confidence, 0.49), notes=[note])

    has_identity = bool(canonical_identity.canonical_name) or "identity_profile" in claim_sections or "org_identity" in claim_sections
    has_affiliation = any(profile.affiliation for profile in profile_index) or any(dive.affiliations for dive in doc_deep_dives) or bool(re.search(r"\b(university|lab|department|company|inc|llc|organization|institute|works at|joined)\b", evidence_blob))
    has_education = any(re.search(r"\b(university|college|school|b\.?s\.?|m\.?s\.?|ph\.?d|degree|graduated)\b", event.event.lower()) for event in timeline) or bool(re.search(r"\b(university|college|school|b\.?s\.?|m\.?s\.?|ph\.?d|degree|graduated)\b", evidence_blob))
    has_publications = bool(publication_inventory or thesis_inventory) or "academic_research" in claim_sections or bool(re.search(r"\b(publication|paper|preprint|arxiv|dblp|pubmed|semantic scholar|orcid|coauthor)\b", evidence_blob))
    has_relationships = bool(collaboration_graph_edges) or "relationships_and_associates" in claim_sections or "org_people_and_relations" in claim_sections or bool(re.search(r"\b(coauthor|advisor|colleague|collaborator|lab|team|partner)\b", evidence_blob))
    has_handles = bool(profile_index) or any(domain in evidence_blob for domain in ("github.com", "gitlab.com", "linkedin.com", "x.com", "twitter.com")) or "social_accounts_and_interests" in claim_sections or "org_presence_and_assets" in claim_sections
    has_timeline = len(timeline) >= (2 if report_type == "person" else 1) or bool(re.search(r"\b(19|20)\d{2}\b", evidence_blob)) or "biography_history" in claim_sections or "org_activity_and_history" in claim_sections or "timeline_normalization" in section_map
    has_limits = True

    return CoverageLedgerModel(
        identity_resolved=item(has_identity, 0.9 if has_identity else 0.2, "Canonical identity resolved." if has_identity else "Identity evidence remains thin."),
        affiliations_resolved=item(has_affiliation, 0.76 if has_affiliation else 0.25, "Affiliation signals found." if has_affiliation else "No stable affiliation evidence found."),
        education_resolved=item(has_education, 0.72 if has_education else 0.2, "Education timeline markers found." if has_education else "Education remains unresolved."),
        publications_resolved=item(has_publications, 0.82 if has_publications else 0.15, "Publication/thesis inventory present." if has_publications else "No publication-grade evidence found."),
        relationships_resolved=item(has_relationships, 0.8 if has_relationships else 0.2, "Typed relationship graph present." if has_relationships else "Associates/collaborators remain unresolved."),
        handles_resolved=item(has_handles, 0.85 if has_handles else 0.2, "Profile index present." if has_handles else "No handle/profile baseline established."),
        timeline_resolved=item(has_timeline, 0.8 if has_timeline else 0.2, "Structured timeline present." if has_timeline else "Timeline remains incomplete."),
        limits_explained=item(has_limits, 0.9, "Limits classify unresolved gaps and contradictions." if not_found_reasons else "Limits will enumerate unresolved gaps and contradictions."),
    )


def run_consistency_validator(
    section_drafts: List[SectionDraftModel],
    claims: List[ClaimModel],
    evidence: List[EvidenceRefModel],
) -> List[ConsistencyIssueModel]:
    issues: List[ConsistencyIssueModel] = []
    drafts_blob = " ".join(draft.content for draft in section_drafts).lower()
    claims_blob = " ".join(claim.text for claim in claims).lower()
    evidence_blob = " ".join([item.snippet for item in evidence] + [item.source_url or "" for item in evidence]).lower()
    section_ids = [draft.section_id for draft in section_drafts]

    if ("no publications found" in drafts_blob or "no publications found" in claims_blob) and re.search(
        r"\b(coauthor|publication|paper|arxiv|dblp|pubmed|semantic scholar|orcid)\b",
        evidence_blob,
    ):
        issues.append(
            ConsistencyIssueModel(
                issue_id="publication_conflict",
                severity="high",
                description="Publication absence claim conflicts with publication/coauthor evidence.",
                conflicting_sections=section_ids,
                targeted_queries=["publication", "coauthor", "arxiv", "dblp", "semantic scholar"],
            )
        )

    if ("affiliation unknown" in drafts_blob or "affiliation unknown" in claims_blob) and re.search(
        r"\b(lab|department|institute|university|company|works at|joined)\b",
        evidence_blob,
    ):
        issues.append(
            ConsistencyIssueModel(
                issue_id="affiliation_conflict",
                severity="high",
                description="Affiliation unknown conflicts with affiliation-grade evidence.",
                conflicting_sections=section_ids,
                targeted_queries=["official profile", "lab page", "department", "company bio"],
            )
        )

    if ("no arxiv results" in drafts_blob or "no arxiv results" in claims_blob) and re.search(
        r"\b(citation|cited by|publication|paper|preprint)\b",
        evidence_blob,
    ):
        issues.append(
            ConsistencyIssueModel(
                issue_id="arxiv_conflict",
                severity="medium",
                description="No-arXiv statement conflicts with broader citation/publication evidence.",
                conflicting_sections=section_ids,
                targeted_queries=["arxiv", "preprint", "citation graph"],
            )
        )
    return issues


def build_limits(
    section_issues: List[str],
    coverage: CoverageLedgerModel,
    consistency_issues: List[ConsistencyIssueModel],
    not_found_reasons: List[NotFoundReasonModel] | None = None,
) -> List[str]:
    limits = list(section_issues)
    for field_name, item in coverage.model_dump().items():
        if not item["resolved"]:
            limits.append(f"{field_name}: unresolved or low-confidence.")
    for issue in consistency_issues:
        limits.append(f"{issue.description} Needs targeted follow-up.")
    for reason in not_found_reasons or []:
        limits.append(f"{reason.category}: {reason.detail}")
    if not limits:
        limits.append("No major unresolved gaps were detected in the current evidence set.")
    return dedupe_str_list(limits)


def build_open_questions(
    coverage: CoverageLedgerModel,
    consistency_issues: List[ConsistencyIssueModel],
) -> List[str]:
    questions: List[str] = []
    for field_name, item in coverage.model_dump().items():
        if not item["resolved"]:
            questions.append(field_name.replace("_", " "))
    for issue in consistency_issues:
        questions.append(issue.description)
    return dedupe_str_list(questions)


def coverage_is_complete(coverage: CoverageLedgerModel, report_type: str = "person") -> bool:
    if report_type == "org":
        required_fields = (
            "identity_resolved",
            "affiliations_resolved",
            "relationships_resolved",
            "handles_resolved",
            "timeline_resolved",
            "limits_explained",
        )
    else:
        required_fields = (
            "identity_resolved",
            "affiliations_resolved",
            "education_resolved",
            "publications_resolved",
            "relationships_resolved",
            "handles_resolved",
            "timeline_resolved",
            "limits_explained",
        )
    return all(getattr(coverage, field).resolved for field in required_fields)


def contradiction_query_hints(issues: List[ConsistencyIssueModel]) -> List[str]:
    hints: List[str] = []
    for issue in issues:
        hints.extend(issue.targeted_queries)
    return dedupe_str_list(hints)


def _deterministic_report_text(memory: ReportMemoryModel) -> str:
    lines = ["Findings"]
    if memory.claims:
        for claim in sorted(memory.claims, key=lambda item: item.confidence, reverse=True):
            confidence = f"{claim.confidence:.2f}"
            refs = ", ".join(claim.evidence_keys) if claim.evidence_keys else "no-evidence"
            lines.append(f"- [{confidence}] {claim.text} ({refs})")
    else:
        lines.append("- No evidence-backed findings were finalized.")

    lines.append("")
    lines.append("Canonical Identity")
    if memory.canonical_identity.canonical_name:
        lines.append(f"- canonical_name: {memory.canonical_identity.canonical_name}")
        lines.append(f"- aliases: {', '.join(memory.canonical_identity.aliases) if memory.canonical_identity.aliases else '-'}")
        lines.append(f"- low_social_footprint: {'yes' if memory.canonical_identity.low_social_footprint else 'no'}")
        lines.append(f"- high_academic_footprint: {'yes' if memory.canonical_identity.high_academic_footprint else 'no'}")
    else:
        lines.append("- Canonical identity not finalized.")

    lines.append("")
    lines.append("Timeline")
    if memory.timeline:
        for event in memory.timeline[:10]:
            refs = ", ".join(event.citation_keys) if event.citation_keys else "-"
            lines.append(f"- {event.date_label}: {event.event} [{refs}]")
    else:
        lines.append("- No structured timeline events extracted.")

    lines.append("")
    lines.append("Publication Inventory")
    if memory.publication_inventory:
        for publication in memory.publication_inventory[:10]:
            lines.append(f"- {publication.year or 'unknown'} | {publication.title} | {publication.venue or '-'}")
    else:
        lines.append("- No structured publication inventory extracted.")

    lines.append("")
    lines.append("Profile Index")
    if memory.profile_index:
        for profile in memory.profile_index[:10]:
            lines.append(f"- {profile.platform}: {profile.url} | last_active={profile.last_active or '-'}")
    else:
        lines.append("- No structured profile index extracted.")

    lines.append("")
    lines.append("Coverage Ledger")
    for name, item in memory.coverage.model_dump().items():
        status = "yes" if item["resolved"] else "no"
        notes = "; ".join(item["notes"]) if item["notes"] else ""
        lines.append(f"- {name}: {status} ({item['confidence']:.2f}) {notes}".rstrip())

    lines.append("")
    lines.append("Evidence Index")
    if memory.evidence:
        for index, item in enumerate(memory.evidence, start=1):
            domain = item.domain or _domain_from_url(item.source_url) or "-"
            when = item.retrieved_at or "unknown-date"
            why = _why_evidence_matters(item.citation_key, memory.claims)
            lines.append(f"{index}. [{item.citation_key}] {domain} | {when} | {item.source_url or '-'} | {why}")
    else:
        lines.append("1. No evidence captured.")

    lines.append("")
    lines.append("Limits")
    if memory.limits:
        for item in memory.limits:
            lines.append(f"- {item}")
    else:
        lines.append("- No explicit limits were recorded.")
    return "\n".join(lines).strip()


def _why_evidence_matters(citation_key: str, claims: List[ClaimModel]) -> str:
    linked = [claim.text for claim in claims if citation_key in claim.evidence_keys]
    if linked:
        return linked[0][:140]
    return "Supports source-backed section drafting."


def _derive_structured_outputs(
    *,
    report_type: str,
    primary_entities: List[str],
    stage1_receipts: List[ToolReceipt],
    claims: List[ClaimModel],
    evidence: List[EvidenceRefModel],
    section_drafts: List[SectionDraftModel],
) -> Dict[str, Any]:
    graph_nodes, graph_edges = _build_collaboration_graph(stage1_receipts)
    return {
        "canonical_identity": _build_canonical_identity(primary_entities, stage1_receipts),
        "disambiguation_evidence": _build_disambiguation_evidence(stage1_receipts),
        "attempt_log": _build_attempt_log(stage1_receipts),
        "not_found_reasons": _build_not_found_reasons(stage1_receipts),
        "timeline": _build_timeline(claims, evidence),
        "publication_inventory": _build_publication_inventory(stage1_receipts, evidence),
        "thesis_inventory": _build_thesis_inventory(stage1_receipts, evidence),
        "research_themes": _build_research_themes(stage1_receipts),
        "collaboration_graph_nodes": graph_nodes,
        "collaboration_graph_edges": graph_edges,
        "coauthor_clusters": _build_coauthor_clusters(stage1_receipts),
        "profile_index": _build_profile_index(stage1_receipts),
        "doc_deep_dives": _build_doc_deep_dives(stage1_receipts, evidence),
    }


def _build_canonical_identity(primary_entities: List[str], receipts: List[ToolReceipt]) -> CanonicalIdentityModel:
    canonical_name = primary_entities[0] if primary_entities else ""
    aliases: List[str] = []
    justifications: List[str] = []
    high_academic = False
    low_social = True
    for receipt in receipts:
        for fact in receipt.key_facts:
            if not isinstance(fact, dict):
                continue
            canonical = fact.get("canonical_identity")
            if isinstance(canonical, dict):
                candidate_name = str(canonical.get("canonical_name") or "").strip()
                if candidate_name and not canonical_name:
                    canonical_name = candidate_name
                aliases.extend([str(item).strip() for item in canonical.get("aliases", []) if str(item).strip()])
            for item in fact.get("disambiguation_evidence", []) if isinstance(fact.get("disambiguation_evidence"), list) else []:
                if isinstance(item, dict) and item.get("value"):
                    justifications.append(str(item["value"]).strip())
            if any(key in fact for key in ("candidates", "records")) and receipt.tool_name in {"orcid_search", "semantic_scholar_search", "dblp_author_search", "pubmed_author_search"}:
                high_academic = True
            if receipt.tool_name in {"github_identity_search", "gitlab_identity_search", "linkedin_download_html_ocr", "reddit_user_search", "mastodon_profile_search", "substack_author_search", "medium_author_search"}:
                low_social = False
    aliases = [item for item in dedupe_str_list(aliases) if item and item != canonical_name]
    if not canonical_name and aliases:
        canonical_name = aliases[0]
    return CanonicalIdentityModel(
        canonical_name=canonical_name,
        aliases=aliases,
        justification=dedupe_str_list(justifications)[:8],
        low_social_footprint=low_social,
        high_academic_footprint=high_academic,
    )


def _build_disambiguation_evidence(receipts: List[ToolReceipt]) -> List[DisambiguationEvidenceModel]:
    items: List[DisambiguationEvidenceModel] = []
    for receipt in receipts:
        for fact in receipt.key_facts:
            if not isinstance(fact, dict):
                continue
            entries = fact.get("disambiguation_evidence")
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                value = str(entry.get("value") or "").strip()
                if not value:
                    continue
                items.append(
                    DisambiguationEvidenceModel(
                        evidence_type=str(entry.get("type") or entry.get("evidence_type") or "signal"),
                        value=value,
                        strength=str(entry.get("strength") or "moderate"),
                    )
                )
    deduped: Dict[str, DisambiguationEvidenceModel] = {}
    for item in items:
        deduped[f"{item.evidence_type}|{item.value.lower()}"] = item
    return list(deduped.values())[:12]


def _build_attempt_log(receipts: List[ToolReceipt]) -> List[AttemptLogEntryModel]:
    logs: List[AttemptLogEntryModel] = []
    for receipt in receipts:
        query_parts = []
        for key, value in sorted((receipt.arguments or {}).items()):
            if key == "runId":
                continue
            if isinstance(value, (str, int, float)) and str(value).strip():
                query_parts.append(f"{key}={value}")
        sources: List[str] = []
        for fact in receipt.key_facts:
            if isinstance(fact, dict):
                if isinstance(fact.get("sourceUrls"), list):
                    sources.extend([str(item) for item in fact.get("sourceUrls", []) if str(item).strip()])
                if isinstance(fact.get("paperUrls"), list):
                    sources.extend([str(item) for item in fact.get("paperUrls", []) if str(item).strip()])
        logs.append(
            AttemptLogEntryModel(
                tool_name=receipt.tool_name,
                query=", ".join(query_parts),
                top_sources=dedupe_str_list(sources)[:5],
                outcome=receipt.summary,
                source_type=receipt.tool_type or "web",
            )
        )
    return logs[:30]


def _build_not_found_reasons(receipts: List[ToolReceipt]) -> List[NotFoundReasonModel]:
    reasons: List[NotFoundReasonModel] = []
    for receipt in receipts:
        summary = receipt.summary.lower()
        query = ", ".join(f"{key}={value}" for key, value in sorted((receipt.arguments or {}).items()) if key != "runId")
        if "404" in summary or "not found" in summary or "zero direct matches" in summary:
            reasons.append(NotFoundReasonModel(category="not_publicly_found", detail=receipt.summary, related_query=query or None))
        if "login" in summary or "auth" in summary:
            reasons.append(NotFoundReasonModel(category="auth_blocked", detail=receipt.summary, related_query=query or None))
        if "ambiguous" in summary:
            reasons.append(NotFoundReasonModel(category="ambiguous_identity", detail=receipt.summary, related_query=query or None))
        if "private" in summary:
            reasons.append(NotFoundReasonModel(category="private", detail=receipt.summary, related_query=query or None))
    deduped: Dict[str, NotFoundReasonModel] = {}
    for reason in reasons:
        deduped[f"{reason.category}|{reason.detail}"] = reason
    return list(deduped.values())[:20]


def _build_timeline(claims: List[ClaimModel], evidence: List[EvidenceRefModel]) -> List[TimelineEventModel]:
    events: List[TimelineEventModel] = []
    citation_map = {item.citation_key: item for item in evidence}
    for claim in claims:
        if not claim.source_url:
            continue
        years = re.findall(r"\b(?:19|20)\d{2}\b", claim.text)
        if not years:
            continue
        source = citation_map.get(claim.evidence_keys[0]) if claim.evidence_keys else None
        events.append(
            TimelineEventModel(
                date_label=years[0],
                event=claim.text[:220],
                citation_keys=claim.evidence_keys[:3],
                source_url=source.source_url if source else claim.source_url,
            )
        )
    deduped: Dict[str, TimelineEventModel] = {}
    for event in events:
        deduped[f"{event.date_label}|{event.event[:80]}"] = event
    ordered = sorted(deduped.values(), key=lambda item: item.date_label)
    return ordered[:12]


def _build_publication_inventory(receipts: List[ToolReceipt], evidence: List[EvidenceRefModel]) -> List[PublicationInventoryItemModel]:
    publications: Dict[str, PublicationInventoryItemModel] = {}
    for receipt in receipts:
        for fact in receipt.key_facts:
            if not isinstance(fact, dict):
                continue
            for key in ("publications", "records", "papers", "extracted_entries"):
                values = fact.get(key)
                if not isinstance(values, list):
                    continue
                for item in values:
                    if not isinstance(item, dict):
                        continue
                    title = str(item.get("title") or item.get("name") or "").strip()
                    if not title:
                        continue
                    author_values = item.get("authors") or item.get("coauthors") or item.get("author_names") or []
                    coauthors: List[str] = []
                    if isinstance(author_values, list):
                        for author in author_values:
                            if isinstance(author, str) and author.strip():
                                coauthors.append(author.strip())
                            elif isinstance(author, dict) and isinstance(author.get("name"), str):
                                coauthors.append(str(author["name"]).strip())
                    links = [str(item.get(link_key)).strip() for link_key in ("url", "pdf_url", "ee") if isinstance(item.get(link_key), str) and str(item.get(link_key)).strip()]
                    publications[title.lower()] = PublicationInventoryItemModel(
                        title=title,
                        year=str(item.get("year") or "").strip() or None,
                        venue=str(item.get("venue") or item.get("journal") or item.get("conference") or "").strip() or None,
                        coauthors=dedupe_str_list(coauthors)[:12],
                        links=dedupe_str_list(links)[:5],
                    )
    return list(publications.values())[:25]


def _build_thesis_inventory(receipts: List[ToolReceipt], evidence: List[EvidenceRefModel]) -> List[ThesisInventoryItemModel]:
    theses: Dict[str, ThesisInventoryItemModel] = {}
    for item in evidence:
        blob = f"{item.title or ''} {item.snippet} {item.source_url or ''}".lower()
        if "thesis" not in blob and "dissertation" not in blob:
            continue
        title = (item.title or item.snippet[:100]).strip()
        theses[title.lower()] = ThesisInventoryItemModel(
            title=title,
            pdf_url=item.source_url if item.source_url and item.source_url.lower().endswith(".pdf") else item.source_url,
        )
    for receipt in receipts:
        for fact in receipt.key_facts:
            if not isinstance(fact, dict):
                continue
            advisors = fact.get("advisors") if isinstance(fact.get("advisors"), list) else []
            for thesis in theses.values():
                if advisors and not thesis.advisor:
                    thesis.advisor = str(advisors[0]).strip()
    return list(theses.values())[:10]


def _build_research_themes(receipts: List[ToolReceipt]) -> List[ResearchThemeModel]:
    counts: Dict[str, int] = {}
    for receipt in receipts:
        for fact in receipt.key_facts:
            if not isinstance(fact, dict):
                continue
            for key in ("topics", "keywords", "methods_keywords", "abstract_keywords"):
                values = fact.get(key)
                if not isinstance(values, list):
                    continue
                for value in values:
                    if not isinstance(value, str) or not value.strip():
                        continue
                    token = value.strip()
                    counts[token] = counts.get(token, 0) + 1
    return [ResearchThemeModel(theme=theme, evidence_count=count) for theme, count in sorted(counts.items(), key=lambda item: (-item[1], item[0].lower()))[:12]]


def _build_collaboration_graph(receipts: List[ToolReceipt]) -> tuple[List[CollaborationGraphNodeModel], List[CollaborationGraphEdgeModel]]:
    nodes: Dict[str, CollaborationGraphNodeModel] = {}
    edges: Dict[str, CollaborationGraphEdgeModel] = {}
    for receipt in receipts:
        for fact in receipt.key_facts:
            if not isinstance(fact, dict):
                continue
            graph = fact.get("collaborationGraph")
            if isinstance(graph, dict):
                for node in graph.get("nodes", []):
                    if isinstance(node, dict):
                        node_id = str(node.get("id") or "").strip()
                        if node_id:
                            nodes[node_id] = CollaborationGraphNodeModel(node_id=node_id, node_type=str(node.get("type") or "Person"), label=str(node.get("label") or node_id))
                for edge in graph.get("edges", []):
                    if isinstance(edge, dict):
                        key = f"{edge.get('src')}|{edge.get('rel')}|{edge.get('dst')}"
                        if key not in edges:
                            edges[key] = CollaborationGraphEdgeModel(
                                src=str(edge.get("src") or ""),
                                rel=str(edge.get("rel") or "COAUTHOR_OF"),
                                dst=str(edge.get("dst") or ""),
                                evidence_count=int(edge.get("count") or 1),
                            )
            coauthors = fact.get("coauthors")
            if isinstance(coauthors, list):
                for coauthor in coauthors:
                    if isinstance(coauthor, dict) and isinstance(coauthor.get("name"), str):
                        node_id = coauthor["name"].strip()
                        nodes[node_id] = CollaborationGraphNodeModel(node_id=node_id, node_type="Person", label=node_id)
    return list(nodes.values())[:80], list(edges.values())[:120]


def _build_coauthor_clusters(receipts: List[ToolReceipt]) -> List[CoauthorClusterModel]:
    clusters: List[CoauthorClusterModel] = []
    for receipt in receipts:
        for fact in receipt.key_facts:
            if not isinstance(fact, dict):
                continue
            values = fact.get("clusters")
            if not isinstance(values, list):
                continue
            for item in values:
                if not isinstance(item, dict):
                    continue
                clusters.append(
                    CoauthorClusterModel(
                        label=str(item.get("label") or "cluster"),
                        members=[str(member).strip() for member in item.get("members", []) if str(member).strip()],
                        representative_works=[str(work).strip() for work in item.get("representative_works", []) if str(work).strip()],
                    )
                )
    return clusters[:10]


def _build_profile_index(receipts: List[ToolReceipt]) -> List[ProfileIndexItemModel]:
    profiles: Dict[str, ProfileIndexItemModel] = {}
    for receipt in receipts:
        result_platform = receipt.tool_name.replace("_search", "").replace("_download_html_ocr", "")
        for fact in receipt.key_facts:
            if not isinstance(fact, dict):
                continue
            url = ""
            for key in ("profileUrl", "profile_url", "url"):
                value = fact.get(key)
                if isinstance(value, str) and value.strip().startswith(("http://", "https://")):
                    url = value.strip()
                    break
            if not url:
                continue
            profiles[url] = ProfileIndexItemModel(
                platform=result_platform,
                url=url,
                last_active=str(fact.get("lastActive") or fact.get("last_active") or "").strip() or None,
                title=str(fact.get("title") or "").strip() or None,
                affiliation=str(fact.get("affiliation") or fact.get("organization") or "").strip() or None,
                projects=[str(item).strip() for item in fact.get("projects", []) if str(item).strip()] if isinstance(fact.get("projects"), list) else [],
                pinned_items=[str(item).strip() for item in fact.get("repositories", []) if isinstance(item, str) and str(item).strip()] if isinstance(fact.get("repositories"), list) else [],
            )
    return list(profiles.values())[:20]


def _build_doc_deep_dives(receipts: List[ToolReceipt], evidence: List[EvidenceRefModel]) -> List[DocDeepDiveModel]:
    dives: Dict[str, DocDeepDiveModel] = {}
    for item in evidence:
        if item.source_type != "file" and ".pdf" not in (item.source_url or "").lower():
            continue
        blob = f"{item.title or ''} {item.snippet}".lower()
        methods = [token for token in re.findall(r"\b[a-z][a-z-]{4,}\b", blob) if token in {"audio", "music", "nlp", "speech", "learning", "research", "workshops"}]
        dives[item.source_url or item.citation_key] = DocDeepDiveModel(
            source_url=item.source_url or item.citation_key,
            affiliations=[],
            advisors=[],
            methods_keywords=dedupe_str_list(methods)[:8],
            acknowledgements=[item.snippet[:200]] if "acknowledg" in blob else [],
        )
    for receipt in receipts:
        for fact in receipt.key_facts:
            if not isinstance(fact, dict):
                continue
            urls = fact.get("paperUrls") if isinstance(fact.get("paperUrls"), list) else []
            affiliations = [str(item).strip() for item in fact.get("affiliations", []) if str(item).strip()] if isinstance(fact.get("affiliations"), list) else []
            for url in urls:
                if url in dives:
                    dives[url].affiliations = dedupe_str_list(dives[url].affiliations + affiliations)[:8]
    return list(dives.values())[:12]


def _build_entities(
    report_type: str,
    primary_entities: List[str],
    claims: List[ClaimModel],
    evidence: List[EvidenceRefModel],
) -> List[EntityModel]:
    entity_type = "Organization" if report_type == "org" else "Person"
    entities: List[EntityModel] = [
        EntityModel(entity_id=item, entity_type=entity_type, name=item, confidence=0.8)
        for item in primary_entities[:4]
    ]
    seen = {item.entity_id for item in entities}
    for claim in claims:
        if claim.subject_entity_id and claim.subject_entity_id not in seen:
            entities.append(
                EntityModel(
                    entity_id=claim.subject_entity_id,
                    entity_type=entity_type,
                    name=claim.subject_entity_id,
                    confidence=max(0.5, claim.confidence),
                )
            )
            seen.add(claim.subject_entity_id)
    for item in evidence:
        if item.source_url:
            domain = _domain_from_url(item.source_url)
            if domain and domain not in seen:
                entities.append(
                    EntityModel(
                        entity_id=domain,
                        entity_type="WebProfile",
                        name=domain,
                        attributes={"url": item.source_url},
                        confidence=0.5,
                    )
                )
                seen.add(domain)
    return entities


def extract_entity_hints_from_text(text: str) -> List[str]:
    out: List[str] = []
    out.extend(extract_domains(text))
    out.extend(extract_handles(text))
    out.extend(extract_urls(text))
    return dedupe_str_list(out)


def extract_urls(text: str) -> List[str]:
    return re.findall(r"https?://[^\s\]]+", text or "")


def extract_handles(text: str) -> List[str]:
    return [f"@{item}" for item in re.findall(r"(?<!\w)@([A-Za-z0-9_]{3,32})", text or "")]


def extract_aliases(text: str) -> List[str]:
    chunks = re.findall(r"\b[A-Za-z][A-Za-z0-9_.-]{2,}\b", text or "")
    return [item for item in chunks if not item.startswith("http")]


def extract_domains(text: str) -> List[str]:
    return re.findall(r"\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}\b", text or "", re.IGNORECASE)


def needs_timeline_normalization(notes: List[str]) -> bool:
    text = " ".join(notes).lower()
    return "timeline" in text or "date" in text


def needs_conflict_resolution(notes: List[str], claims: List[ClaimModel]) -> bool:
    if any(claim.conflict_flags for claim in claims):
        return True
    text = " ".join(notes).lower()
    return "conflict" in text or "contradiction" in text


def dedupe_str_list(items: List[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for item in items:
        value = item.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def pick_list(payload: Dict[str, Any], keys: List[str]) -> List[Any]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return []


def pick_dict(payload: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, dict):
            return value
    return {}


def pick_str(payload: Dict[str, Any], keys: List[str]) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _domain_from_url(url: str | None) -> str | None:
    if not url:
        return None
    try:
        return urlparse(url).netloc.lower() or None
    except Exception:
        return None


def _infer_source_type(row: Dict[str, Any]) -> str:
    blob = " ".join(str(value) for value in row.values()).lower()
    if ".pdf" in blob or "pdf" in blob:
        return "file"
    if any(token in blob for token in ("arxiv", "dblp", "pubmed", "orcid", "semantic scholar")):
        return "scholar"
    return "web"


def _default_predicate_for_section(section_id: str) -> str:
    mapping = {
        "identity_profile": "identity_signal",
        "org_identity": "identity_signal",
        "biography_history": "timeline_signal",
        "org_activity_and_history": "timeline_signal",
        "academic_research": "publication_signal",
        "relationships_and_associates": "relationship_signal",
        "org_people_and_relations": "relationship_signal",
        "social_accounts_and_interests": "handle_signal",
        "org_presence_and_assets": "handle_signal",
    }
    return mapping.get(section_id, "observed")
