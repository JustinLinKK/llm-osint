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

from openrouter_llm import get_openrouter_timeout, invoke_complete_json

logger = get_logger(__name__)
REPORT_RELATED_PERSON_REJECT_TOKENS = {
    "none",
    "null",
    "unknown",
    "na",
    "n/a",
    "publication",
    "publications",
    "record",
    "records",
    "result",
    "results",
    "source",
    "sources",
    "search",
    "research",
    "profile",
    "profiles",
    "candidate",
    "candidates",
    "tavily",
    "google",
    "serp",
    "duckduckgo",
    "github",
    "gitlab",
    "linkedin",
}
REPORT_PROVIDER_BLOCKLIST = {
    "tavily",
    "google",
    "duckduckgo",
    "wikipedia",
    "researchgate",
    "linkedin",
    "github",
    "gitlab",
}
REPORT_ORG_GENERIC_TOKENS = {
    "search",
    "research",
    "result",
    "results",
    "source",
    "sources",
    "profile",
    "profiles",
    "person",
    "people",
    "public",
    "web",
}
REPORT_ORG_DESCRIPTOR_TERMS = {
    "startup",
    "stealth startup",
    "stealth company",
    "stealth mode",
    "self-employed",
    "self employed",
    "independent",
    "confidential",
}


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _report_final_timeout() -> float:
    return get_openrouter_timeout(
        "OPENROUTER_REPORT_TIMEOUT_SECONDS",
        get_openrouter_timeout(
            "OPENROUTER_PLANNER_TIMEOUT_SECONDS",
            get_openrouter_timeout("OPENROUTER_TIMEOUT_SECONDS", 400.0),
        ),
    )


def _report_worker_timeout() -> float:
    return get_openrouter_timeout(
        "OPENROUTER_REPORT_WORKER_TIMEOUT_SECONDS",
        get_openrouter_timeout(
            "OPENROUTER_WORKER_TIMEOUT_SECONDS",
            get_openrouter_timeout("OPENROUTER_TIMEOUT_SECONDS", 400.0),
        ),
    )


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


def _section_graph_profile(report_type: str, section_id: str) -> tuple[str, List[str]]:
    if report_type == "org":
        mapping = {
            "org_identity": ("Identity", ["Organization", "Official Profile", "Identifiers", "Aliases"]),
            "org_people_and_relations": ("People", ["Organization", "Role/Team", "Person", "External Relationship"]),
            "org_presence_and_assets": ("Technical", ["Organization", "Domain/Profile", "Repository/Asset", "Topic"]),
            "org_activity_and_history": ("Timeline", ["Organization", "TimelineEvent", "Document/Announcement", "Impact"]),
            "org_timeline": ("Timeline", ["Organization", "TimelineEvent", "Role/Document", "Date"]),
            "org_source_documents": ("Documents", ["Organization", "Document", "Institution/Registry", "TimelineEvent"]),
            "org_risks_uncertainty": ("Risk", ["Organization", "Document/Issue", "TimelineEvent", "Uncertainty"]),
            "methodological_limits": ("Limits", ["Primary Subject", "Missing Branch", "Blocked Pivot", "Why unresolved"]),
        }
    else:
        mapping = {
            "identity_profile": ("Identity", ["Person", "ContactPoint/Profile", "Handle/Domain", "Location"]),
            "biography_history": ("Work", ["Person", "Experience/EducationalCredential", "Organization/Institution", "TimelineEvent"]),
            "timeline_normalization": ("Timeline", ["Person", "TimelineEvent", "Experience/Affiliation/Credential", "Organization/Publication"]),
            "academic_research": ("Research", ["Person", "Publication", "Conference/Grant/Patent", "Topic"]),
            "code_software_footprint": ("Technical", ["Person", "Repository/Project", "Organization", "Topic"]),
            "public_contact_methods": ("Contacts", ["Person", "ContactPoint", "Email/Phone/Profile", "Domain/Location"]),
            "relationships_and_associates": ("People", ["Person", "Affiliation/Experience/Publication", "Person/Organization", "Why it matters"]),
            "collaboration_clusters": ("People", ["Person", "Publication", "Coauthor Cluster", "Institution"]),
            "source_documents": ("Documents", ["Person", "Document", "Organization/Institution", "TimelineEvent"]),
            "social_accounts_and_interests": ("Contacts", ["Person", "Handle/Profile", "Topic/Community", "Organization"]),
            "legal_risk_history": ("Risk", ["Person", "Document/Organization", "TimelineEvent", "Uncertainty"]),
            "methodological_limits": ("Limits", ["Primary Subject", "Missing Branch", "Blocked Pivot", "Why unresolved"]),
        }
    return mapping.get(section_id, ("Other", ["Primary Subject", "Related Entity", "Evidence", "Uncertainty"]))


def default_outline(report_type: str, primary_entities: List[str]) -> List[SectionTaskModel]:
    core_ids = primary_entities[:4]
    def make_task(
        *,
        section_id: str,
        title: str,
        objective: str,
        required: bool,
        query_hints: List[str],
    ) -> SectionTaskModel:
        section_group, graph_chain = _section_graph_profile(report_type, section_id)
        return SectionTaskModel(
            section_id=section_id,
            title=title,
            objective=objective,
            required=required,
            section_group=section_group,
            graph_chain=graph_chain,
            entity_ids=core_ids,
            query_hints=query_hints,
        )

    if report_type == "org":
        return [
            make_task(
                section_id="org_identity",
                title="Organization identity",
                objective="Legal/brand names, aliases, ownership clues, registration and official properties.",
                required=True,
                query_hints=["legal name", "registry", "about", "official website"],
            ),
            make_task(
                section_id="org_people_and_relations",
                title="People and relationships",
                objective="Leadership, employees, partners, subsidiaries, and notable external relationships.",
                required=True,
                query_hints=["founder", "CEO", "team", "partners", "subsidiary"],
            ),
            make_task(
                section_id="org_presence_and_assets",
                title="Presence and assets",
                objective="Domains, infrastructure, social media accounts, public channels, and digital footprint.",
                required=True,
                query_hints=["domain", "subdomain", "linkedin", "x.com", "github"],
            ),
            make_task(
                section_id="org_activity_and_history",
                title="Activity and history",
                objective="Timeline of announcements, products, incidents, events, hiring, and public milestones.",
                required=True,
                query_hints=["news", "press release", "launch", "timeline", "history"],
            ),
            make_task(
                section_id="org_timeline",
                title="Timeline",
                objective="Render a dated chronology of corporate milestones, leadership changes, filings, launches, and archived changes.",
                required=True,
                query_hints=["timeline", "year", "filing date", "history", "milestone"],
            ),
            make_task(
                section_id="org_source_documents",
                title="Source documents",
                objective="Highlight official documents, filings, archived pages, PDFs, and primary-source pages that anchor the organization profile.",
                required=False,
                query_hints=["pdf", "filing", "official document", "archive", "about page"],
            ),
            make_task(
                section_id="org_risks_uncertainty",
                title="Risks and uncertainties",
                objective="Conflicts, unresolved claims, legal/regulatory signals, and confidence caveats.",
                required=True,
                query_hints=["lawsuit", "violation", "complaint", "controversy", "risk"],
            ),
            make_task(
                section_id="methodological_limits",
                title="Methodological limits",
                objective="State unresolved coverage gaps, blocked pivots, ambiguous identities, and why some deterministic follow-ups could not be completed.",
                required=True,
                query_hints=["limitations", "uncertainty", "not found", "ambiguous", "archive"],
            ),
        ]
    return [
        make_task(
            section_id="identity_profile",
            title="Identity profile",
            objective="Name variants, identifiers, known aliases, demographics clues, and core biographic markers.",
            required=True,
            query_hints=["full name", "alias", "bio", "about", "profile"],
        ),
        make_task(
            section_id="biography_history",
            title="Biography and history",
            objective="Educational, academic, employment, publication, and historical milestones with dates and institutions.",
            required=True,
            query_hints=["education", "academic history", "employment history", "publication", "research", "timeline"],
        ),
        make_task(
            section_id="timeline_normalization",
            title="Timeline",
            objective="Render a chronological sequence of dated milestones across identity, education, employment, publications, archives, and business roles.",
            required=True,
            query_hints=["timeline", "year", "date", "joined", "graduated", "published"],
        ),
        make_task(
            section_id="academic_research",
            title="Academic / Research",
            objective="Resolved academic identities, ORCID/DBLP/Semantic Scholar/PubMed profiles, publication patterns, grants, patents, venues, and traceable evidence.",
            required=False,
            query_hints=["orcid", "semantic scholar", "dblp", "pubmed", "grant", "patent", "conference", "publication count", "evidence url"],
        ),
        make_task(
            section_id="code_software_footprint",
            title="Code / Software Footprint",
            objective="Public GitHub and personal-site signals, repository footprint, org memberships, package/container publishing clues, and technical affiliation evidence.",
            required=False,
            query_hints=["github", "gitlab", "hugging face", "personal site", "repository", "npm", "pypi", "crates", "docker hub"],
        ),
        make_task(
            section_id="public_contact_methods",
            title="Public contact methods",
            objective="Publicly available emails, phone numbers, websites, contact pages, directories, and location/contact signals.",
            required=True,
            query_hints=["address", "location", "city", "email", "phone"],
        ),
        make_task(
            section_id="relationships_and_associates",
            title="Relationships and associates",
            objective="Related people, colleagues, co-authors, advisors, employers, collaborators, and publicly visible relationship types.",
            required=True,
            query_hints=["co-author", "advisor", "colleague", "works at", "lab", "department", "network"],
        ),
        make_task(
            section_id="collaboration_clusters",
            title="Collaboration clusters",
            objective="Describe collaborator groupings, repeated coauthor clusters, labs, departments, and how those clusters relate to the primary target.",
            required=False,
            query_hints=["coauthor cluster", "collaboration group", "lab", "advisor", "coauthor graph"],
        ),
        make_task(
            section_id="source_documents",
            title="Source documents",
            objective="Highlight official documents, theses, archived pages, PDFs, and primary-source records that anchor the profile.",
            required=False,
            query_hints=["pdf", "thesis", "dissertation", "cv", "official page", "archive"],
        ),
        make_task(
            section_id="social_accounts_and_interests",
            title="Social accounts and interests",
            objective="Social media accounts, recurring topics, hobbies, interests, affiliations, and public behavior patterns from posts/content.",
            required=True,
            query_hints=["linkedin", "x.com", "instagram", "hobby", "interest", "posts", "activity", "community"],
        ),
        make_task(
            section_id="legal_risk_history",
            title="Legal and risk history",
            objective="Public legal, court, crime, sanction, controversy, conflict, and uncertainty signals with caveats.",
            required=True,
            query_hints=["crime", "arrest", "court", "lawsuit", "controversy", "conflict", "risk", "uncertain"],
        ),
        make_task(
            section_id="methodological_limits",
            title="Methodological limits",
            objective="State unresolved coverage gaps, ambiguous pivots, negative searches, and why specific follow-up chains were not completed.",
            required=True,
            query_hints=["limitations", "uncertainty", "not found", "ambiguous", "blocked"],
        ),
    ]


def pick_primary_entities(
    mcp_client: McpClientProtocol,
    run_id: str,
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
        result = mcp_client.call_tool(
            "graph_search_entities",
            {"runId": run_id, "scope": "run", "query": query},
        )
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


def graph_context_signals(
    mcp_client: McpClientProtocol,
    run_id: str,
    entity_ids: List[str],
) -> tuple[List[str], List[str], List[str]]:
    if not _env_flag("STAGE2_ENABLE_GRAPH_CONTEXT", False):
        return ([], [], [])

    aliases: List[str] = []
    handles: List[str] = []
    domains: List[str] = []
    for entity_id in entity_ids[:2]:
        result = mcp_client.call_tool(
            "graph_neighbors",
            {"runId": run_id, "scope": "run", "entityId": entity_id, "depth": 1},
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


def build_section_queries(task: SectionTaskModel, llm3: OpenRouterLLM | None, run_id: str | None = None) -> List[str]:
    base_queries = dedupe_str_list(
        [task.title, task.objective, task.section_group, task.revision_focus, task.next_step_suggestion, " -> ".join(task.graph_chain)]
        + task.query_hints
        + task.entity_ids
        + task.graph_chain
    )
    if llm3 is None:
        return base_queries[:3]

    payload = {
        "section": task.model_dump(),
        "base_queries": base_queries[:8],
        "output_schema": {"queries": ["string"]},
    }
    try:
        parsed = invoke_complete_json(
            llm3,
            "Generate compact OSINT retrieval query variants. Return JSON only.",
            payload,
            temperature=0.1,
            timeout=_report_worker_timeout(),
            run_id=run_id,
            operation="stage2.query_variants",
            metadata={"sectionId": task.section_id},
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
                hits.append({**row, "db_source": "vector"})
    deduped: Dict[str, Dict[str, Any]] = {}
    for row in hits:
        doc_id = str(row.get("document_id") or row.get("documentId") or "")
        snippet = str(row.get("snippet") or row.get("text") or "")
        key = f"{doc_id}|{snippet[:80]}"
        if key not in deduped:
            deduped[key] = row
    return list(deduped.values())


def graph_multi_entity_query(
    mcp_client: McpClientProtocol,
    run_id: str,
    entity_ids: List[str],
    neighbor_depth: int = 1,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for entity_id in dedupe_str_list(entity_ids)[:4]:
        entity_result = mcp_client.call_tool(
            "graph_get_entity",
            {"runId": run_id, "scope": "run", "entityId": entity_id},
        )
        if entity_result.ok and isinstance(entity_result.content, dict):
            props = pick_dict(entity_result.content, ["properties", "props"])
            labels = pick_list(entity_result.content, ["labels"])
            row = _graph_row_from_entity_payload(entity_id, props, labels, score=1.0)
            if row:
                rows.append(row)

        neighbor_result = mcp_client.call_tool(
            "graph_neighbors",
            {"runId": run_id, "scope": "run", "entityId": entity_id, "depth": neighbor_depth},
        )
        if not neighbor_result.ok or not isinstance(neighbor_result.content, dict):
            continue
        neighbors = pick_list(neighbor_result.content, ["neighbors", "entities", "items"])
        for neighbor in neighbors:
            if not isinstance(neighbor, dict):
                continue
            props = pick_dict(neighbor, ["properties", "props"])
            labels = pick_list(neighbor, ["labels"])
            rel_types = pick_list(neighbor, ["relTypes", "rel_types"])
            row = _graph_row_from_entity_payload(
                pick_str(props, ["node_id", "person_id", "org_id", "location_id", "address", "uri", "name"]) or entity_id,
                props,
                labels,
                score=0.7,
                rel_types=[str(item) for item in rel_types if isinstance(item, str)],
            )
            if row:
                rows.append(row)

    deduped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = f"{row.get('graph_entity_id') or row.get('document_id') or ''}|{str(row.get('snippet') or '')[:120]}"
        if key not in deduped:
            deduped[key] = row
    return list(deduped.values())


PLACEHOLDER_EMAIL_RE = re.compile(r"error-[^@\s]+@duckduckgo\.com", re.IGNORECASE)


def _sanitize_snippet(snippet: str) -> str:
    text = snippet or ""
    if not text:
        return ""
    text = PLACEHOLDER_EMAIL_RE.sub("[placeholder-email]", text)
    # Filter known placeholder URLs from leaking into report citations.
    text = text.replace("https://hi", "[invalid-url]")
    return text


def _looks_like_tool_invocation_json(snippet: str) -> bool:
    compact = (snippet or "").lstrip()
    if not compact.startswith("{"):
        return False
    lowered = compact[:400].lower()
    return '"tool"' in lowered and '"arguments"' in lowered


def _is_valid_public_source_url(url: str) -> bool:
    candidate = (url or "").strip()
    if not candidate.startswith(("http://", "https://")):
        return False
    try:
        domain = urlparse(candidate).netloc.lower()
    except Exception:
        return False
    if not domain or "." not in domain:
        return False
    if domain.startswith("localhost") or domain.startswith("127.0.0.1"):
        return False
    return True


def _tokenize_relevance_terms(text: str) -> List[str]:
    lowered = str(text or "").lower()
    tokens = re.findall(r"[a-z0-9][a-z0-9._-]{1,}", lowered)
    stopwords = {
        "about",
        "from",
        "with",
        "this",
        "that",
        "into",
        "where",
        "which",
        "their",
        "while",
        "when",
        "section",
        "report",
        "analysis",
        "objective",
        "profile",
    }
    return [token for token in tokens if token not in stopwords and len(token) >= 3]


def _evidence_relevance_score(
    section_context: SectionTaskModel | None,
    snippet: str,
    source_url: str | None,
    title: str | None,
) -> float:
    if section_context is None:
        return 1.0
    context_blob = " ".join(
        [
            section_context.title,
            section_context.objective,
            section_context.section_group,
            " ".join(section_context.graph_chain),
            " ".join(section_context.query_hints),
            " ".join(section_context.entity_ids),
        ]
    )
    context_tokens = set(_tokenize_relevance_terms(context_blob))
    if not context_tokens:
        return 1.0
    evidence_blob = " ".join([snippet or "", source_url or "", title or ""])
    evidence_tokens = set(_tokenize_relevance_terms(evidence_blob))
    if not evidence_tokens:
        return 0.0
    overlap = context_tokens & evidence_tokens
    overlap_ratio = len(overlap) / max(1, len(context_tokens))
    density = len(overlap) / max(1, len(evidence_tokens))
    return min(1.0, overlap_ratio * 0.75 + density * 0.25)


def _include_in_evidence_appendix(item: EvidenceRefModel) -> bool:
    if _looks_like_tool_invocation_json(item.snippet or ""):
        return False
    if item.source_url:
        return _is_valid_public_source_url(item.source_url)
    return bool(item.document_id or item.object_ref)


def pack_evidence(
    section_id: str,
    rows: List[Dict[str, Any]],
    k: int,
    section_context: SectionTaskModel | None = None,
) -> List[EvidenceRefModel]:
    sorted_rows = sorted(
        [row for row in rows if _row_has_database_evidence(row)],
        key=lambda item: float(item.get("score", 0.0) or 0.0),
        reverse=True,
    )[:k]
    packed: List[EvidenceRefModel] = []
    for idx, row in enumerate(sorted_rows, start=1):
        source_url = pick_str(row, ["sourceUrl", "source_url", "url"])
        metadata = pick_dict(row, ["metadata", "payload"])
        if not source_url and metadata:
            source_url = pick_str(metadata, ["sourceUrl", "source_url", "url"])
        snippet = _sanitize_snippet(str(row.get("snippet") or row.get("text") or "").strip()[:500])
        if _looks_like_tool_invocation_json(snippet):
            continue
        if not source_url:
            source_url = _first_url_in_text(snippet)
        if source_url and not _is_valid_public_source_url(source_url):
            source_url = None
        document_id = str(row.get("document_id") or row.get("documentId") or "") or None
        object_ref = pick_dict(row, ["objectRef", "object_ref"]) or pick_dict(metadata, ["objectRef", "object_ref"])
        graph_ref = pick_dict(row, ["graph_ref"])
        evidence_object_key = (
            pick_str(object_ref, ["objectKey", "object_key"])
            or pick_str(metadata, ["evidence_object_key", "object_key"])
            or pick_str(row, ["evidence_object_key"])
        )
        if section_context is not None:
            relevance_score = _evidence_relevance_score(
                section_context,
                snippet,
                source_url,
                pick_str(row, ["title", "document_title", "page_title"]),
            )
            if relevance_score < 0.06:
                continue
        else:
            relevance_score = 1.0
        if not source_url and not document_id and not object_ref and not graph_ref:
            # Do not cite tool receipts or internal artifacts as evidence.
            continue
        packed.append(
            EvidenceRefModel(
                citation_key=f"{section_id.upper()}_{idx}",
                section_id=section_id,
                document_id=document_id,
                snippet=snippet,
                source_url=source_url,
                source_url_unavailable_reason=(
                    None
                    if source_url
                    else "source_url_missing_in_retrieved_evidence"
                ),
                title=pick_str(row, ["title", "document_title", "page_title"]),
                domain=_domain_from_url(source_url),
                retrieved_at=pick_str(row, ["retrievedAt", "retrieved_at", "created_at"]) or pick_str(metadata, ["retrievedAt", "retrieved_at", "created_at"]),
                content_hash=pick_str(row, ["contentHash", "content_hash", "sha256"]),
                relevance_score=relevance_score,
                evidence_object_key=evidence_object_key,
                source_type=_infer_source_type(row),
                score=float(row.get("score", 0.0) or 0.0),
                db_source=pick_str(row, ["db_source"]) or ("graph" if row.get("graph_entity_id") or row.get("graph_ref") else "vector"),
                object_ref=object_ref,
                graph_ref=graph_ref,
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
    run_id: str,
    task: SectionTaskModel,
    claims: List[ClaimModel],
    evidence: List[EvidenceRefModel],
    llm3: OpenRouterLLM | None,
) -> str:
    if llm3 is None:
        lines = [f"Section: {task.title}", f"Objective: {task.objective}"]
        if task.section_group:
            lines.append(f"Section group: {task.section_group}")
        if task.graph_chain:
            lines.append(f"Graph chain: {' -> '.join(task.graph_chain)}")
        if task.revision_focus:
            lines.append(f"Revision focus: {task.revision_focus}")
        if task.next_step_suggestion:
            lines.append(f"Next step: {task.next_step_suggestion}")
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
        parsed = invoke_complete_json(
            llm3,
            REPORT_SECTION_DRAFT_SYSTEM_PROMPT,
            payload,
            temperature=0.2,
            timeout=_report_worker_timeout(),
            run_id=run_id,
            operation="stage2.section_draft",
            metadata={"sectionId": task.section_id},
        )
        section_text = parsed.get("section_text")
        if isinstance(section_text, str) and section_text.strip():
            return section_text.strip()
    except Exception:
        logger.exception("Stage 2 section drafting failed")
    return draft_section_content(run_id, task, claims, evidence, llm3=None)


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
        graph_key = ""
        if isinstance(item.graph_ref, dict):
            graph_key = str(item.graph_ref.get("entityId") or item.graph_ref.get("edgeId") or "")
        key = f"{item.db_source}|{item.content_hash or item.document_id or graph_key}|{item.snippet[:100]}|{item.section_id}"
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
            noteboard=state.get("noteboard", []),
            stage1_receipts=state.get("stage1_receipts", []),
            claims=state.get("claim_ledger", []),
            evidence=state.get("evidence_refs", []),
            section_issues=state.get("section_issues", []),
            section_drafts=drafts,
            latest_observation="",
        )

    ordered = _benchmark_style_report_text(
        report_memory,
        drafts,
        str(state.get("report_type", "person")),
        list(state.get("section_issues", [])),
    )
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
        parsed = invoke_complete_json(
            llm3,
            FINAL_REPORT_ASSEMBLY_SYSTEM_PROMPT,
            payload,
            temperature=0.2,
            timeout=_report_final_timeout(),
            run_id=state.get("run_id"),
            operation="stage2.final_report",
        )
        report_text = parsed.get("report_text")
        if isinstance(report_text, str) and report_text.strip():
            assembled = report_text.strip()
            if _looks_like_benchmark_report(assembled) and (len(assembled) >= 200 or len(drafts) <= 2):
                return assembled
    except Exception:
        logger.exception("Final report assembly failed")
    return ordered


def assemble_evidence_appendix(items: List[EvidenceRefModel]) -> str:
    lines = ["Evidence Index"]
    visible = [item for item in items if _include_in_evidence_appendix(item)]
    if not visible:
        lines.append("1. [NO_EVIDENCE] -:- | unknown-date | - | No stable evidence refs collected.")
        return "\n".join(lines)
    for index, item in enumerate(visible, start=1):
        domain = item.domain or _domain_from_url(item.source_url) or "-"
        src = item.source_url or "-"
        retrieved = item.retrieved_at or "unknown-date"
        snippet = _sanitize_snippet(item.snippet or "")[:160]
        lines.append(f"{index}. [{item.citation_key}] {item.db_source}:{domain} | {retrieved} | {src} | {snippet}")
    return "\n".join(lines)


def build_report_memory(
    *,
    question: str,
    report_type: str,
    primary_entities: List[str],
    noteboard: List[str],
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
        noteboard=noteboard,
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
        claims=dedupe_claims([claim for claim in claims if claim.evidence_keys]),
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
    verified_claims = [claim for claim in claims if claim.evidence_keys]
    claim_sections = {claim.section_id for claim in verified_claims}
    claim_blob = " ".join(claim.text for claim in verified_claims).lower()
    stable_evidence = [item for item in evidence if item.source_url or item.document_id or item.object_ref]
    evidence_urls = [item.source_url or "" for item in stable_evidence]
    evidence_snippet_blob = " ".join(item.snippet for item in stable_evidence).lower()
    evidence_url_blob = " ".join(evidence_urls).lower()
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
    has_affiliation = (
        any(profile.affiliation for profile in profile_index)
        or any(dive.affiliations for dive in doc_deep_dives)
        or bool(re.search(r"\b(university|lab|department|company|inc|llc|organization|institute|works at|joined)\b", claim_blob))
        or bool(re.search(r"\b(university|lab|department|company|inc|llc|organization|institute|works at|joined)\b", evidence_snippet_blob))
    )
    has_education = (
        any(re.search(r"\b(university|college|school|b\.?s\.?|m\.?s\.?|ph\.?d|degree|graduated)\b", event.event.lower()) for event in timeline)
        or bool(re.search(r"\b(university|college|school|b\.?s\.?|m\.?s\.?|ph\.?d|degree|graduated)\b", claim_blob))
        or bool(re.search(r"\b(university|college|school|b\.?s\.?|m\.?s\.?|ph\.?d|degree|graduated)\b", evidence_snippet_blob))
    )
    has_publications = bool(publication_inventory or thesis_inventory) or "academic_research" in claim_sections
    has_relationships = bool(collaboration_graph_edges) or "relationships_and_associates" in claim_sections or "org_people_and_relations" in claim_sections
    has_contacts = (
        bool(re.search(r"\b(email|phone|contact page|contact us)\b", claim_blob))
        or bool(re.search(r"\b(email|phone|contact page|contact us)\b", evidence_snippet_blob))
        or bool(re.search(r"\bmailto:|@[\w.-]+\.[a-z]{2,}\b", evidence_url_blob))
        or "public_contact_methods" in claim_sections
    )
    has_handles = (
        bool(profile_index)
        or any(domain in evidence_url_blob for domain in ("github.com", "gitlab.com", "linkedin.com", "x.com", "twitter.com"))
        or "social_accounts_and_interests" in claim_sections
        or "org_presence_and_assets" in claim_sections
    )
    has_aliases = bool(canonical_identity.aliases) or bool(re.search(r"\b(alias|aka|username|handle)\b", claim_blob))
    has_code_presence = (
        bool(profile_index)
        or bool(re.search(r"\b(github|gitlab|repository|repositories|package|npm|pypi|crates|hugging face)\b", claim_blob))
        or bool(re.search(r"\b(github|gitlab|repository|repositories|package|npm|pypi|crates|hugging face)\b", evidence_snippet_blob))
        or "code_software_footprint" in claim_sections
    )
    has_business_roles = (
        bool(re.search(r"\b(founder|director|officer|board|sec filing|incorporated|company number|jurisdiction)\b", claim_blob))
        or bool(re.search(r"\b(founder|director|officer|board|sec filing|incorporated|company number|jurisdiction)\b", evidence_snippet_blob))
        or "org_identity" in claim_sections
        or "org_people_and_relations" in claim_sections
    )
    has_archived_history = (
        any("archive" in (item.source_url or "").lower() for item in stable_evidence)
        or any("webcache" in (item.source_url or "").lower() for item in stable_evidence)
        or bool(re.search(r"\b(archive|archived|wayback|snapshot)\b", claim_blob))
        or bool(re.search(r"\b(archive|archived|wayback|snapshot)\b", evidence_snippet_blob))
    )
    has_timeline = (
        len(timeline) >= (2 if report_type == "person" else 1)
        or bool(re.search(r"\b(19|20)\d{2}\b", claim_blob))
        or ("biography_history" in claim_sections and bool(verified_claims))
        or ("org_activity_and_history" in claim_sections and bool(verified_claims))
        or ("timeline_normalization" in section_map and bool(verified_claims))
    )
    has_limits = True

    return CoverageLedgerModel(
        identity_resolved=item(has_identity, 0.9 if has_identity else 0.2, "Canonical identity resolved." if has_identity else "Identity evidence remains thin."),
        aliases_resolved=item(has_aliases, 0.76 if has_aliases else 0.2, "Alias and handle variants were resolved." if has_aliases else "Alias and handle evidence remains thin."),
        affiliations_resolved=item(has_affiliation, 0.76 if has_affiliation else 0.25, "Affiliation signals found." if has_affiliation else "No stable affiliation evidence found."),
        education_resolved=item(has_education, 0.72 if has_education else 0.2, "Education timeline markers found." if has_education else "Education remains unresolved."),
        publications_resolved=item(has_publications, 0.82 if has_publications else 0.15, "Publication/thesis inventory present." if has_publications else "No publication-grade evidence found."),
        relationships_resolved=item(has_relationships, 0.8 if has_relationships else 0.2, "Typed relationship graph present." if has_relationships else "Associates/collaborators remain unresolved."),
        contacts_resolved=item(has_contacts, 0.78 if has_contacts else 0.2, "Public contact evidence was found." if has_contacts else "Public contact coverage remains incomplete."),
        handles_resolved=item(has_handles, 0.85 if has_handles else 0.2, "Profile index present." if has_handles else "No handle/profile baseline established."),
        code_presence_resolved=item(has_code_presence, 0.8 if has_code_presence else 0.2, "Code/software footprint found." if has_code_presence else "No public code footprint was confirmed."),
        business_roles_resolved=item(has_business_roles, 0.72 if has_business_roles else 0.2, "Business/directorship evidence found." if has_business_roles else "Business-role coverage remains unresolved."),
        archived_history_resolved=item(has_archived_history, 0.7 if has_archived_history else 0.2, "Archived history evidence found." if has_archived_history else "Archived-history coverage remains unresolved."),
        timeline_resolved=item(has_timeline, 0.8 if has_timeline else 0.2, "Structured timeline present." if has_timeline else "Timeline remains incomplete."),
        limits_explained=item(has_limits, 0.9, "Limits classify unresolved gaps and contradictions." if not_found_reasons else "Limits will enumerate unresolved gaps and contradictions."),
    )


def build_depth_quality_issues(
    *,
    report_type: str,
    primary_entities: List[str],
    stage1_receipts: List["ToolReceipt"],
    section_drafts: List[SectionDraftModel],
) -> List[str]:
    if report_type != "person":
        return []
    candidates = _extract_related_depth_candidates(stage1_receipts, primary_entities)
    if not candidates:
        return []
    draft_blob = " ".join(draft.content for draft in section_drafts).casefold()
    issues: List[str] = []
    for candidate in candidates:
        entity_name = str(candidate.get("entity_name") or "").strip()
        entity_type = str(candidate.get("entity_type") or "").strip()
        if not entity_name or not entity_type:
            continue
        if _stage1_receipts_investigated_related_entity(stage1_receipts, entity_name, entity_type):
            continue
        mentioned_in_report = entity_name.casefold() in draft_blob
        prefix = "Depth gap" if mentioned_in_report else "Missing depth follow-up"
        issues.append(
            f"{prefix}: related {entity_type} {entity_name} was discovered but not investigated beyond mention-level coverage."
        )
    return dedupe_str_list(issues)


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
            "aliases_resolved",
            "affiliations_resolved",
            "relationships_resolved",
            "contacts_resolved",
            "handles_resolved",
            "timeline_resolved",
            "limits_explained",
        )
    else:
        required_fields = (
            "identity_resolved",
            "aliases_resolved",
            "affiliations_resolved",
            "education_resolved",
            "publications_resolved",
            "relationships_resolved",
            "contacts_resolved",
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


def _benchmark_style_report_text(
    memory: ReportMemoryModel,
    drafts: List[SectionDraftModel],
    report_type: str,
    section_issues: List[str],
) -> str:
    lines = [_benchmark_report_title(memory, report_type)]
    if drafts:
        for draft in drafts:
            heading = _normalize_report_heading(draft.title)
            content = draft.content.strip()
            if not content:
                continue
            lines.append(f"## {heading}")
            lines.append(content)
    else:
        lines.append("## Findings")
        lines.append("No report sections were generated from the available evidence.")

    limitations = dedupe_str_list(list(memory.limits) + list(section_issues) + list(memory.open_questions))
    if limitations:
        lines.append("## Synthesis of Findings and Methodological Limitations")
        lines.append(_limitations_paragraph(limitations))
    return "\n".join(lines).strip()


def _benchmark_report_title(memory: ReportMemoryModel, report_type: str) -> str:
    subject = memory.canonical_identity.canonical_name or (memory.entities[0].name if memory.entities else "Target")
    scope = "Profile"
    if report_type == "org":
        scope = "Organizational Profile"
    elif memory.publication_inventory or memory.thesis_inventory:
        scope = "Research and Public Presence Profile"
    return f"Qwen Deep Research {scope} of {subject}"


def _normalize_report_heading(title: str) -> str:
    cleaned = " ".join(str(title or "").strip().split())
    if not cleaned:
        return "Section"
    return cleaned.lstrip("#").strip()


def _limitations_paragraph(items: List[str]) -> str:
    cleaned = [item.strip().rstrip(".") for item in items if isinstance(item, str) and item.strip()]
    if not cleaned:
        return "This report did not surface material methodological gaps in the available public evidence."
    summary = "; ".join(cleaned[:6])
    return (
        "This report is limited by the currently retrieved public evidence. "
        f"Key remaining gaps or cautions include: {summary}."
    )


def _looks_like_benchmark_report(report_text: str) -> bool:
    normalized = report_text.strip()
    if len(normalized) < 120:
        return False
    legacy_markers = (
        "\nFindings\n",
        "\nCanonical Identity\n",
        "\nCoverage Ledger\n",
        "\nEvidence Index\n",
    )
    if any(marker in normalized for marker in legacy_markers):
        return False
    return normalized.startswith("Qwen Deep Research") or "\n## " in normalized


def _why_evidence_matters(citation_key: str, claims: List[ClaimModel]) -> str:
    linked = [claim.text for claim in claims if citation_key in claim.evidence_keys]
    if linked:
        return linked[0][:140]
    return "Supports source-backed section drafting."


def _derive_structured_outputs(
    *,
    report_type: str,
    primary_entities: List[str],
    noteboard: List[str],
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
        "not_found_reasons": _build_not_found_reasons(stage1_receipts, noteboard),
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


def _extract_related_depth_candidates(receipts: List["ToolReceipt"], primary_entities: List[str]) -> List[Dict[str, Any]]:
    primary_people = {item.casefold() for item in primary_entities}
    candidates: Dict[str, Dict[str, Any]] = {}
    person_keys = {"relatedPeople", "coauthors", "authors", "advisor", "advisors", "collaborators", "colleagues", "mentors"}
    org_keys = {"organizations", "organization", "affiliations", "institution", "institutions", "employers", "companies", "company", "labs", "lab", "schools", "school"}

    def ensure(name: str, entity_type: str) -> Dict[str, Any]:
        key = f"{entity_type}:{name.casefold()}"
        item = candidates.get(key)
        if item is None:
            item = {"entity_name": name, "entity_type": entity_type, "score": 0, "mentions": 0}
            candidates[key] = item
        return item

    for receipt in receipts:
        if not receipt.ok:
            continue
        for fact in receipt.key_facts:
            if not isinstance(fact, dict):
                continue
            for key in person_keys:
                if key not in fact:
                    continue
                for person in _related_people_from_value(fact.get(key)):
                    if person.casefold() in primary_people:
                        continue
                    item = ensure(person, "person")
                    item["score"] += 2 if key in {"coauthors", "advisor", "advisors", "collaborators"} else 1
                    item["mentions"] += 1
            for key in org_keys:
                if key not in fact:
                    continue
                for org in _related_orgs_from_value(fact.get(key)):
                    item = ensure(org, "organization")
                    item["score"] += 2 if key in {"companies", "company", "labs", "lab", "institution", "institutions"} else 1
                    item["mentions"] += 1

    ranked = [item for item in candidates.values() if int(item.get("score", 0)) >= 2]
    ranked.sort(key=lambda item: (int(item.get("score", 0)), int(item.get("mentions", 0)), item.get("entity_name", "")), reverse=True)
    people = 0
    orgs = 0
    limited: List[Dict[str, Any]] = []
    for item in ranked:
        if item["entity_type"] == "person":
            if people >= 3:
                continue
            people += 1
        else:
            if orgs >= 2:
                continue
            orgs += 1
        limited.append(item)
    return limited


def _related_people_from_value(value: Any) -> List[str]:
    values: List[str] = []
    if isinstance(value, str):
        values.extend(_filter_related_person_candidates(extract_person_targets(value)))
    elif isinstance(value, list):
        for item in value:
            values.extend(_related_people_from_value(item))
    elif isinstance(value, dict):
        for key in ("name", "person", "author", "advisor", "colleague", "collaborator", "displayName"):
            item = value.get(key)
            if isinstance(item, str):
                values.extend(_filter_related_person_candidates(extract_person_targets(item)))
    return dedupe_str_list(values)


def _filter_related_person_candidates(values: List[str]) -> List[str]:
    filtered: List[str] = []
    for candidate in values:
        cleaned = " ".join(str(candidate or "").strip().split())
        tokens = [token.casefold() for token in cleaned.split() if token]
        if len(tokens) < 2 or len(tokens) > 4:
            continue
        if any(token in REPORT_RELATED_PERSON_REJECT_TOKENS for token in tokens):
            continue
        if any(token in REPORT_PROVIDER_BLOCKLIST for token in tokens):
            continue
        if not all(re.fullmatch(r"[A-Za-z][A-Za-z'-]*", token) for token in cleaned.split()):
            continue
        filtered.append(cleaned)
    return dedupe_str_list(filtered)


def _related_orgs_from_value(value: Any) -> List[str]:
    values: List[str] = []
    if isinstance(value, str):
        normalized = _normalize_related_org_candidate(value)
        if normalized:
            values.append(normalized)
    elif isinstance(value, list):
        for item in value:
            values.extend(_related_orgs_from_value(item))
    elif isinstance(value, dict):
        for key in ("name", "organization", "institution", "company", "lab", "school", "department", "employer", "affiliation"):
            item = value.get(key)
            if isinstance(item, str):
                normalized = _normalize_related_org_candidate(item)
                if normalized:
                    values.append(normalized)
    return dedupe_str_list(values)


def _normalize_related_org_candidate(value: str) -> str | None:
    candidate = " ".join((value or "").split()).strip(" -,:;")
    if len(candidate) < 3:
        return None
    lowered = candidate.casefold()
    if lowered in REPORT_ORG_DESCRIPTOR_TERMS:
        return None
    tokens = [token.casefold() for token in re.findall(r"[A-Za-z][A-Za-z'-]*", candidate)]
    if not tokens:
        return None
    if (
        tokens[0] in REPORT_PROVIDER_BLOCKLIST
        and len(tokens) <= 2
        and (len(tokens) == 1 or tokens[1] in {"research", "search", "person", "results", "sources", "profile"})
    ):
        return None
    if lowered in {"tavily research", "tavily person search", "google serp person search"}:
        return None
    if tokens and all(token in REPORT_ORG_GENERIC_TOKENS for token in tokens):
        return None
    markers = ("university", "college", "school", "lab", "laboratory", "institute", "department", "company", "corp", "inc", "llc", "group", "center", "centre")
    if not any(marker in lowered for marker in markers) and len(candidate.split()) < 2:
        return None
    return candidate


def _stage1_receipts_investigated_related_entity(receipts: List["ToolReceipt"], entity_name: str, entity_type: str) -> bool:
    person_tools = {
        "tavily_research",
        "tavily_person_search",
        "extract_webpage",
        "crawl_webpage",
        "person_search",
        "github_identity_search",
        "gitlab_identity_search",
        "arxiv_search_and_download",
        "orcid_search",
        "semantic_scholar_search",
        "dblp_author_search",
    }
    org_tools = {
        "tavily_research",
        "extract_webpage",
        "crawl_webpage",
        "open_corporates_search",
        "company_officer_search",
        "domain_whois_search",
        "contact_page_extractor",
        "org_staff_page_search",
        "wayback_fetch_url",
    }
    tool_set = person_tools if entity_type == "person" else org_tools
    target = entity_name.casefold()
    for receipt in receipts:
        if not receipt.ok or receipt.tool_name not in tool_set:
            continue
        for value in receipt.arguments.values():
            if isinstance(value, str) and value.strip() and target in value.casefold():
                return True
        if target in receipt.summary.casefold():
            return True
    return False


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


def _build_not_found_reasons(receipts: List[ToolReceipt], noteboard: List[str] | None = None) -> List[NotFoundReasonModel]:
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
    for note in noteboard or []:
        text = str(note).strip()
        lowered = text.lower()
        if "unresolved coverage:" in lowered:
            detail = text.split(":", 1)[1].strip() if ":" in text else text
            reasons.append(NotFoundReasonModel(category="not_searched", detail=f"Coverage gap remained unresolved: {detail}"))
        elif "coverage scorecard" in lowered:
            reasons.append(NotFoundReasonModel(category="not_searched", detail=text))
    deduped: Dict[str, NotFoundReasonModel] = {}
    for reason in reasons:
        deduped[f"{reason.category}|{reason.detail}"] = reason
    return list(deduped.values())[:20]


def _build_timeline(claims: List[ClaimModel], evidence: List[EvidenceRefModel]) -> List[TimelineEventModel]:
    events: List[TimelineEventModel] = []
    citation_map = {item.citation_key: item for item in evidence}
    for claim in claims:
        if not claim.evidence_keys:
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
                source_url=(source.source_url if source else None) or claim.source_url,
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
            candidates = fact.get("candidates")
            if isinstance(candidates, list):
                for candidate in candidates:
                    if not isinstance(candidate, dict):
                        continue
                    for evidence_item in candidate.get("evidence", []) if isinstance(candidate.get("evidence"), list) else []:
                        if not isinstance(evidence_item, dict):
                            continue
                        title = str(evidence_item.get("title") or "").strip()
                        if not title:
                            continue
                        url = str(evidence_item.get("url") or "").strip()
                        publications[title.lower()] = PublicationInventoryItemModel(
                            title=title,
                            year=str(evidence_item.get("year") or "").strip() or None,
                            venue=str(evidence_item.get("venue") or evidence_item.get("source") or "").strip() or None,
                            coauthors=[],
                            links=[url] if url else [],
                        )
    for item in evidence:
        if not _looks_like_publication_evidence(item):
            continue
        title = (item.title or "").strip() or _title_from_snippet(item.snippet)
        if not title:
            continue
        publications[title.lower()] = PublicationInventoryItemModel(
            title=title,
            year=_first_year(item.snippet or item.source_url or ""),
            venue=_venue_from_url(item.source_url),
            coauthors=[],
            links=[item.source_url] if item.source_url else [],
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
            direct_urls = _extract_urls_from_fact(fact, ("profileUrl", "profile_url", "url", "sourceUrl", "source_url"))
            for url in direct_urls:
                _upsert_profile_index_item(
                    profiles,
                    url=url,
                    platform=_platform_from_url(url) or result_platform,
                    last_active=str(fact.get("lastActive") or fact.get("last_active") or "").strip() or None,
                    title=str(fact.get("title") or fact.get("displayName") or "").strip() or None,
                    affiliation=str(fact.get("affiliation") or fact.get("organization") or fact.get("institution") or "").strip() or None,
                    projects=[str(item).strip() for item in fact.get("projects", []) if str(item).strip()] if isinstance(fact.get("projects"), list) else [],
                    pinned_items=[str(item).strip() for item in fact.get("repositories", []) if isinstance(item, str) and str(item).strip()] if isinstance(fact.get("repositories"), list) else [],
                )
            for list_key in ("matchedProfiles", "platformHits", "externalLinks", "pages", "sourceUrls", "source_urls", "profileUrls", "profile_urls"):
                values = fact.get(list_key)
                if not isinstance(values, list):
                    continue
                for item in values:
                    url = _url_from_mixed_value(item)
                    if not url:
                        continue
                    _upsert_profile_index_item(
                        profiles,
                        url=url,
                        platform=_platform_from_url(url) or result_platform,
                        title=_string_from_mixed_value(item, ("title", "displayName", "name")),
                        affiliation=_string_from_mixed_value(item, ("affiliation", "organization", "institution", "company")),
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


def _graph_row_from_entity_payload(
    entity_id: str,
    props: Dict[str, Any],
    labels: List[Any],
    score: float,
    rel_types: List[str] | None = None,
) -> Dict[str, Any] | None:
    if not props:
        return None
    canonical_name = pick_str(props, ["canonical_name", "name", "title", "display_name"]) or entity_id
    attributes = props.get("attributes") if isinstance(props.get("attributes"), list) else []
    rel_text = f" relations={', '.join(rel_types[:3])}." if rel_types else ""
    attr_text = "; ".join(str(item).strip() for item in attributes[:4] if str(item).strip())
    snippet_parts = [
        f"Graph entity {canonical_name}.",
        f"type={pick_str(props, ['type', 'osint_bucket']) or (labels[0] if labels else 'Entity')}.",
    ]
    if attr_text:
        snippet_parts.append(f"attributes={attr_text}.")
    if rel_text:
        snippet_parts.append(rel_text)
    source_url = pick_str(props, ["source_url", "sourceUrl", "uri", "url"])
    graph_ref = {
        "entityId": pick_str(props, ["node_id", "person_id", "org_id", "location_id", "address", "uri", "name"]) or entity_id,
        "labels": [str(item) for item in labels if isinstance(item, str)],
    }
    if rel_types:
        graph_ref["relTypes"] = rel_types[:5]
    row: Dict[str, Any] = {
        "graph_entity_id": graph_ref["entityId"],
        "graph_ref": graph_ref,
        "snippet": " ".join(part for part in snippet_parts if part).strip(),
        "title": canonical_name,
        "sourceUrl": source_url,
        "document_id": pick_str(props, ["evidence_document_id"]),
        "score": score,
        "db_source": "graph",
    }
    evidence_ref = {
        "bucket": pick_str(props, ["evidence_bucket"]),
        "objectKey": pick_str(props, ["evidence_object_key"]),
        "versionId": pick_str(props, ["evidence_version_id"]),
        "etag": pick_str(props, ["evidence_etag"]),
        "documentId": pick_str(props, ["evidence_document_id"]),
    }
    if any(evidence_ref.values()):
        row["objectRef"] = {key: value for key, value in evidence_ref.items() if value}
    return row


def _row_has_database_evidence(row: Dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    object_ref = pick_dict(row, ["objectRef", "object_ref"])
    if pick_str(row, ["db_source"]) == "graph":
        return bool(
            pick_str(row, ["graph_entity_id", "graph_relation_id"])
            or pick_dict(row, ["graph_ref"])
            or pick_str(row, ["document_id", "documentId"])
            or object_ref
        )
    return bool(
        pick_str(row, ["document_id", "documentId"])
        or object_ref
    )


def _first_url_in_text(text: str) -> str | None:
    match = re.search(r"https?://[^\s\])>]+", text or "")
    if not match:
        return None
    return match.group(0).rstrip(".,;")


def _domain_from_url(url: str | None) -> str | None:
    if not url:
        return None
    try:
        return urlparse(url).netloc.lower() or None
    except Exception:
        return None


def _looks_like_publication_evidence(item: EvidenceRefModel) -> bool:
    blob = " ".join(filter(None, [item.title, item.snippet, item.source_url])).lower()
    return bool(re.search(r"\b(arxiv|paper|preprint|proceedings|conference|journal|doi|emnlp|acl|neurips|iclr|openreview|semanticscholar|sciencedirect)\b", blob))


def _title_from_snippet(snippet: str) -> str | None:
    text = " ".join((snippet or "").split())
    if not text:
        return None
    before_url = re.split(r"https?://", text, maxsplit=1)[0].strip(" -|:")
    candidate = before_url[:160].strip(" .,:;")
    return candidate or None


def _first_year(text: str) -> str | None:
    match = re.search(r"\b(?:19|20)\d{2}\b", text or "")
    return match.group(0) if match else None


def _venue_from_url(url: str | None) -> str | None:
    domain = _domain_from_url(url)
    if not domain:
        return None
    if "arxiv" in domain:
        return "arXiv"
    if "aclanthology" in domain:
        return "ACL Anthology"
    if "openreview" in domain:
        return "OpenReview"
    if "semanticscholar" in domain:
        return "Semantic Scholar"
    if "sciencedirect" in domain:
        return "ScienceDirect"
    return domain


def _extract_urls_from_fact(fact: Dict[str, Any], keys: tuple[str, ...]) -> List[str]:
    urls: List[str] = []
    for key in keys:
        value = fact.get(key)
        if isinstance(value, str) and value.strip().startswith(("http://", "https://")):
            urls.append(value.strip())
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, str) and item.strip().startswith(("http://", "https://")):
                    urls.append(item.strip())
    return dedupe_str_list(urls)


def _url_from_mixed_value(value: Any) -> str | None:
    if isinstance(value, str) and value.strip().startswith(("http://", "https://")):
        return value.strip()
    if isinstance(value, dict):
        for key in ("url", "profileUrl", "profile_url", "sourceUrl", "source_url"):
            item = value.get(key)
            if isinstance(item, str) and item.strip().startswith(("http://", "https://")):
                return item.strip()
    return None


def _string_from_mixed_value(value: Any, keys: tuple[str, ...]) -> str | None:
    if not isinstance(value, dict):
        return None
    for key in keys:
        item = value.get(key)
        if isinstance(item, str) and item.strip():
            return item.strip()
    return None


def _platform_from_url(url: str) -> str | None:
    domain = _domain_from_url(url) or ""
    if "github.com" in domain:
        return "github"
    if "gitlab.com" in domain:
        return "gitlab"
    if "linkedin.com" in domain:
        return "linkedin"
    if "openreview.net" in domain:
        return "openreview"
    if "researchgate.net" in domain:
        return "researchgate"
    if "semanticscholar.org" in domain:
        return "semanticscholar"
    if "arxiv.org" in domain:
        return "arxiv"
    return domain or None


def _upsert_profile_index_item(
    profiles: Dict[str, ProfileIndexItemModel],
    *,
    url: str,
    platform: str | None = None,
    last_active: str | None = None,
    title: str | None = None,
    affiliation: str | None = None,
    projects: List[str] | None = None,
    pinned_items: List[str] | None = None,
) -> None:
    existing = profiles.get(url)
    if existing is None:
        profiles[url] = ProfileIndexItemModel(
            platform=platform or "web",
            url=url,
            last_active=last_active,
            title=title,
            affiliation=affiliation,
            projects=projects or [],
            pinned_items=pinned_items or [],
        )
        return
    if platform and existing.platform == "web":
        existing.platform = platform
    if last_active and not existing.last_active:
        existing.last_active = last_active
    if title and not existing.title:
        existing.title = title
    if affiliation and not existing.affiliation:
        existing.affiliation = affiliation
    existing.projects = dedupe_str_list(existing.projects + (projects or []))[:12]
    existing.pinned_items = dedupe_str_list(existing.pinned_items + (pinned_items or []))[:12]


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
