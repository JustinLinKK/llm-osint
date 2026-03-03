from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, TypedDict
from urllib.parse import urlparse

from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field

from mcp_client import McpClientProtocol, RoutedMcpClient
from openrouter_llm import OpenRouterLLM
from run_events import emit_run_event
from system_prompts import WORK_PLANNER_SYSTEM_PROMPT
from target_normalization import extract_person_targets, normalize_person_candidate, sanitize_search_tool_arguments
from tool_worker_graph import ToolReceipt, run_tool_worker, tool_argument_signature
from logger import get_logger
from env import load_env
from orchestrator.rules.academic_rules import PRIORITY_HIGH, add_task_if_new, derive_academic_follow_up_tasks, prune_dedupe_store
from orchestrator.rules.archive_identity_rules import derive_archive_identity_follow_up_tasks
from orchestrator.rules.business_rules import derive_business_follow_up_tasks
from orchestrator.rules.relationship_rules import derive_relationship_follow_up_tasks
from orchestrator.business_graph import build_business_graph_entities
from orchestrator.coverage import coverage_led_stop_condition, empty_coverage_ledger
from orchestrator.technical_graph import build_technical_graph_entities
from orchestrator.rules.technical_rules import derive_technical_follow_up_tasks

logger = get_logger(__name__)
STAGE1_MAX_TOOLS_PER_ITERATION = max(
    1, int(os.getenv("STAGE1_MAX_TOOLS_PER_ITERATION", "5"))
)
DEFAULT_MAX_WORKER = max(
    1,
    int(os.getenv("LANGGRAPH_MAX_WORKER", os.getenv("LANGGRAPH_MAX_WORKERS", "5"))),
)

URL_REGEX = re.compile(r"https?://[^\s\]]+")
EMAIL_REGEX = re.compile(
    r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
DOMAIN_REGEX = re.compile(
    r"\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}\b", re.IGNORECASE)
USERNAME_REGEX = re.compile(r"(?<!\w)@([A-Za-z0-9_]{3,32})")
PHONE_REGEX = re.compile(
    r"(?:\+\d{1,3}[\s.-]?)?(?:\(?\d{2,4}\)?[\s.-]){2,}\d{2,4}"
)
DATE_LIKE_PHONE_REGEX = re.compile(
    r"^(?:\d{4}[-/.]\d{1,2}[-/.]\d{1,2}|\d{1,2}[-/.]\d{1,2}[-/.]\d{4})$"
)
IPV4_REGEX = re.compile(
    r"\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)\b")
CAPITALIZED_NAME_REGEX = re.compile(r"\b[A-Z][a-z]+(?:[\s-]+[A-Z][a-z]+){0,3}\b")
PERSON_HINT_REGEX = re.compile(
    r"(?i)\b(?:investigate|investigation(?:\s+into)?|profile|research|look\s+into|find\s+info\s+on|osint(?:\s+on)?)\b[:\s-]*([A-Za-z][A-Za-z'\s-]{1,79})"
)
DERIVED_DOMAIN_RECON_BLOCKLIST = {
    "aclanthology.org",
    "acs.org",
    "arxiv.org",
    "dblp.org",
    "deepai.org",
    "emnlp.org",
    "github.com",
    "gitlab.com",
    "google.com",
    "googleusercontent.com",
    "linkedin.com",
    "medium.com",
    "openreview.net",
    "reddit.com",
    "researchgate.net",
    "rsna.org",
    "sciencedirect.com",
    "substack.com",
    "twitter.com",
    "x.com",
}
AUTO_GRAPH_ENTITY_LIMIT = 50
PERSON_CANDIDATE_STOPWORDS = {
    "please",
    "investigate",
    "investigation",
    "profile",
    "research",
    "look",
    "into",
    "find",
    "info",
    "osint",
    "person",
    "target",
    "public",
    "records",
    "social",
    "domain",
    "email",
    "phone",
    "website",
    "company",
    "organization",
}
PERSON_CANDIDATE_BREAKWORDS = {
    "and",
    "or",
    "with",
    "for",
    "about",
    "gather",
    "collect",
    "find",
    "look",
    "into",
}


class ToolPlanItem(BaseModel):
    tool: str
    arguments: Dict[str, Any]
    rationale: str


class PlannerState(TypedDict):
    run_id: str
    prompt: str
    inputs: List[str]
    seed_urls: List[str]
    pending_urls: List[str]
    current_fetch_urls: List[str]
    visited_urls: List[str]
    allowed_hosts: List[str]
    tool_plan: List[ToolPlanItem]
    latest_tool_receipts: List[ToolReceipt]
    rationale: str
    documents_created: List[str]
    tool_receipts: List[ToolReceipt]
    iteration: int
    max_iterations: int
    done: bool
    enough_info: bool
    noteboard: List[str]
    next_stage: str
    queued_tasks: List[Dict[str, Any]]
    academic_task_dedupe: Dict[str, int]
    technical_task_dedupe: Dict[str, int]
    business_task_dedupe: Dict[str, int]
    archive_identity_task_dedupe: Dict[str, int]
    relationship_task_dedupe: Dict[str, int]
    coverage_ledger: Dict[str, bool]


@dataclass
class PlannerResult:
    run_id: str
    tool_plan: List[ToolPlanItem]
    documents_created: List[str]
    rationale: str
    tool_receipts: List[ToolReceipt]
    iterations: int
    noteboard: List[str]
    next_stage: str


def build_planner_graph(
    mcp_client: McpClientProtocol,
    llm: OpenRouterLLM | None = None,
    max_worker: int = DEFAULT_MAX_WORKER,
) -> StateGraph:
    graph = StateGraph(PlannerState)
    worker_limit = max(1, max_worker)

    def analyze_input(state: PlannerState) -> PlannerState:
        prompt_urls = _extract_urls(state.get("prompt", ""))
        input_urls: List[str] = []
        for item in state.get("inputs", []):
            input_urls.extend(_extract_urls(item))

        seed_urls = _dedupe(prompt_urls + input_urls)
        allowed_hosts = _extract_allowed_hosts(seed_urls)
        logger.info("Planner input analyzed", extra={"seed_urls": seed_urls})
        return {
            **state,
            "seed_urls": seed_urls,
            "pending_urls": seed_urls,
            "current_fetch_urls": [],
            "visited_urls": [],
            "allowed_hosts": allowed_hosts,
            "iteration": 0,
            "done": False,
            "queued_tasks": [],
            "academic_task_dedupe": {},
            "technical_task_dedupe": {},
            "business_task_dedupe": {},
            "archive_identity_task_dedupe": {},
            "relationship_task_dedupe": {},
            "coverage_ledger": empty_coverage_ledger(),
        }

    def plan_tools(state: PlannerState) -> PlannerState:
        seed_urls = list(state.get("seed_urls", []))
        pending_urls = list(state.get("pending_urls", []))
        visited_urls = list(state.get("visited_urls", []))
        allowed_hosts = list(state.get("allowed_hosts", []))
        rationale = ""
        enough_info = False
        llm_plan: List[ToolPlanItem] = []
        domains = _extract_domains_from_state(state)
        emails = _extract_emails_from_state(state)
        usernames = _extract_usernames_from_state(state)
        linkedin_profiles = _extract_linkedin_profiles_from_state(state)
        phone_numbers = _extract_phone_numbers_from_state(state)
        person_targets = _extract_person_targets_from_state(state)
        primary_person_targets = _extract_primary_person_targets(state)
        related_person_targets = [item for item in person_targets if item not in set(primary_person_targets)]

        tool_catalog = [
            # ————————————————————————————————
            # CORE TOOLS — Strong deterministic signal
            # Planner should prefer these for authoritative extraction
            # ————————————————————————————————

            {
                "name": "fetch_url",
                "description": "Utility: fetch URL & return raw HTTP response.",
                "type": "utility",
                "confidence": 0.9,
                "category": ["http", "fetch"],
                "args": {"runId": "uuid", "url": "string"},
            },
            {
                "name": "osint_maigret_username",
                "description": "Core: deep username profiling; high coverage, metadata enriched.",
                "type": "username_recon",
                "confidence": 0.8,
                "category": ["identity", "username"],
                "args": {"runId": "uuid", "username": "string"},
            },
            {
                "name": "osint_amass_domain",
                "description": "Core: passive domain & subdomain discovery across multiple sources.",
                "type": "domain_recon",
                "confidence": 0.9,
                "category": ["domain", "dns"],
                "args": {"runId": "uuid", "domain": "string", "passive": "boolean"},
            },
            {
                "name": "osint_whatweb_target",
                "description": "Core: fingerprint web technologies & frameworks.",
                "type": "web_tech",
                "confidence": 0.85,
                "category": ["fingerprint", "web"],
                "args": {"runId": "uuid", "target": "string"},
            },
            {
                "name": "osint_exiftool_extract",
                "description": "Core: deterministic metadata extraction from a local file or a MinIO object.",
                "type": "file_meta",
                "confidence": 0.95,
                "category": ["metadata", "file"],
                "args": {
                    "runId": "uuid",
                    "path": "string",
                    "objectKey": "string",
                    "bucket": "string",
                    "versionId": "string",
                },
            },
            {
                "name": "x_get_user_posts_api",
                "description": "Core: fetch recent posts from an X username via official API v2.",
                "type": "social_posts",
                "confidence": 0.8,
                "category": ["social", "x", "posts"],
                "args": {"runId": "uuid", "username": "string", "max_results": "int"},
            },
            {
                "name": "linkedin_download_html_ocr",
                "description": "Core: download LinkedIn profile/activity HTML via Browserbase for later parsing/OCR.",
                "type": "social_profile_capture",
                "confidence": 0.75,
                "category": ["social", "linkedin", "capture"],
                "args": {"runId": "uuid", "profile": "string", "output_dir": "string"},
            },
            {
                "name": "tavily_research",
                "description": "Core: run Tavily's async research workflow to produce a cited source-backed report for a person, company, site, or topic.",
                "type": "deep_research",
                "confidence": 0.86,
                "category": ["research", "tavily", "sources"],
                "args": {"runId": "uuid", "input": "string", "model": "string", "timeout_seconds": "int"},
            },
            {
                "name": "tavily_person_search",
                "description": "Core: search person via Tavily and return high-signal public-web discovery results.",
                "type": "web_search",
                "confidence": 0.82,
                "category": ["search", "person", "tavily"],
                "args": {"runId": "uuid", "target_name": "string", "max_results": "int"},
            },
            {
                "name": "google_serp_person_search",
                "description": "Core: run Google SERP person search and archive resulting HTML pages; useful for biography/history/contact/relationship source discovery.",
                "type": "web_search",
                "confidence": 0.7,
                "category": ["search", "person"],
                "args": {"runId": "uuid", "target_name": "string", "max_results": "int"},
            },
            {
                "name": "github_identity_search",
                "description": "Core: resolve public GitHub identity, repos, org memberships, and linked site/email signals.",
                "type": "code_identity",
                "confidence": 0.9,
                "category": ["identity", "code", "github"],
                "args": {
                    "runId": "uuid",
                    "person_name": "string",
                    "username": "string",
                    "profile_url": "string",
                    "email": "string",
                    "repo_url": "string",
                },
            },
            {
                "name": "personal_site_search",
                "description": "Core: resolve a direct personal site URL/domain and extract contact/linkage signals.",
                "type": "personal_site",
                "confidence": 0.8,
                "category": ["website", "identity", "contact"],
                "args": {
                    "runId": "uuid",
                    "name": "string",
                    "url": "string",
                    "blog": "string",
                    "domain": "string",
                    "email": "string",
                },
            },
            {
                "name": "gitlab_identity_search",
                "description": "Enrichment: resolve public GitLab identity and project namespace footprint.",
                "type": "code_identity",
                "confidence": 0.85,
                "category": ["identity", "code", "gitlab"],
                "args": {
                    "runId": "uuid",
                    "person_name": "string",
                    "username": "string",
                    "profile_url": "string",
                },
            },
            {
                "name": "package_registry_search",
                "description": "Core: aggregate package registry search across npm and crates.io for maintainer/publication signals.",
                "type": "package_registry",
                "confidence": 0.8,
                "category": ["registry", "packages", "aggregator"],
                "args": {
                    "runId": "uuid",
                    "person_name": "string",
                    "username": "string",
                    "email": "string",
                },
            },
            {
                "name": "npm_author_search",
                "description": "Enrichment: search npm registry by author, maintainer handle, or email.",
                "type": "package_registry",
                "confidence": 0.78,
                "category": ["registry", "npm", "packages"],
                "args": {
                    "runId": "uuid",
                    "person_name": "string",
                    "username": "string",
                    "email": "string",
                },
            },
            {
                "name": "crates_author_search",
                "description": "Enrichment: search crates.io users and published Rust crates.",
                "type": "package_registry",
                "confidence": 0.76,
                "category": ["registry", "crates", "packages"],
                "args": {
                    "runId": "uuid",
                    "person_name": "string",
                    "username": "string",
                },
            },
            {
                "name": "wayback_fetch_url",
                "description": "Core: resolve compact Wayback snapshots for a strong profile or site URL.",
                "type": "archive_lookup",
                "confidence": 0.8,
                "category": ["archive", "history", "url"],
                "args": {"runId": "uuid", "url": "string", "max_results": "int"},
            },
            {
                "name": "open_corporates_search",
                "description": "Core: resolve a company into a stable corporate registry record with officers.",
                "type": "business_registry",
                "confidence": 0.82,
                "category": ["business", "company", "registry"],
                "args": {"runId": "uuid", "company_name": "string", "jurisdiction_code": "string", "company_number": "string"},
            },
            {
                "name": "company_officer_search",
                "description": "Core: search companies where a person is an officer or director.",
                "type": "business_role",
                "confidence": 0.82,
                "category": ["business", "officer", "director"],
                "args": {"runId": "uuid", "person_name": "string", "jurisdiction_code": "string", "max_results": "int"},
            },
            {
                "name": "company_filing_search",
                "description": "Enrichment: fetch company filing history from OpenCorporates or SEC submissions.",
                "type": "business_filing",
                "confidence": 0.78,
                "category": ["business", "filings"],
                "args": {"runId": "uuid", "company_number": "string", "jurisdiction_code": "string", "cik": "string", "max_results": "int"},
            },
            {
                "name": "sec_person_search",
                "description": "Enrichment: search SEC records for executive, director, or insider involvement.",
                "type": "business_sec",
                "confidence": 0.76,
                "category": ["business", "sec", "public-company"],
                "args": {"runId": "uuid", "person_name": "string", "company_name": "string", "cik": "string", "max_results": "int"},
            },
            {
                "name": "director_disclosure_search",
                "description": "Enrichment: parse director disclosures from SEC proxy filings.",
                "type": "business_director",
                "confidence": 0.74,
                "category": ["business", "director", "proxy"],
                "args": {"runId": "uuid", "filing_url": "string", "company_name": "string", "max_results": "int"},
            },
            {
                "name": "domain_whois_search",
                "description": "Core: resolve domain ownership and registration metadata via RDAP.",
                "type": "business_domain",
                "confidence": 0.8,
                "category": ["business", "domain", "rdap"],
                "args": {"runId": "uuid", "domain": "string", "max_results": "int"},
            },
            {
                "name": "wayback_domain_timeline_search",
                "description": "Enrichment: fetch Wayback snapshot timeline for a domain.",
                "type": "archive_timeline",
                "confidence": 0.8,
                "category": ["archive", "domain", "history"],
                "args": {"runId": "uuid", "domain": "string", "max_results": "int"},
            },
            {
                "name": "historical_bio_diff",
                "description": "Internal: compare earliest and latest archived bio text for structured changes.",
                "type": "archive_diff",
                "confidence": 0.85,
                "category": ["archive", "history", "diff"],
                "args": {
                    "runId": "uuid",
                    "earliest_text": "string",
                    "latest_text": "string",
                    "earliest_url": "string",
                    "latest_url": "string",
                },
            },
            {
                "name": "sanctions_watchlist_search",
                "description": "High-signal: exact-name sanctions watchlist check using public lists.",
                "type": "sanctions_check",
                "confidence": 1.0,
                "category": ["legal", "sanctions", "risk"],
                "args": {"runId": "uuid", "person_name": "string"},
            },
            {
                "name": "alias_variant_generator",
                "description": "Internal: generate deterministic alias and username variants from a person name.",
                "type": "identity_expansion",
                "confidence": 0.9,
                "category": ["identity", "alias"],
                "args": {"runId": "uuid", "person_name": "string"},
            },
            {
                "name": "username_permutation_search",
                "description": "Internal: check direct profile URL permutations across GitHub/GitLab/Reddit.",
                "type": "identity_expansion",
                "confidence": 0.8,
                "category": ["identity", "username"],
                "args": {"runId": "uuid", "username": "string"},
            },
            {
                "name": "cross_platform_profile_resolver",
                "description": "Internal: deterministically resolve cross-platform profile identity matches.",
                "type": "identity_resolution",
                "confidence": 0.85,
                "category": ["identity", "resolver"],
                "args": {"runId": "uuid", "profiles": "list"},
            },
            {
                "name": "institution_directory_search",
                "description": "Enrichment: search a known institution domain for a direct profile or directory result.",
                "type": "institution_directory",
                "confidence": 0.6,
                "category": ["identity", "institution", "directory"],
                "args": {"runId": "uuid", "institution_domain": "string", "person_name": "string"},
            },
            {
                "name": "email_pattern_inference",
                "description": "Internal: infer likely email patterns from a domain and person name.",
                "type": "contact_inference",
                "confidence": 0.7,
                "category": ["contact", "email"],
                "args": {"runId": "uuid", "domain": "string", "person_name": "string"},
            },
            {
                "name": "contact_page_extractor",
                "description": "Enrichment: fetch common contact/about/team pages and extract public contact signals.",
                "type": "contact_extraction",
                "confidence": 0.75,
                "category": ["contact", "website"],
                "args": {"runId": "uuid", "site_url": "string"},
            },
            {
                "name": "reddit_user_search",
                "description": "Enrichment: resolve a public Reddit profile directly from a username.",
                "type": "social_profile",
                "confidence": 0.8,
                "category": ["social", "reddit", "username"],
                "args": {"runId": "uuid", "username": "string"},
            },
            {
                "name": "mastodon_profile_search",
                "description": "Enrichment: resolve a public Mastodon profile from a profile URL or instance+username.",
                "type": "social_profile",
                "confidence": 0.75,
                "category": ["social", "mastodon", "profile"],
                "args": {"runId": "uuid", "profile_url": "string", "instance": "string", "username": "string"},
            },
            {
                "name": "substack_author_search",
                "description": "Enrichment: resolve a public Substack author/publication page and linked contacts.",
                "type": "social_profile",
                "confidence": 0.72,
                "category": ["social", "substack", "author"],
                "args": {"runId": "uuid", "url": "string", "subdomain": "string"},
            },
            {
                "name": "medium_author_search",
                "description": "Enrichment: resolve a public Medium author page and article links.",
                "type": "social_profile",
                "confidence": 0.72,
                "category": ["social", "medium", "author"],
                "args": {"runId": "uuid", "username": "string", "profile_url": "string"},
            },
            {
                "name": "coauthor_graph_search",
                "description": "Relationship: derive coauthor and venue overlap from publication data.",
                "type": "relationship_graph",
                "confidence": 0.8,
                "category": ["relationship", "academic", "coauthor"],
                "args": {"runId": "uuid", "person_name": "string", "publication_data": "json"},
            },
            {
                "name": "org_staff_page_search",
                "description": "Relationship: fetch common org staff/team pages and extract structured staff entries.",
                "type": "relationship_org",
                "confidence": 0.72,
                "category": ["relationship", "organization", "staff"],
                "args": {"runId": "uuid", "org_url": "string"},
            },
            {
                "name": "board_member_overlap_search",
                "description": "Relationship: compare officer/director lists for overlapping memberships.",
                "type": "relationship_overlap",
                "confidence": 0.82,
                "category": ["relationship", "company", "board"],
                "args": {"runId": "uuid", "roles": "json"},
            },
            {
                "name": "shared_contact_pivot_search",
                "description": "Relationship: compare public emails, organizations, and addresses for shared pivots.",
                "type": "relationship_contact",
                "confidence": 0.75,
                "category": ["relationship", "contact", "pivot"],
                "args": {"runId": "uuid", "contacts": "json", "emails": "string[]"},
            },
            {
                "name": "arxiv_search_and_download",
                "description": "Core: search arXiv by author/topic and optionally download matched papers, co-author, affiliation, and publication-history clues.",
                "type": "research_papers",
                "confidence": 0.85,
                "category": ["arxiv", "papers"],
                "args": {"runId": "uuid", "author": "string", "topic": "string", "max_results": "int"},
            },
            {
                "name": "person_search",
                "description": "Core: broad person search workflow over public web pages for biography, history, contact methods, and related-people discovery.",
                "type": "person_recon",
                "confidence": 0.65,
                "category": ["person", "web"],
                "args": {"runId": "uuid", "name": "string", "max_results": "int"},
            },

            # ————————————————————————————————
            # ENRICHMENT TOOLS — Useful but noisy
            # Require post-filtering, deduplication, or scoring
            # ————————————————————————————————

            {
                "name": "osint_holehe_email",
                "description": "Enrichment: checks email presence across services; noisy, non-validated hits.",
                "type": "email_recon",
                "confidence": 0.5,
                "category": ["email", "account_check"],
                "note": "Requires strict post-filtering and dedup scoring",
                "args": {"runId": "uuid", "email": "string"},
            },
            {
                "name": "osint_theharvester_email_domain",
                "description": "Enrichment: gather emails, hosts, domains from public sources; noisy search scraping.",
                "type": "domain_recon",
                "confidence": 0.6,
                "category": ["email", "domain"],
                "note": "High noise; filter against known good sources",
                "args": {"runId": "uuid", "domain": "string", "source": "string", "limit": "int"},
            },
            {
                "name": "osint_reconng_domain",
                "description": "Enrichment: run Recon-ng modules; module quality varies widely.",
                "type": "domain_recon",
                "confidence": 0.55,
                "category": ["recon_framework"],
                "note": "Module selection critically affects results",
                "args": {"runId": "uuid", "domain": "string", "module": "string"},
            },
            {
                "name": "osint_spiderfoot_scan",
                "description": "Enrichment: broad SpiderFoot scan; great coverage but high volume.",
                "type": "scan_recon",
                "confidence": 0.65,
                "category": ["broad_scan"],
                "note": "Aggregate external sources; post-filter heavily",
                "args": {"runId": "uuid", "target": "string", "modules": "string|array"},
            },
            {
                "name": "osint_sublist3r_domain",
                "description": "Enrichment: subdomain enumeration; complementary to Amass.",
                "type": "domain_recon",
                "confidence": 0.6,
                "category": ["domain", "dns"],
                "note": "Lower coverage than Amass; use with scoring",
                "args": {"runId": "uuid", "domain": "string"},
            },

            # ————————————————————————————————
            # MANUAL or ANALYST-DRIVEN TOOLS
            # Not automated; triggers human task
            # ————————————————————————————————

            {
                "name": "osint_maltego_manual",
                "description": "Manual graph link analysis workflow placeholder (visual investigation).",
                "type": "manual",
                "confidence": None,
                "category": ["visual", "manual"],
                "note": "Requires human guidance & interpretation",
                "args": {"runId": "uuid"},
            },
            {
                "name": "osint_foca_manual",
                "description": "Manual FOCA workflow placeholder for document metadata analysis.",
                "type": "manual",
                "confidence": None,
                "category": ["manual", "metadata"],
                "note": "Human-orchestrated extraction & interpretation",
                "args": {"runId": "uuid"},
            },
        ]
        catalog_tool_names = {item["name"] for item in tool_catalog}
        if llm is not None:
            try:
                prompt = _inject_noteboard(
                    state.get("prompt", ""), state.get("noteboard", []))
                result = llm.plan_tools(
                    prompt,
                    state.get("inputs", []),
                    tool_catalog,
                    prior_tool_calls=_planner_completed_tool_calls(state),
                    system_prompt=WORK_PLANNER_SYSTEM_PROMPT,
                )
                rationale = result.get("rationale", "")
                enough_info = bool(result.get("enough_info", False))
                llm_plan = _normalize_llm_tool_plan(
                    result.get("plan", []), state["run_id"], catalog_tool_names, person_targets
                )
                llm_urls = [url for url in result.get(
                    "urls", []) if isinstance(url, str)]
                seed_urls = _dedupe(seed_urls + llm_urls)
                pending_urls = _dedupe(pending_urls + llm_urls)
                allowed_hosts = _dedupe(allowed_hosts + _extract_allowed_hosts(llm_urls))
            except Exception as exc:
                logger.error("Planner LLM failed", extra={"error": str(exc)})
                rationale = "LLM planning failed, using heuristic URL extraction."

        current_fetch_urls = _select_fetch_batch(pending_urls, visited_urls)

        plan: List[ToolPlanItem] = list(llm_plan)
        queued_tasks = list(state.get("queued_tasks", []))
        remaining_queued_tasks: List[Dict[str, Any]] = []
        for task in sorted(queued_tasks, key=lambda item: int(item.get("priority", 0)), reverse=True):
            tool_name = task.get("tool_name")
            payload = task.get("payload")
            reason = task.get("reason")
            if not isinstance(tool_name, str) or not isinstance(payload, dict):
                continue
            if _receipt_has_argument_signature(state, tool_name, payload):
                continue
            if len(plan) < 12:
                plan.append(
                    ToolPlanItem(
                        tool=tool_name,
                        arguments=payload,
                        rationale=str(reason or f"Deterministic follow-up for {tool_name}."),
                    )
                )
            else:
                remaining_queued_tasks.append(task)
        if current_fetch_urls:
            rationale = rationale or (
                f"Fetching {len(current_fetch_urls)} page(s) from the crawl frontier and following internal links within scope."
            )

        for url in current_fetch_urls:
            plan.append(
                ToolPlanItem(
                    tool="fetch_url",
                    arguments={"runId": state["run_id"], "url": url},
                    rationale=f"Fetch URL from crawl frontier for evidence collection: {url}",
                )
            )
            plan.append(
                ToolPlanItem(
                    tool="osint_whatweb_target",
                    arguments={"runId": state["run_id"], "target": url},
                    rationale=f"Fingerprint web technologies for target: {url}",
                )
            )

        if state.get("iteration", 0) == 0:
            for domain in domains:
                if not _receipt_has_value(state, "osint_amass_domain", {"domain": domain}):
                    plan.append(
                        ToolPlanItem(
                            tool="osint_amass_domain",
                            arguments={"runId": state["run_id"], "domain": domain, "passive": True},
                            rationale=f"Core baseline: expand passive subdomain footprint with Amass: {domain}",
                        )
                    )
                if not _receipt_has_argument_signature(state, "domain_whois_search", {"domain": domain}):
                    plan.append(
                        ToolPlanItem(
                            tool="domain_whois_search",
                            arguments={"runId": state["run_id"], "domain": domain, "max_results": 5},
                            rationale=f"Resolve domain ownership and registrar metadata for: {domain}",
                        )
                    )

            for username in usernames:
                if not _receipt_has_value(state, "osint_maigret_username", {"username": username}):
                    plan.append(
                        ToolPlanItem(
                            tool="osint_maigret_username",
                            arguments={"runId": state["run_id"], "username": username},
                            rationale=f"Core baseline: profile username metadata with Maigret: {username}",
                        )
                    )
                if not _receipt_has_value(state, "x_get_user_posts_api", {"username": username}):
                    plan.append(
                        ToolPlanItem(
                            tool="x_get_user_posts_api",
                            arguments={"runId": state["run_id"], "username": username, "max_results": 10},
                            rationale=f"Fetch recent X posts for discovered handle: @{username}",
                        )
                    )
                if not _receipt_has_argument_signature(state, "reddit_user_search", {"username": username}):
                    plan.append(
                        ToolPlanItem(
                            tool="reddit_user_search",
                            arguments={"runId": state["run_id"], "username": username},
                            rationale=f"Resolve direct Reddit profile coverage for discovered username: {username}",
                        )
                    )
                if not _receipt_has_argument_signature(state, "medium_author_search", {"username": username}):
                    plan.append(
                        ToolPlanItem(
                            tool="medium_author_search",
                            arguments={"runId": state["run_id"], "username": username},
                            rationale=f"Check whether the discovered username maps to a public Medium author profile: {username}",
                        )
                    )

            for profile in linkedin_profiles:
                if not _receipt_has_value(state, "linkedin_download_html_ocr", {"profile": profile}):
                    plan.append(
                        ToolPlanItem(
                            tool="linkedin_download_html_ocr",
                            arguments={"runId": state["run_id"], "profile": profile},
                            rationale=f"Capture LinkedIn HTML for profile/activity evidence: {profile}",
                        )
                    )

            for target_name in primary_person_targets[:3]:
                has_tavily_research = _receipt_has_value(state, "tavily_research", {"input": target_name})
                has_tavily_search = _receipt_has_value(state, "tavily_person_search", {"targetName": target_name})
                if not _receipt_has_argument_signature(state, "alias_variant_generator", {"person_name": target_name}):
                    plan.append(
                        ToolPlanItem(
                            tool="alias_variant_generator",
                            arguments={"runId": state["run_id"], "person_name": target_name},
                            rationale=f"Generate deterministic alias and username variants for: {target_name}",
                        )
                    )
                if not _receipt_has_argument_signature(state, "sanctions_watchlist_search", {"person_name": target_name}):
                    plan.append(
                        ToolPlanItem(
                            tool="sanctions_watchlist_search",
                            arguments={"runId": state["run_id"], "person_name": target_name},
                            rationale=f"Run exact-name sanctions watchlist check for: {target_name}",
                        )
                    )
                if not has_tavily_research:
                    plan.append(
                        ToolPlanItem(
                            tool="tavily_research",
                            arguments={"runId": state["run_id"], "input": target_name, "timeout_seconds": 240},
                            rationale=f"Use Tavily research as the high-depth public-web entry point for a cited synthesis of biography, affiliations, relationships, and public footprint for: {target_name}",
                        )
                    )
                if not has_tavily_search:
                    plan.append(
                        ToolPlanItem(
                            tool="tavily_person_search",
                            arguments={"runId": state["run_id"], "target_name": target_name, "max_results": 10},
                            rationale=f"Use Tavily search as the broad discovery layer for biography, history, contact, and relationship clues for: {target_name}",
                        )
                    )
                if has_tavily_search and not _receipt_has_value(state, "google_serp_person_search", {"targetName": target_name}):
                    plan.append(
                        ToolPlanItem(
                            tool="google_serp_person_search",
                            arguments={"runId": state["run_id"], "target_name": target_name, "max_results": 10},
                            rationale=f"Fallback public-web discovery via Google SERP for: {target_name}",
                        )
                    )
                if has_tavily_search and not _receipt_has_value(state, "person_search", {"name": target_name}):
                    plan.append(
                        ToolPlanItem(
                            tool="person_search",
                            arguments={"runId": state["run_id"], "name": target_name, "max_results": 10},
                            rationale=f"Run broad person search to collect corroborating public profiles, history, and contact signals for: {target_name}",
                        )
                    )
                if not _receipt_has_argument_signature(state, "github_identity_search", {"person_name": target_name}):
                    plan.append(
                        ToolPlanItem(
                            tool="github_identity_search",
                            arguments={"runId": state["run_id"], "person_name": target_name, "max_results": 5},
                            rationale=f"Resolve public code identity anchors, repositories, and org memberships for: {target_name}",
                        )
                    )
                if not _receipt_has_argument_signature(state, "gitlab_identity_search", {"person_name": target_name}):
                    plan.append(
                        ToolPlanItem(
                            tool="gitlab_identity_search",
                            arguments={"runId": state["run_id"], "person_name": target_name, "max_results": 5},
                            rationale=f"Resolve public GitLab code identity and namespace signals for: {target_name}",
                        )
                    )
                if not _receipt_has_argument_signature(state, "package_registry_search", {"person_name": target_name}):
                    plan.append(
                        ToolPlanItem(
                            tool="package_registry_search",
                            arguments={"runId": state["run_id"], "person_name": target_name, "max_results": 5},
                            rationale=f"Search public package registries for package publication signals for: {target_name}",
                        )
                    )
                if not _receipt_has_value(state, "arxiv_search_and_download", {"author": target_name}):
                    plan.append(
                        ToolPlanItem(
                            tool="arxiv_search_and_download",
                            arguments={"runId": state["run_id"], "author": target_name, "topic": "", "max_results": 10},
                            rationale=f"Check publication history, co-authors, and affiliation pivots for: {target_name}",
                        )
                    )
                for tool_name, description in (
                    ("orcid_search", "Resolve public academic identity via ORCID"),
                    ("semantic_scholar_search", "Resolve public academic identity via Semantic Scholar"),
                    ("dblp_author_search", "Resolve public academic identity via DBLP"),
                ):
                    if not _receipt_has_argument_signature(state, tool_name, {"person_name": target_name}):
                        plan.append(
                            ToolPlanItem(
                                tool=tool_name,
                                arguments={"runId": state["run_id"], "person_name": target_name, "max_results": 10},
                                rationale=f"{description}: {target_name}",
                            )
                        )

        else:
            for target_name in related_person_targets[:4]:
                has_tavily_research = _receipt_has_value(state, "tavily_research", {"input": target_name})
                has_tavily_search = _receipt_has_value(state, "tavily_person_search", {"targetName": target_name})
                if not has_tavily_research:
                    plan.append(
                        ToolPlanItem(
                            tool="tavily_research",
                            arguments={"runId": state["run_id"], "input": target_name, "timeout_seconds": 180},
                            rationale=f"Expand related-person coverage with cited Tavily research for discovered person: {target_name}",
                        )
                    )
                if not has_tavily_search:
                    plan.append(
                        ToolPlanItem(
                            tool="tavily_person_search",
                            arguments={"runId": state["run_id"], "target_name": target_name, "max_results": 8},
                            rationale=f"Expand related-person coverage using Tavily search for discovered person: {target_name}",
                        )
                    )
                if has_tavily_search and not _receipt_has_value(state, "google_serp_person_search", {"targetName": target_name}):
                    plan.append(
                        ToolPlanItem(
                            tool="google_serp_person_search",
                            arguments={"runId": state["run_id"], "target_name": target_name, "max_results": 8},
                            rationale=f"Fallback related-person coverage via Google SERP for discovered person: {target_name}",
                        )
                    )
                if has_tavily_search and not _receipt_has_value(state, "person_search", {"name": target_name}):
                    plan.append(
                        ToolPlanItem(
                            tool="person_search",
                            arguments={"runId": state["run_id"], "name": target_name, "max_results": 8},
                            rationale=f"Collect biography, contact, and relationship clues for discovered related person: {target_name}",
                        )
                    )

            for email in emails[:6]:
                if not _receipt_has_argument_signature(state, "package_registry_search", {"email": email}):
                    plan.append(
                        ToolPlanItem(
                            tool="package_registry_search",
                            arguments={"runId": state["run_id"], "email": email, "max_results": 5},
                            rationale=f"Search public package registries for the discovered public email pivot: {email}",
                        )
                    )
                if not _receipt_has_value(state, "osint_holehe_email", {"email": email}):
                    plan.append(
                        ToolPlanItem(
                            tool="osint_holehe_email",
                            arguments={"runId": state["run_id"], "email": email},
                            rationale=f"Investigate discovered public email/account pivot: {email}",
                        )
                    )

            for phone in phone_numbers[:4]:
                if not _receipt_has_value(state, "osint_phoneinfoga_number", {"target": phone, "phone": phone, "number": phone}):
                    plan.append(
                        ToolPlanItem(
                            tool="osint_phoneinfoga_number",
                            arguments={"runId": state["run_id"], "number": phone},
                            rationale=f"Investigate discovered public phone pivot: {phone}",
                        )
                    )

            for domain in domains[:4]:
                if not _receipt_has_argument_signature(state, "domain_whois_search", {"domain": domain}):
                    plan.append(
                        ToolPlanItem(
                            tool="domain_whois_search",
                            arguments={"runId": state["run_id"], "domain": domain, "max_results": 5},
                            rationale=f"Resolve RDAP ownership for discovered domain pivot: {domain}",
                        )
                    )
                if primary_person_targets[:1] and not _receipt_has_argument_signature(state, "email_pattern_inference", {"domain": domain, "person_name": primary_person_targets[0]}):
                    plan.append(
                        ToolPlanItem(
                            tool="email_pattern_inference",
                            arguments={"runId": state["run_id"], "domain": domain, "person_name": primary_person_targets[0]},
                            rationale=f"Infer likely public email patterns for discovered domain pivot: {domain}",
                        )
                    )
                if not _receipt_has_value(state, "osint_theharvester_email_domain", {"domain": domain}):
                    plan.append(
                        ToolPlanItem(
                            tool="osint_theharvester_email_domain",
                            arguments={"runId": state["run_id"], "domain": domain, "source": "all", "limit": 50},
                            rationale=f"Expand email and host coverage for discovered domain pivot: {domain}",
                        )
                    )

            for username in usernames[:6]:
                if not _receipt_has_argument_signature(state, "username_permutation_search", {"username": username}):
                    plan.append(
                        ToolPlanItem(
                            tool="username_permutation_search",
                            arguments={"runId": state["run_id"], "username": username},
                            rationale=f"Check direct cross-platform URL permutations for discovered username pivot: {username}",
                        )
                    )
                if not _receipt_has_argument_signature(state, "github_identity_search", {"username": username}):
                    plan.append(
                        ToolPlanItem(
                            tool="github_identity_search",
                            arguments={"runId": state["run_id"], "username": username, "max_results": 5},
                            rationale=f"Resolve whether the discovered username pivot has a GitHub code identity: {username}",
                        )
                    )
                if not _receipt_has_argument_signature(state, "gitlab_identity_search", {"username": username}):
                    plan.append(
                        ToolPlanItem(
                            tool="gitlab_identity_search",
                            arguments={"runId": state["run_id"], "username": username, "max_results": 5},
                            rationale=f"Resolve whether the discovered username pivot has a GitLab code identity: {username}",
                        )
                    )
                if not _receipt_has_argument_signature(state, "package_registry_search", {"username": username}):
                    plan.append(
                        ToolPlanItem(
                            tool="package_registry_search",
                            arguments={"runId": state["run_id"], "username": username, "max_results": 5},
                            rationale=f"Search public package registries for discovered maintainer-handle pivot: {username}",
                        )
                    )
                if not _receipt_has_value(state, "osint_maigret_username", {"username": username}):
                    plan.append(
                        ToolPlanItem(
                            tool="osint_maigret_username",
                            arguments={"runId": state["run_id"], "username": username},
                            rationale=f"Expand social/profile coverage for discovered username pivot: {username}",
                        )
                    )
                if not _receipt_has_value(state, "x_get_user_posts_api", {"username": username}):
                    plan.append(
                        ToolPlanItem(
                            tool="x_get_user_posts_api",
                            arguments={"runId": state["run_id"], "username": username, "max_results": 10},
                            rationale=f"Collect public posts for discovered username pivot: @{username}",
                        )
                    )
                if not _receipt_has_argument_signature(state, "reddit_user_search", {"username": username}):
                    plan.append(
                        ToolPlanItem(
                            tool="reddit_user_search",
                            arguments={"runId": state["run_id"], "username": username},
                            rationale=f"Resolve a public Reddit profile for discovered username pivot: {username}",
                        )
                    )
                if not _receipt_has_argument_signature(state, "medium_author_search", {"username": username}):
                    plan.append(
                        ToolPlanItem(
                            tool="medium_author_search",
                            arguments={"runId": state["run_id"], "username": username},
                            rationale=f"Resolve a public Medium author profile for discovered username pivot: {username}",
                        )
                    )

            for profile in linkedin_profiles[:6]:
                if not _receipt_has_value(state, "linkedin_download_html_ocr", {"profile": profile}):
                    plan.append(
                        ToolPlanItem(
                            tool="linkedin_download_html_ocr",
                            arguments={"runId": state["run_id"], "profile": profile},
                            rationale=f"Capture LinkedIn evidence for discovered person/institution profile: {profile}",
                        )
                    )

        plan = _dedupe_tool_plan(plan)
        plan = _filter_completed_tool_plan(state, plan)
        uncapped_count = len(plan)
        if len(plan) > STAGE1_MAX_TOOLS_PER_ITERATION:
            plan = plan[:STAGE1_MAX_TOOLS_PER_ITERATION]
            logger.info(
                "Planner tool plan capped",
                extra={
                    "requested_count": uncapped_count,
                    "capped_count": len(plan),
                    "stage1_max_tools_per_iteration": STAGE1_MAX_TOOLS_PER_ITERATION,
                },
            )
        current_fetch_urls = _fetch_urls_from_plan(plan)

        logger.info("Planner tool plan created", extra={"count": len(plan)})
        return {
            **state,
            "seed_urls": seed_urls,
            "pending_urls": pending_urls,
            "allowed_hosts": allowed_hosts,
            "tool_plan": plan,
            "current_fetch_urls": current_fetch_urls,
            "rationale": rationale,
            "enough_info": enough_info,
            "queued_tasks": remaining_queued_tasks,
        }

    def explain_plan(state: PlannerState) -> PlannerState:
        rationale = state.get("rationale") or (
            "No URLs found in input. Planner will wait for more seeds."
            if not state.get("tool_plan")
            else "\n".join([item.rationale for item in state["tool_plan"]])
        )
        emit_run_event(state["run_id"], "TOOLS_SELECTED", {"rationale": rationale, "tools": [
                       item.model_dump() for item in state.get("tool_plan", [])]})
        logger.info("Planner plan explained", extra={
                    "tool_count": len(state.get("tool_plan", []))})
        return {**state, "rationale": rationale}

    def execute_tools(state: PlannerState) -> PlannerState:
        # Fan out tool-worker calls for the current planner round.
        latest_receipts: List[ToolReceipt] = []
        tool_plan = list(state.get("tool_plan", []))

        def execute_plan_item(index: int, item: ToolPlanItem) -> tuple[int, List[ToolReceipt]]:
            receipts: List[ToolReceipt] = []
            worker_result = run_tool_worker(
                mcp_client, state["run_id"], item.tool, item.arguments)
            receipt = worker_result.receipt
            receipts.append(receipt)
            logger.info("Planner executed tool", extra={
                        "tool": item.tool, "ok": receipt.ok})

            auto_entities = _build_auto_graph_entities(
                item.tool, item.arguments, worker_result.result)
            if auto_entities:
                ingest_result = run_tool_worker(
                    mcp_client,
                    state["run_id"],
                    "ingest_graph_entities",
                    {
                        "runId": state["run_id"],
                        "entitiesJson": auto_entities,
                    },
                )
                ingest_receipt = ingest_result.receipt
                receipts.append(ingest_receipt)
                logger.info(
                    "Planner auto-ingested graph entities",
                    extra={
                        "source_tool": item.tool,
                        "entity_count": len(auto_entities),
                        "ok": ingest_receipt.ok,
                    },
                )

            return index, receipts

        if worker_limit == 1 or len(tool_plan) <= 1:
            for index, item in enumerate(tool_plan):
                _, receipts = execute_plan_item(index, item)
                latest_receipts.extend(receipts)
        else:
            ordered_receipts: Dict[int, List[ToolReceipt]] = {}
            with ThreadPoolExecutor(max_workers=worker_limit) as executor:
                futures = [
                    executor.submit(execute_plan_item, index, item)
                    for index, item in enumerate(tool_plan)
                ]
                for future in as_completed(futures):
                    index, receipts = future.result()
                    ordered_receipts[index] = receipts

            for index in range(len(tool_plan)):
                latest_receipts.extend(ordered_receipts.get(index, []))

        emit_run_event(
            state["run_id"],
            "TOOL_WORKERS_FANOUT_COMPLETED",
            {"receipt_count": len(latest_receipts), "maxWorker": worker_limit},
        )
        return {**state, "latest_tool_receipts": latest_receipts}

    def planner_review_receipts(state: PlannerState) -> PlannerState:
        latest_receipts = list(state.get("latest_tool_receipts", []))
        all_receipts = list(state.get("tool_receipts", []))
        documents_created = list(state.get("documents_created", []))
        noteboard = list(state.get("noteboard", []))
        pending_urls = list(state.get("pending_urls", []))
        current_fetch_urls = list(state.get("current_fetch_urls", []))
        visited_urls = list(state.get("visited_urls", []))
        allowed_hosts = list(state.get("allowed_hosts", []))
        discovered_urls: List[str] = []
        queued_tasks = list(state.get("queued_tasks", []))
        academic_task_dedupe = dict(state.get("academic_task_dedupe", {}))
        technical_task_dedupe = dict(state.get("technical_task_dedupe", {}))
        business_task_dedupe = dict(state.get("business_task_dedupe", {}))
        archive_identity_task_dedupe = dict(state.get("archive_identity_task_dedupe", {}))
        relationship_task_dedupe = dict(state.get("relationship_task_dedupe", {}))

        for receipt in latest_receipts:
            all_receipts.append(receipt)
            for document_id in receipt.document_ids:
                if document_id:
                    documents_created.append(document_id)
            note = _format_receipt_note(receipt)
            if note:
                noteboard.append(note)
            if receipt.tool_name == "fetch_url":
                source_url = _extract_fetch_receipt_url(receipt)
                if source_url:
                    visited_urls.append(source_url)
            for hint in receipt.next_hints:
                discovered = _normalize_crawl_url(hint)
                if discovered:
                    discovered_urls.append(discovered)

        visited_urls = _dedupe(visited_urls + current_fetch_urls)
        filtered_discovered_urls = _filter_discovered_urls(
            discovered_urls, allowed_hosts, visited_urls)
        pending_urls = _dedupe(
            [url for url in pending_urls if url not in set(current_fetch_urls)]
            + filtered_discovered_urls
        )
        if filtered_discovered_urls:
            noteboard.append(
                f"Discovered {len(filtered_discovered_urls)} in-scope internal URL(s) for follow-up fetch."
            )

        primary_person_targets = _extract_primary_person_targets(state)
        entity_resolution_follow_up_tasks, archive_identity_task_dedupe, entity_resolution_notes = _derive_entity_resolution_follow_up_tasks(
            run_id=state["run_id"],
            receipts=all_receipts,
            iteration=state.get("iteration", 0),
            dedupe_store=archive_identity_task_dedupe,
        )
        if entity_resolution_follow_up_tasks:
            queued_tasks.extend(
                [
                    {
                        "tool_name": task.tool_name,
                        "payload": task.payload,
                        "priority": task.priority,
                        "reason": task.reason,
                        "dedupe_key": task.dedupe_key,
                    }
                    for task in entity_resolution_follow_up_tasks
                ]
            )
            noteboard.append(
                f"Queued {len(entity_resolution_follow_up_tasks)} deterministic identity-resolution follow-up task(s)."
            )
        noteboard.extend(entity_resolution_notes)

        follow_up_tasks, academic_task_dedupe, academic_notes = derive_academic_follow_up_tasks(
            run_id=state["run_id"],
            receipts=latest_receipts,
            primary_person_targets=primary_person_targets,
            iteration=state.get("iteration", 0),
            dedupe_store=academic_task_dedupe,
        )
        if follow_up_tasks:
            queued_tasks.extend(
                [
                    {
                        "tool_name": task.tool_name,
                        "payload": task.payload,
                        "priority": task.priority,
                        "reason": task.reason,
                        "dedupe_key": task.dedupe_key,
                    }
                    for task in follow_up_tasks
                ]
            )
            noteboard.append(f"Queued {len(follow_up_tasks)} deterministic academic follow-up task(s).")
        noteboard.extend(academic_notes)

        technical_follow_up_tasks, technical_task_dedupe, technical_notes = derive_technical_follow_up_tasks(
            run_id=state["run_id"],
            receipts=latest_receipts,
            primary_person_targets=primary_person_targets,
            iteration=state.get("iteration", 0),
            dedupe_store=technical_task_dedupe,
        )
        if technical_follow_up_tasks:
            queued_tasks.extend(
                [
                    {
                        "tool_name": task.tool_name,
                        "payload": task.payload,
                        "priority": task.priority,
                        "reason": task.reason,
                        "dedupe_key": task.dedupe_key,
                    }
                    for task in technical_follow_up_tasks
                ]
            )
            noteboard.append(
                f"Queued {len(technical_follow_up_tasks)} deterministic technical follow-up task(s)."
            )
        noteboard.extend(technical_notes)

        business_follow_up_tasks, business_task_dedupe, business_notes = derive_business_follow_up_tasks(
            run_id=state["run_id"],
            receipts=latest_receipts,
            primary_person_targets=primary_person_targets,
            iteration=state.get("iteration", 0),
            dedupe_store=business_task_dedupe,
        )
        if business_follow_up_tasks:
            queued_tasks.extend(
                [
                    {
                        "tool_name": task.tool_name,
                        "payload": task.payload,
                        "priority": task.priority,
                        "reason": task.reason,
                        "dedupe_key": task.dedupe_key,
                    }
                    for task in business_follow_up_tasks
                ]
            )
            noteboard.append(
                f"Queued {len(business_follow_up_tasks)} deterministic business follow-up task(s)."
            )
        noteboard.extend(business_notes)

        archive_identity_follow_up_tasks, archive_identity_task_dedupe, archive_identity_notes = derive_archive_identity_follow_up_tasks(
            run_id=state["run_id"],
            receipts=latest_receipts,
            primary_person_targets=primary_person_targets,
            iteration=state.get("iteration", 0),
            dedupe_store=archive_identity_task_dedupe,
        )
        if archive_identity_follow_up_tasks:
            queued_tasks.extend(
                [
                    {
                        "tool_name": task.tool_name,
                        "payload": task.payload,
                        "priority": task.priority,
                        "reason": task.reason,
                        "dedupe_key": task.dedupe_key,
                    }
                    for task in archive_identity_follow_up_tasks
                ]
            )
            noteboard.append(
                f"Queued {len(archive_identity_follow_up_tasks)} deterministic archive/identity follow-up task(s)."
            )
        noteboard.extend(archive_identity_notes)

        relationship_follow_up_tasks, relationship_task_dedupe, relationship_notes = derive_relationship_follow_up_tasks(
            run_id=state["run_id"],
            receipts=latest_receipts,
            primary_person_targets=primary_person_targets,
            iteration=state.get("iteration", 0),
            dedupe_store=relationship_task_dedupe,
        )
        if relationship_follow_up_tasks:
            queued_tasks.extend(
                [
                    {
                        "tool_name": task.tool_name,
                        "payload": task.payload,
                        "priority": task.priority,
                        "reason": task.reason,
                        "dedupe_key": task.dedupe_key,
                    }
                    for task in relationship_follow_up_tasks
                ]
            )
            noteboard.append(
                f"Queued {len(relationship_follow_up_tasks)} deterministic relationship follow-up task(s)."
            )
        noteboard.extend(relationship_notes)

        consistency_follow_up_tasks, academic_task_dedupe, consistency_notes = _derive_consistency_follow_up_tasks(
            run_id=state["run_id"],
            receipts=all_receipts,
            primary_person_targets=primary_person_targets,
            iteration=state.get("iteration", 0),
            dedupe_store=academic_task_dedupe,
        )
        if consistency_follow_up_tasks:
            queued_tasks.extend(
                [
                    {
                        "tool_name": task.tool_name,
                        "payload": task.payload,
                        "priority": task.priority,
                        "reason": task.reason,
                        "dedupe_key": task.dedupe_key,
                    }
                    for task in consistency_follow_up_tasks
                ]
            )
            noteboard.append(
                f"Queued {len(consistency_follow_up_tasks)} contradiction-resolution follow-up task(s)."
            )
        noteboard.extend(consistency_notes)

        coverage_ledger = _derive_coverage_ledger({**state, "tool_receipts": all_receipts, "noteboard": noteboard})

        noteboard = _trim_noteboard(noteboard)
        emit_run_event(
            state["run_id"],
            "NOTEBOARD_UPDATED",
            {"notes": noteboard},
        )
        logger.info("Planner noteboard updated", extra={
                    "note_count": len(noteboard)})
        return {
            **state,
            "tool_receipts": all_receipts,
            "documents_created": documents_created,
            "noteboard": noteboard,
            "pending_urls": pending_urls,
            "current_fetch_urls": [],
            "visited_urls": visited_urls,
            "queued_tasks": queued_tasks,
            "academic_task_dedupe": academic_task_dedupe,
            "technical_task_dedupe": technical_task_dedupe,
            "business_task_dedupe": business_task_dedupe,
            "archive_identity_task_dedupe": archive_identity_task_dedupe,
            "relationship_task_dedupe": relationship_task_dedupe,
            "coverage_ledger": coverage_ledger,
        }

    def decide_stop_or_refine(state: PlannerState) -> PlannerState:
        iteration = state.get("iteration", 0) + 1
        coverage_ledger = _derive_coverage_ledger(state)
        coverage_ok = coverage_led_stop_condition(coverage_ledger)
        done = (
            iteration >= state.get("max_iterations", 1)
            or not state.get("tool_plan")
            or coverage_ok
        )
        next_stage = "stage2" if done else "stage1"
        return {
            **state,
            "iteration": iteration,
            "done": done,
            "next_stage": next_stage,
            "coverage_ledger": coverage_ledger,
        }

    def should_continue(state: PlannerState) -> str:
        return END if state.get("done") else "plan_tools"

    graph.add_node("analyze_input", analyze_input)
    graph.add_node("plan_tools", plan_tools)
    graph.add_node("explain_plan", explain_plan)
    graph.add_node("execute_tools", execute_tools)
    graph.add_node("planner_review_receipts", planner_review_receipts)
    graph.add_node("decide_stop_or_refine", decide_stop_or_refine)

    graph.set_entry_point("analyze_input")
    graph.add_edge("analyze_input", "plan_tools")
    graph.add_edge("plan_tools", "explain_plan")
    graph.add_edge("explain_plan", "execute_tools")
    graph.add_edge("execute_tools", "planner_review_receipts")
    graph.add_edge("planner_review_receipts", "decide_stop_or_refine")
    graph.add_conditional_edges("decide_stop_or_refine", should_continue)

    return graph


def run_planner(
    run_id: str,
    prompt: str,
    inputs: List[str] | None = None,
    max_iterations: int = 3,
    max_worker: int = DEFAULT_MAX_WORKER,
) -> PlannerResult:
    load_env()
    emit_run_event(run_id, "PLANNER_STARTED", {})
    llm: OpenRouterLLM | None = None
    if os.getenv("OPENROUTER_API_KEY"):
        planner_model = os.getenv(
            "OPENROUTER_PLANNER_MODEL") or os.getenv("OPENROUTER_MODEL")
        llm = OpenRouterLLM(model=planner_model)

    run_title = _derive_run_title(prompt, inputs or [], llm)
    _persist_run_title(run_id, run_title)
    emit_run_event(run_id, "RUN_TITLE_SET", {"title": run_title})

    mcp_client = RoutedMcpClient()
    mcp_client.start()

    try:
        graph = build_planner_graph(mcp_client, llm, max_worker=max_worker)
        state: PlannerState = {
            "run_id": run_id,
            "prompt": prompt,
            "inputs": inputs or [],
            "seed_urls": [],
            "pending_urls": [],
            "current_fetch_urls": [],
            "visited_urls": [],
            "allowed_hosts": [],
            "tool_plan": [],
            "latest_tool_receipts": [],
            "rationale": "",
            "documents_created": [],
            "tool_receipts": [],
            "iteration": 0,
            "max_iterations": max_iterations,
            "done": False,
            "enough_info": False,
            "noteboard": [],
            "next_stage": "stage1",
            "queued_tasks": [],
            "academic_task_dedupe": {},
            "technical_task_dedupe": {},
            "business_task_dedupe": {},
            "archive_identity_task_dedupe": {},
            "relationship_task_dedupe": {},
            "coverage_ledger": empty_coverage_ledger(),
        }

        final_state = graph.compile().invoke(state)
        logger.info("Planner run complete", extra={
                    "run_id": run_id, "iterations": final_state.get("iteration", 0)})
        return PlannerResult(
            run_id=run_id,
            tool_plan=final_state.get("tool_plan", []),
            documents_created=final_state.get("documents_created", []),
            rationale=final_state.get("rationale", ""),
            tool_receipts=final_state.get("tool_receipts", []),
            iterations=final_state.get("iteration", 0),
            noteboard=final_state.get("noteboard", []),
            next_stage=final_state.get("next_stage", "stage1"),
        )
    finally:
        mcp_client.close()


def _extract_urls(text: str) -> List[str]:
    return URL_REGEX.findall(text or "")


def _extract_emails(text: str) -> List[str]:
    return EMAIL_REGEX.findall(text or "")


def _extract_domains(text: str) -> List[str]:
    return DOMAIN_REGEX.findall(text or "")


def _extract_usernames(text: str) -> List[str]:
    return USERNAME_REGEX.findall(text or "")


def _extract_phone_numbers(text: str) -> List[str]:
    raw = PHONE_REGEX.findall(text or "")
    numbers: List[str] = []
    for item in raw:
        normalized = item.strip()
        if normalized and any(ch in normalized for ch in " +-.()") and not _looks_like_dateish_phone_candidate(normalized):
            numbers.append(normalized)
    return numbers


def _looks_like_dateish_phone_candidate(value: str) -> bool:
    compact = value.strip()
    if not compact:
        return False
    compact = compact.strip("()")
    compact = re.sub(r"\s+", "", compact)
    if not DATE_LIKE_PHONE_REGEX.fullmatch(compact):
        return False
    parts = re.split(r"[-/.]", compact)
    if len(parts) != 3:
        return False
    try:
        first = int(parts[0])
        second = int(parts[1])
        third = int(parts[2])
    except ValueError:
        return False
    if len(parts[0]) == 4:
        return 1 <= second <= 12 and 1 <= third <= 31
    if len(parts[2]) == 4:
        return 1 <= first <= 12 and 1 <= second <= 31
    return False


def _extract_ipv4(text: str) -> List[str]:
    return IPV4_REGEX.findall(text or "")


def _extract_domains_from_state(state: PlannerState) -> List[str]:
    explicit_domains: List[str] = []
    for item in [state.get("prompt", "")] + list(state.get("inputs", [])):
        explicit_domains.extend(_extract_domains(item))
        for url in _extract_urls(item):
            host = _domain_from_url(url)
            if host:
                explicit_domains.append(host)

    scoped_domains: List[str] = list(state.get("allowed_hosts", []))
    for url in (
        list(state.get("seed_urls", []))
        + list(state.get("pending_urls", []))
        + list(state.get("current_fetch_urls", []))
        + list(state.get("visited_urls", []))
    ):
        host = _domain_from_url(url)
        if host:
            scoped_domains.append(host)

    email_domains: List[str] = []
    for email in _extract_emails_from_state(state):
        _, _, domain = email.rpartition("@")
        if domain:
            email_domains.append(domain)

    derived_domains = [
        domain
        for domain in scoped_domains + email_domains
        if _is_domain_recon_candidate(domain)
    ]
    return _dedupe(explicit_domains + derived_domains)


def _extract_emails_from_state(state: PlannerState) -> List[str]:
    emails: List[str] = []
    combined = _state_text_corpus(state)
    for item in combined:
        emails.extend(_extract_emails(item))
    return _dedupe(emails)


def _extract_usernames_from_state(state: PlannerState) -> List[str]:
    usernames: List[str] = []
    combined = _state_text_corpus(state)
    for item in combined:
        usernames.extend(_extract_usernames(item))
    return _dedupe(usernames)


def _extract_linkedin_profiles_from_state(state: PlannerState) -> List[str]:
    profiles: List[str] = []
    combined = _state_text_corpus(state)
    for item in combined:
        for url in _extract_urls(item):
            parsed = urlparse(url)
            host = (parsed.hostname or "").lower()
            if host.startswith("www."):
                host = host[4:]
            if host != "linkedin.com":
                continue
            if parsed.path.startswith("/in/") or parsed.path.startswith("/company/"):
                profiles.append(url)
    return _dedupe(profiles)


def _extract_phone_numbers_from_state(state: PlannerState) -> List[str]:
    numbers: List[str] = []
    combined = _state_text_corpus(state)
    for item in combined:
        numbers.extend(_extract_phone_numbers(item))
    return _dedupe(numbers)


def _extract_ipv4_from_state(state: PlannerState) -> List[str]:
    hosts: List[str] = []
    combined = _state_text_corpus(state)
    for item in combined:
        hosts.extend(_extract_ipv4(item))
    return _dedupe(hosts)


def _extract_person_targets_from_state(state: PlannerState) -> List[str]:
    return _dedupe(_extract_primary_person_targets(state) + _extract_related_person_targets_from_receipts(state))


def _extract_primary_person_targets(state: PlannerState) -> List[str]:
    candidates: List[str] = []
    candidates.extend(extract_person_targets(state.get("prompt", "") or ""))
    for item in state.get("inputs", []):
        candidates.extend(extract_person_targets(item or ""))
    return _dedupe(candidates)


def _state_text_corpus(state: PlannerState) -> List[str]:
    texts = [state.get("prompt", "")] + list(state.get("inputs", [])) + list(state.get("noteboard", []))
    for receipt in state.get("tool_receipts", []):
        texts.append(receipt.summary)
        for fact in receipt.key_facts:
            if not isinstance(fact, dict):
                continue
            for value in fact.values():
                if isinstance(value, str):
                    texts.append(value)
                elif isinstance(value, list):
                    texts.extend([str(item) for item in value if isinstance(item, str)])
    return texts


def _extract_related_person_targets_from_receipts(state: PlannerState) -> List[str]:
    candidates: List[str] = []
    interesting_keys = {
        "relatedPeople",
        "coauthors",
        "authors",
        "advisor",
        "advisors",
        "collaborators",
        "colleagues",
        "mentors",
        "labMembers",
    }
    for receipt in state.get("tool_receipts", []):
        for fact in receipt.key_facts:
            if not isinstance(fact, dict):
                continue
            for key, value in fact.items():
                if key not in interesting_keys:
                    continue
                if isinstance(value, str):
                    candidates.extend(extract_person_targets(value))
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, str):
                            candidates.extend(extract_person_targets(item))
    return _dedupe(candidates)


def _receipt_has_value(state: PlannerState, tool_name: str, expected: Dict[str, str]) -> bool:
    normalized_expected = {key: value.strip().casefold() for key, value in expected.items() if isinstance(value, str) and value.strip()}
    if not normalized_expected:
        return False
    for receipt in state.get("tool_receipts", []):
        if receipt.tool_name != tool_name:
            continue
        for fact in receipt.key_facts:
            if not isinstance(fact, dict):
                continue
            matched = False
            for key, value in normalized_expected.items():
                for candidate_key in (key, key.lower(), key.replace("Name", "_name"), key.replace("_name", "Name")):
                    candidate = fact.get(candidate_key)
                    if isinstance(candidate, str) and candidate.strip().casefold() == value:
                        matched = True
                        break
                if matched:
                    break
            if matched:
                return True
    return False


def _receipt_has_argument_signature(state: PlannerState, tool_name: str, expected: Dict[str, Any]) -> bool:
    if not expected:
        return False
    expected_signature = tool_argument_signature(tool_name, expected)
    if expected_signature == tool_argument_signature(tool_name, {}):
        return False
    for receipt in state.get("tool_receipts", []):
        if receipt.tool_name != tool_name:
            continue
        if receipt.argument_signature == expected_signature:
            return True
        if receipt.arguments and tool_argument_signature(tool_name, receipt.arguments) == expected_signature:
            return True
        lowered_expected = {
            key: str(value).strip().casefold()
            for key, value in expected.items()
            if key != "runId" and value is not None and str(value).strip()
        }
        if not lowered_expected:
            return False
        fact_values: List[str] = []
        for fact in receipt.key_facts:
            if not isinstance(fact, dict):
                continue
            for value in fact.values():
                if isinstance(value, str):
                    fact_values.append(value.casefold())
                elif isinstance(value, list):
                    fact_values.extend(str(item).casefold() for item in value if isinstance(item, (str, int, float)))
        summary = receipt.summary.casefold()
        if all(
            expected_value in summary or any(expected_value == item or expected_value in item for item in fact_values)
            for expected_value in lowered_expected.values()
        ):
            return True
    return False


def _planner_has_minimum_person_coverage(state: PlannerState) -> bool:
    primary_targets = _extract_primary_person_targets(state)
    if not primary_targets:
        return True
    emails = _extract_emails_from_state(state)
    phones = _extract_phone_numbers_from_state(state)
    person_targets = _extract_person_targets_from_state(state)
    notes_blob = " ".join(_state_text_corpus(state)).lower()
    relationship_signal = len(person_targets) > len(primary_targets) or any(
        marker in notes_blob for marker in ("co-author", "coauthor", "advisor", "colleague", "collaborator", "works at")
    )
    history_signal = any(
        marker in notes_blob for marker in (
            "university",
            "phd",
            "student",
            "publication",
            "paper",
            "research",
            "history",
            "worked at",
            "joined",
            "former",
            "crime",
            "arrest",
            "court",
        )
    )
    contact_signal = bool(emails or phones or _extract_linkedin_profiles_from_state(state))
    return history_signal and relationship_signal and contact_signal


def _derive_coverage_ledger(state: PlannerState) -> Dict[str, bool]:
    ledger = empty_coverage_ledger()
    receipts = [receipt for receipt in state.get("tool_receipts", []) if receipt.ok]
    notes_blob = " ".join(state.get("noteboard", []) + _state_text_corpus(state)).lower()

    identity_tools = {
        "person_search",
        "tavily_research",
        "tavily_person_search",
        "google_serp_person_search",
        "linkedin_download_html_ocr",
        "github_identity_search",
        "alias_variant_generator",
        "username_permutation_search",
        "cross_platform_profile_resolver",
        "institution_directory_search",
        "reddit_user_search",
        "mastodon_profile_search",
        "substack_author_search",
        "medium_author_search",
        "orcid_search",
        "semantic_scholar_search",
        "dblp_author_search",
    }
    academic_tools = {
        "orcid_search",
        "semantic_scholar_search",
        "dblp_author_search",
        "pubmed_author_search",
        "grant_search_person",
        "conference_profile_search",
    }
    code_tools = {"github_identity_search", "gitlab_identity_search", "bitbucket_identity_search", "huggingface_profile_search"}
    package_tools = {"package_registry_search", "npm_author_search", "pypi_author_search", "crates_author_search", "dockerhub_profile_search"}
    business_tools = {
        "company_officer_search",
        "company_filing_search",
        "open_corporates_search",
        "sec_person_search",
        "director_disclosure_search",
    }
    archive_tools = {"wayback_fetch_url", "wayback_domain_timeline_search", "archived_profile_search", "cached_page_fetch", "historical_bio_diff"}
    public_record_tools = {
        "public_records_search",
        "court_record_search",
        "regulatory_search",
        "sanctions_watchlist_search",
        "campaign_finance_search",
        "lobbying_registry_search",
        "professional_license_search",
    }
    relationship_tools = {
        "coauthor_graph_search",
        "org_staff_page_search",
        "board_member_overlap_search",
        "shared_contact_pivot_search",
    }

    for receipt in receipts:
        if receipt.tool_name in identity_tools:
            ledger["identity"] = True
        if receipt.tool_name in academic_tools:
            ledger["academic"] = True
            ledger["history"] = True
        if receipt.tool_name in code_tools or receipt.tool_name in package_tools:
            ledger["code_presence"] = True
        if receipt.tool_name in package_tools:
            ledger["package_publications"] = True
        if receipt.tool_name in business_tools:
            ledger["business_roles"] = True
        if receipt.tool_name in archive_tools:
            ledger["archived_history"] = True
        if receipt.tool_name in public_record_tools:
            ledger["public_records"] = True
        if receipt.tool_name in relationship_tools:
            ledger["relationships"] = True

        for fact in receipt.key_facts:
            if not isinstance(fact, dict):
                continue
            if any(key in fact for key in ("emails", "phones", "contactSignals", "profileUrls", "patterns")):
                ledger["contacts"] = True
            if any(key in fact for key in ("relatedPeople", "coauthors", "organizations", "staff", "overlaps", "sharedDomains", "sharedOrganizations", "sharedAddresses")):
                ledger["relationships"] = True
            if "organizations" in fact and isinstance(fact.get("organizations"), list) and fact.get("organizations"):
                ledger["technical_org_affiliations"] = True
                ledger["relationships"] = True
            if "repositories" in fact and isinstance(fact.get("repositories"), list) and fact.get("repositories"):
                ledger["code_presence"] = True
            if "publications" in fact and isinstance(fact.get("publications"), list) and fact.get("publications"):
                ledger["package_publications"] = True
                ledger["code_presence"] = True

    if any(token in notes_blob for token in ("email", "phone", "contact", "linkedin.com/in/", "github.com/", "personal site")):
        ledger["contacts"] = ledger["contacts"] or ("email" in notes_blob or "phone" in notes_blob or "personal site" in notes_blob)
        ledger["identity"] = ledger["identity"] or ("github.com/" in notes_blob or "linkedin.com/in/" in notes_blob)
    if any(token in notes_blob for token in ("co-author", "coauthor", "advisor", "colleague", "collaborator", "organization affiliation")):
        ledger["relationships"] = True
    if any(token in notes_blob for token in ("publication", "research", "history", "worked at", "joined", "former", "education", "university")):
        ledger["history"] = True
    if any(token in notes_blob for token in ("github profile", "github username", "repository", "repositories", "code identity")):
        ledger["code_presence"] = True
        ledger["identity"] = True
    return ledger


def _fact_list(receipt: ToolReceipt, *keys: str) -> List[Any]:
    values: List[Any] = []
    for fact in receipt.key_facts:
        if not isinstance(fact, dict):
            continue
        for key in keys:
            raw = fact.get(key)
            if isinstance(raw, list):
                values.extend(raw)
    return values


def _fact_scalar(receipt: ToolReceipt, *keys: str) -> Any:
    for fact in receipt.key_facts:
        if not isinstance(fact, dict):
            continue
        for key in keys:
            if key in fact:
                return fact.get(key)
    return None


def _receipt_publication_records(receipt: ToolReceipt) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for item in _fact_list(receipt, "publications", "records", "papers", "extracted_entries"):
        if isinstance(item, dict):
            records.append(item)
    return records


def _receipt_has_publication_signal(receipt: ToolReceipt) -> bool:
    if _receipt_publication_records(receipt):
        return True
    for fact in receipt.key_facts:
        if not isinstance(fact, dict):
            continue
        if any(key in fact for key in ("coauthors", "paperUrls", "citationCount", "citations", "citedBy")):
            return True
        candidates = fact.get("candidates")
        if isinstance(candidates, list):
            for candidate in candidates:
                if not isinstance(candidate, dict):
                    continue
                works_summary = candidate.get("works_summary") if isinstance(candidate.get("works_summary"), dict) else {}
                if any(float(works_summary.get(key) or 0.0) > 0 for key in ("paper_count", "citation_count", "works_count")):
                    return True
    return False


def _receipt_has_relationship_signal(receipt: ToolReceipt) -> bool:
    for fact in receipt.key_facts:
        if not isinstance(fact, dict):
            continue
        if any(key in fact for key in ("relatedPeople", "coauthors", "organizations", "staff", "overlaps", "sharedDomains", "sharedOrganizations", "sharedAddresses")):
            value = next((fact.get(key) for key in ("relatedPeople", "coauthors", "organizations", "staff", "overlaps", "sharedDomains", "sharedOrganizations", "sharedAddresses") if key in fact), None)
            if isinstance(value, list) and value:
                return True
    return False


def _receipt_reports_no_arxiv_results(receipt: ToolReceipt) -> bool:
    if receipt.tool_name != "arxiv_search_and_download":
        return False
    total_available = _fact_scalar(receipt, "total_available", "collected_count")
    if isinstance(total_available, int):
        return total_available == 0
    summary = receipt.summary.lower()
    return "reviewed 0 matched paper" in summary or "no arxiv results" in summary


def _receipt_reports_no_relationships(receipt: ToolReceipt) -> bool:
    summary = receipt.summary.lower()
    return "no collaborators" in summary or "did not reveal any collaborators" in summary or "no coauthor" in summary


def _derive_consistency_follow_up_tasks(
    *,
    run_id: str,
    receipts: List[ToolReceipt],
    primary_person_targets: List[str],
    iteration: int,
    dedupe_store: Dict[str, int],
):
    dedupe_store = prune_dedupe_store(dedupe_store, iteration)
    tasks = []
    notes: List[str] = []
    primary_name = primary_person_targets[:1][0] if primary_person_targets else ""
    if not primary_name:
        return tasks, dedupe_store, notes

    has_publication_signal = any(_receipt_has_publication_signal(receipt) for receipt in receipts if receipt.ok)
    has_relationship_signal = any(_receipt_has_relationship_signal(receipt) for receipt in receipts if receipt.ok)
    arxiv_conflict = has_publication_signal and any(_receipt_reports_no_arxiv_results(receipt) for receipt in receipts if receipt.ok)
    relationship_conflict = has_relationship_signal and any(_receipt_reports_no_relationships(receipt) for receipt in receipts if receipt.ok)

    if arxiv_conflict:
        notes.append(
            "Contradiction detected: arXiv returned zero direct matches while other scholarly evidence suggests publications; continuing academic verification."
        )
        add_task_if_new(
            tasks,
            dedupe_store,
            iteration,
            tool_name="semantic_scholar_search",
            payload={"runId": run_id, "person_name": primary_name, "max_results": 10},
            priority=PRIORITY_HIGH,
            reason="Resolve publication contradiction: arXiv absence does not explain broader scholarly evidence.",
        )
        add_task_if_new(
            tasks,
            dedupe_store,
            iteration,
            tool_name="dblp_author_search",
            payload={"runId": run_id, "person_name": primary_name, "max_results": 10},
            priority=PRIORITY_HIGH,
            reason="Resolve publication contradiction with authoritative bibliographic coverage.",
        )
        add_task_if_new(
            tasks,
            dedupe_store,
            iteration,
            tool_name="conference_profile_search",
            payload={"runId": run_id, "person_name": primary_name, "max_results": 10},
            priority=PRIORITY_HIGH,
            reason="Resolve venue-level publication contradiction when arXiv coverage is incomplete.",
        )

    if relationship_conflict:
        publication_data: List[Dict[str, Any]] = []
        for receipt in receipts:
            publication_data.extend(_receipt_publication_records(receipt))
        if publication_data:
            notes.append(
                "Contradiction detected: collaborator absence claim conflicts with publication-derived relationship evidence; rebuilding coauthor graph."
            )
            add_task_if_new(
                tasks,
                dedupe_store,
                iteration,
                tool_name="coauthor_graph_search",
                payload={"runId": run_id, "person_name": primary_name, "publication_data": publication_data[:30]},
                priority=PRIORITY_HIGH,
                reason="Resolve collaborator contradiction with publication-derived coauthor graph.",
            )

    return tasks, dedupe_store, notes


def _profile_candidates_from_receipts(receipts: List[ToolReceipt]) -> List[Dict[str, Any]]:
    profiles: List[Dict[str, Any]] = []
    for receipt in receipts:
        profile: Dict[str, Any] = {"platform": receipt.tool_name}
        for fact in receipt.key_facts:
            if not isinstance(fact, dict):
                continue
            if isinstance(fact.get("profileUrl"), str) and fact["profileUrl"].strip():
                profile["profile_url"] = fact["profileUrl"].strip()
            if isinstance(fact.get("username"), str) and fact["username"].strip():
                profile["username"] = fact["username"].strip()
            if isinstance(fact.get("displayName"), str) and fact["displayName"].strip():
                profile["name"] = fact["displayName"].strip()
            candidates = fact.get("candidates")
            if isinstance(candidates, list) and candidates:
                candidate = candidates[0]
                if isinstance(candidate, dict):
                    if isinstance(candidate.get("canonical_name"), str) and candidate["canonical_name"].strip():
                        profile["name"] = candidate["canonical_name"].strip()
                    if isinstance(candidate.get("affiliations"), list):
                        profile["affiliations"] = [str(item).strip() for item in candidate.get("affiliations", []) if str(item).strip()]
                    if isinstance(candidate.get("evidence"), list):
                        publications = []
                        for evidence in candidate.get("evidence", []):
                            if isinstance(evidence, dict) and isinstance(evidence.get("snippet"), str) and evidence["snippet"].strip():
                                publications.append({"title": evidence["snippet"].strip()})
                        if publications:
                            profile["publications"] = publications
        if len(profile) > 1:
            profiles.append(profile)
    deduped: Dict[str, Dict[str, Any]] = {}
    for profile in profiles:
        key = str(profile.get("profile_url") or profile.get("username") or profile.get("name") or "")
        if key:
            deduped[key] = profile
    return list(deduped.values())


def _derive_entity_resolution_follow_up_tasks(
    *,
    run_id: str,
    receipts: List[ToolReceipt],
    iteration: int,
    dedupe_store: Dict[str, int],
):
    dedupe_store = prune_dedupe_store(dedupe_store, iteration)
    tasks = []
    notes: List[str] = []
    profiles = _profile_candidates_from_receipts(receipts)
    if len(profiles) >= 2:
        add_task_if_new(
            tasks,
            dedupe_store,
            iteration,
            tool_name="cross_platform_profile_resolver",
            payload={"runId": run_id, "profiles": profiles[:8]},
            priority=PRIORITY_HIGH,
            reason="Resolve whether discovered academic/social/code profiles map to one canonical identity.",
        )
        notes.append(f"Queued cross-platform identity resolution across {len(profiles[:8])} profile candidates.")
    for receipt in receipts:
        if receipt.tool_name == "cross_platform_profile_resolver":
            for fact in receipt.key_facts:
                if not isinstance(fact, dict):
                    continue
                canonical = fact.get("canonical_identity")
                if isinstance(canonical, dict) and canonical.get("canonical_name"):
                    notes.append(
                        f"EntityResolver merged aliases under canonical identity {canonical['canonical_name']}."
                    )
    return tasks, dedupe_store, notes


def _domain_from_url(url: str) -> str | None:
    match = re.match(r"^https?://([^/:?#]+)", url.strip(), re.IGNORECASE)
    if not match:
        return None
    host = match.group(1).lower().strip()
    if host.startswith("www."):
        return host[4:]
    return host


def _normalize_host(host: str) -> str:
    normalized = host.strip().lower()
    if normalized.startswith("www."):
        normalized = normalized[4:]
    return normalized


def _domain_matches(domain: str, suffix: str) -> bool:
    normalized_domain = _normalize_host(domain)
    normalized_suffix = _normalize_host(suffix)
    return normalized_domain == normalized_suffix or normalized_domain.endswith(f".{normalized_suffix}")


def _is_domain_recon_candidate(domain: str) -> bool:
    normalized = _normalize_host(domain)
    if not normalized or "." not in normalized:
        return False
    return not any(_domain_matches(normalized, blocked) for blocked in DERIVED_DOMAIN_RECON_BLOCKLIST)


def _extract_allowed_hosts(urls: List[str]) -> List[str]:
    hosts: List[str] = []
    for url in urls:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            continue
        hosts.append(_normalize_host(parsed.hostname))
    return _dedupe(hosts)


def _normalize_crawl_url(value: str) -> str | None:
    candidate = value.strip().rstrip(".,)")
    if not candidate:
        return None
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    normalized = parsed._replace(fragment="")
    return normalized.geturl()


def _is_crawlable_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()
    if path.endswith((
        ".css",
        ".js",
        ".json",
        ".xml",
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".svg",
        ".webp",
        ".ico",
        ".woff",
        ".woff2",
        ".ttf",
        ".eot",
        ".map",
        ".zip",
        ".gz",
        ".mp3",
        ".mp4",
        ".mov",
        ".avi",
        ".webm",
    )):
        return False
    return True


def _select_fetch_batch(pending_urls: List[str], visited_urls: List[str], max_urls: int = 5) -> List[str]:
    visited = set(visited_urls)
    batch: List[str] = []
    for url in pending_urls:
        normalized = _normalize_crawl_url(url)
        if not normalized or normalized in visited or not _is_crawlable_url(normalized):
            continue
        batch.append(normalized)
        if len(batch) >= max_urls:
            break
    return batch


def _filter_discovered_urls(urls: List[str], allowed_hosts: List[str], visited_urls: List[str]) -> List[str]:
    allowed = set(_normalize_host(host) for host in allowed_hosts)
    visited = set(visited_urls)
    filtered: List[str] = []
    for url in urls:
        normalized = _normalize_crawl_url(url)
        if not normalized or normalized in visited or not _is_crawlable_url(normalized):
            continue
        host = _normalize_host(urlparse(normalized).hostname or "")
        if allowed and host not in allowed:
            continue
        filtered.append(normalized)
    return _dedupe(filtered)


def _extract_fetch_receipt_url(receipt: ToolReceipt) -> str | None:
    for fact in receipt.key_facts:
        if not isinstance(fact, dict):
            continue
        final_url = fact.get("finalUrl")
        if isinstance(final_url, str):
            normalized = _normalize_crawl_url(final_url)
            if normalized:
                return normalized
        url = fact.get("url")
        if isinstance(url, str):
            normalized = _normalize_crawl_url(url)
            if normalized:
                return normalized
    return None


def _dedupe(items: List[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for item in items:
        normalized = item.strip().rstrip(".,)")
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _normalize_person_candidate(value: str) -> str | None:
    return normalize_person_candidate(value)


def _normalize_llm_tool_plan(
    raw_plan: Any,
    run_id: str,
    allowed_tools: set[str],
    fallback_person_targets: List[str] | None = None,
) -> List[ToolPlanItem]:
    if not isinstance(raw_plan, list):
        return []

    normalized_plan: List[ToolPlanItem] = []
    for item in raw_plan:
        if not isinstance(item, dict):
            continue
        tool = item.get("tool")
        if not isinstance(tool, str) or tool not in allowed_tools:
            continue

        raw_args = item.get("args")
        if not isinstance(raw_args, dict):
            raw_args = item.get("arguments")
        if not isinstance(raw_args, dict):
            raw_args = {}

        arguments = dict(raw_args)
        arguments["runId"] = run_id
        arguments = sanitize_search_tool_arguments(
            tool,
            arguments,
            fallback_person_targets=fallback_person_targets,
        )

        rationale = item.get("rationale") or item.get("reasoning") or f"LLM selected {tool}."
        normalized_plan.append(
            ToolPlanItem(
                tool=tool,
                arguments=arguments,
                rationale=str(rationale),
            )
        )

    return normalized_plan


def _planner_completed_tool_calls(state: PlannerState, limit: int = 25) -> List[Dict[str, Any]]:
    completed_calls: List[Dict[str, Any]] = []
    for receipt in state.get("tool_receipts", []):
        completed_calls.append(
            {
                "tool": receipt.tool_name,
                "arguments": {
                    key: value
                    for key, value in receipt.arguments.items()
                    if key != "runId"
                },
                "ok": bool(receipt.ok),
                "argument_signature": receipt.argument_signature,
                "summary": receipt.summary[:280],
            }
        )
    return completed_calls[-limit:]


def _filter_completed_tool_plan(state: PlannerState, plan: List[ToolPlanItem]) -> List[ToolPlanItem]:
    filtered: List[ToolPlanItem] = []
    for item in plan:
        if _tool_plan_matches_completed_receipt(state, item.tool, item.arguments):
            continue
        filtered.append(item)
    return filtered


def _dedupe_tool_plan(plan: List[ToolPlanItem]) -> List[ToolPlanItem]:
    seen: Dict[str, int] = {}
    deduped: List[ToolPlanItem] = []
    for item in plan:
        key = _tool_plan_dedupe_key(item.tool, item.arguments)
        existing_index = seen.get(key)
        if existing_index is None:
            seen[key] = len(deduped)
            deduped.append(item)
            continue
        deduped[existing_index] = _merge_tool_plan_items(deduped[existing_index], item)
    return deduped


def _tool_plan_dedupe_key(tool_name: str, arguments: Dict[str, Any]) -> str:
    normalized = {key: value for key, value in arguments.items() if key != "runId"}

    def text_key(*keys: str) -> str | None:
        for key in keys:
            value = normalized.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip().casefold()
        return None

    def url_key(*keys: str) -> str | None:
        for key in keys:
            value = normalized.get(key)
            if not isinstance(value, str) or not value.strip():
                continue
            normalized_url = _normalize_crawl_url(value)
            if normalized_url:
                return normalized_url.casefold()
            return value.strip().casefold()
        return None

    def normalized_domain_key(*keys: str) -> str | None:
        for key in keys:
            value = normalized.get(key)
            if not isinstance(value, str) or not value.strip():
                continue
            host = _normalize_host(value)
            if host and "." in host:
                return host
        return None

    def email_domain_key(*keys: str) -> str | None:
        for key in keys:
            value = normalized.get(key)
            if not isinstance(value, str) or "@" not in value:
                continue
            _, _, domain = value.strip().partition("@")
            host = _normalize_host(domain)
            if host and "." in host:
                return host
        return None

    def site_domain_key(*keys: str) -> str | None:
        for key in keys:
            value = normalized.get(key)
            if not isinstance(value, str) or not value.strip():
                continue
            host = _domain_from_url(value)
            if host:
                return _normalize_host(host)
        return None

    semantic_value: str | None = None
    if tool_name in {"fetch_url", "wayback_fetch_url"}:
        semantic_value = url_key("url")
    elif tool_name in {"osint_whatweb_target"}:
        semantic_value = url_key("target")
    elif tool_name in {"linkedin_download_html_ocr"}:
        semantic_value = url_key("profile")
    elif tool_name in {"personal_site_search"}:
        semantic_value = (
            site_domain_key("url", "profile_url", "blog")
            or normalized_domain_key("domain")
            or email_domain_key("email")
            or url_key("url", "profile_url", "blog")
        )
    elif tool_name in {"domain_whois_search", "osint_amass_domain", "osint_theharvester_email_domain"}:
        semantic_value = text_key("domain")
    elif tool_name in {"email_pattern_inference"}:
        domain = text_key("domain")
        person = text_key("person_name")
        if domain and person:
            semantic_value = f"{domain}|{person}"
        else:
            semantic_value = domain or person
    elif tool_name in {"osint_holehe_email", "package_registry_search"}:
        semantic_value = text_key("email", "username", "person_name")
    elif tool_name in {"osint_phoneinfoga_number"}:
        semantic_value = text_key("number", "phone", "target")
    elif tool_name in {"google_serp_person_search"}:
        semantic_value = text_key("target_name", "query")
    elif tool_name in {"person_search"}:
        semantic_value = text_key("name", "query")
    elif tool_name in {"arxiv_search_and_download"}:
        semantic_value = text_key("author", "topic")
    elif tool_name in {"sanctions_watchlist_search", "company_officer_search"}:
        semantic_value = text_key("person_name")
    elif tool_name in {
        "github_identity_search",
        "gitlab_identity_search",
        "osint_maigret_username",
        "username_permutation_search",
        "x_get_user_posts_api",
        "reddit_user_search",
        "medium_author_search",
    }:
        semantic_value = text_key("username", "profile_url", "person_name")

    if semantic_value:
        return f"{tool_name}|semantic|{semantic_value}"
    return tool_argument_signature(tool_name, arguments)


def _merge_tool_plan_items(current: ToolPlanItem, incoming: ToolPlanItem) -> ToolPlanItem:
    merged_arguments = dict(current.arguments)
    for key, value in incoming.arguments.items():
        if key == "runId":
            merged_arguments[key] = value
            continue
        if key not in merged_arguments or _is_empty_argument_value(merged_arguments[key]):
            merged_arguments[key] = value
            continue
        if _is_empty_argument_value(value):
            continue
        if isinstance(merged_arguments[key], (int, float)) and isinstance(value, (int, float)):
            merged_arguments[key] = max(merged_arguments[key], value)
            continue

    rationale = current.rationale
    if incoming.rationale and incoming.rationale not in rationale:
        rationale = f"{rationale} {incoming.rationale}".strip()

    return ToolPlanItem(tool=current.tool, arguments=merged_arguments, rationale=rationale)


def _tool_plan_matches_completed_receipt(
    state: PlannerState, tool_name: str, arguments: Dict[str, Any]
) -> bool:
    semantic_key = _tool_plan_dedupe_key(tool_name, arguments)
    for receipt in state.get("tool_receipts", []):
        if receipt.tool_name != tool_name or not receipt.ok:
            continue
        receipt_arguments = receipt.arguments if isinstance(receipt.arguments, dict) else {}
        if receipt_arguments and _tool_plan_dedupe_key(receipt.tool_name, receipt_arguments) == semantic_key:
            return True
        if receipt.argument_signature and receipt.argument_signature == tool_argument_signature(tool_name, arguments):
            return True
    return _receipt_has_argument_signature(state, tool_name, arguments)


def _is_empty_argument_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, list):
        return len(value) == 0
    if isinstance(value, dict):
        return len(value) == 0
    return False


def _fetch_urls_from_plan(plan: List[ToolPlanItem]) -> List[str]:
    fetch_urls: List[str] = []
    for item in plan:
        if item.tool != "fetch_url":
            continue
        url = item.arguments.get("url")
        if isinstance(url, str):
            normalized = _normalize_crawl_url(url)
            if normalized:
                fetch_urls.append(normalized)
    return _dedupe(fetch_urls)


def json_like(value: Dict[str, Any]) -> str:
    try:
        return str(sorted(value.items()))
    except Exception:
        return str(value)


def _format_receipt_note(receipt: ToolReceipt) -> str | None:
    if not receipt.ok:
        return None
    if receipt.tool_name == "fetch_url" and receipt.document_ids:
        return f"Fetched content → document {receipt.document_ids[0]}"
    if receipt.tool_name == "ingest_text" and receipt.document_ids:
        chunk_count = None
        for fact in receipt.key_facts:
            if "chunkCount" in fact:
                chunk_count = fact.get("chunkCount")
        if chunk_count is not None:
            return f"Ingested text → document {receipt.document_ids[0]} ({chunk_count} chunks)"
        return f"Ingested text → document {receipt.document_ids[0]}"
    if receipt.tool_name == "ingest_graph_entity":
        return "Ingested graph entity"
    return receipt.summary


def _trim_noteboard(notes: List[str], max_items: int = 20) -> List[str]:
    if len(notes) <= max_items:
        return notes
    return notes[-max_items:]


def _inject_noteboard(prompt: str, notes: List[str]) -> str:
    if not notes:
        return prompt
    summary = "\n".join(f"- {note}" for note in notes)
    return f"{prompt}\n\nNoteboard (key findings so far):\n{summary}"


def _derive_run_title(prompt: str, inputs: List[str], llm: OpenRouterLLM | None) -> str:
    normalized = " ".join((prompt or "").strip().split())
    fallback = normalized[:160] if normalized else "Untitled investigation"

    enable_llm_title = os.getenv("LANGGRAPH_ENABLE_LLM_RUN_TITLE", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if llm is not None and enable_llm_title:
        try:
            title = llm.generate_run_title(prompt, inputs)
            if title:
                return title
        except Exception as exc:
            logger.warning("Run title generation failed",
                           extra={"error": str(exc)})
    return fallback


def _persist_run_title(run_id: str, title: str) -> None:
    import psycopg

    dsn = os.getenv(
        "DATABASE_URL", "postgresql://osint:osint@postgres:5432/osint")
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE runs
                SET title = %s
                WHERE run_id = %s
                  AND (title IS NULL OR btrim(title) = '')
                """,
                (title, run_id),
            )


def _build_auto_graph_entities(tool_name: str, arguments: Dict[str, Any], result: Dict[str, Any]) -> List[Dict[str, Any]]:
    technical_entities = build_technical_graph_entities(tool_name, arguments, result)
    if technical_entities:
        return technical_entities

    business_entities = build_business_graph_entities(tool_name, arguments, result)
    if business_entities:
        return business_entities

    if tool_name not in {"osint_sherlock_username", "osint_maigret_username", "osint_whatsmyname_username"}:
        return []

    username = _extract_username_for_graph(arguments, result)
    if not username:
        return []

    evidence = _extract_object_evidence(result)
    profiles = _extract_profile_candidates(tool_name, result)
    if not profiles:
        return []

    relations: List[Dict[str, Any]] = []
    for profile in profiles[:AUTO_GRAPH_ENTITY_LIMIT]:
        relation: Dict[str, Any] = {
            "type": "HAS_PROFILE",
            "targetType": "Article",
            "targetProperties": {
                "uri": profile["url"],
                "name": profile["site"],
            },
        }
        if evidence:
            relation["evidenceRef"] = evidence
        relations.append(relation)

    entity: Dict[str, Any] = {
        "entityType": "Person",
        "entityId": f"username:{username.lower()}",
        "properties": {"name": username, "username": username},
        "relations": relations,
    }
    if evidence:
        entity["evidence"] = {"objectRef": evidence}

    return [entity]


def _extract_username_for_graph(arguments: Dict[str, Any], result: Dict[str, Any]) -> str | None:
    username = arguments.get("username")
    if not isinstance(username, str) or not username.strip():
        username = result.get("username")
    if not isinstance(username, str):
        return None
    normalized = username.strip().lstrip("@")
    return normalized or None


def _extract_object_evidence(result: Dict[str, Any]) -> Dict[str, Any] | None:
    evidence = result.get("evidence")
    if not isinstance(evidence, dict):
        return None

    document_id = evidence.get("documentId")
    bucket = evidence.get("bucket")
    object_key = evidence.get("objectKey")
    version_id = evidence.get("versionId")
    etag = evidence.get("etag")
    if not document_id and not (bucket and object_key):
        return None

    output: Dict[str, Any] = {}
    if isinstance(document_id, str) and document_id:
        output["documentId"] = document_id
    if isinstance(bucket, str) and bucket:
        output["bucket"] = bucket
    if isinstance(object_key, str) and object_key:
        output["objectKey"] = object_key
    if isinstance(version_id, str) and version_id:
        output["versionId"] = version_id
    if isinstance(etag, str) and etag:
        output["etag"] = etag
    return output or None


def _extract_profile_candidates(tool_name: str, result: Dict[str, Any]) -> List[Dict[str, str]]:
    candidates: List[Dict[str, str]] = []

    if tool_name == "osint_whatsmyname_username":
        found = result.get("found")
        if isinstance(found, list):
            for entry in found:
                if not isinstance(entry, dict):
                    continue
                url = _clean_profile_url(entry.get("url"))
                if not url:
                    continue
                site = entry.get("site")
                label = site if isinstance(
                    site, str) and site.strip() else _site_label_from_url(url)
                candidates.append({"url": url, "site": label})

    elif tool_name == "osint_sherlock_username":
        found = result.get("found")
        if isinstance(found, list):
            for entry in found:
                if not isinstance(entry, str):
                    continue
                match = URL_REGEX.search(entry)
                if not match:
                    continue
                url = _clean_profile_url(match.group(0))
                if not url:
                    continue
                label = _site_label_from_sherlock_line(entry, url)
                candidates.append({"url": url, "site": label})

    elif tool_name == "osint_maigret_username":
        parsed = result.get("parsed")
        urls = _extract_urls_from_any(parsed)
        for url in urls:
            cleaned = _clean_profile_url(url)
            if not cleaned:
                continue
            candidates.append(
                {"url": cleaned, "site": _site_label_from_url(cleaned)})

    return _dedupe_profile_candidates(candidates)


def _extract_urls_from_any(value: Any) -> List[str]:
    urls: List[str] = []
    if isinstance(value, str):
        for match in URL_REGEX.findall(value):
            urls.append(match)
        return urls

    if isinstance(value, list):
        for item in value:
            urls.extend(_extract_urls_from_any(item))
        return urls

    if isinstance(value, dict):
        for item in value.values():
            urls.extend(_extract_urls_from_any(item))
        return urls

    return urls


def _clean_profile_url(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip().rstrip(".,)")
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return candidate


def _site_label_from_sherlock_line(line: str, url: str) -> str:
    if ":" in line:
        prefix = line.split(":", 1)[0].strip()
        prefix = prefix.replace("[+]", "").replace("[*]", "").strip()
        if prefix:
            return prefix
    return _site_label_from_url(url)


def _site_label_from_url(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host or "profile"


def _dedupe_profile_candidates(candidates: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen: set[str] = set()
    deduped: List[Dict[str, str]] = []
    for candidate in candidates:
        url = candidate.get("url", "")
        if not url or url in seen:
            continue
        seen.add(url)
        deduped.append(candidate)
    return deduped
