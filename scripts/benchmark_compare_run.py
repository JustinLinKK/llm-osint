#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


URL_RE = re.compile(r"https?://\S+")
SECTION_RE = re.compile(r"^##\s+", re.MULTILINE)

ROOT = Path("/workspaces/llm-osint")
BENCHMARK_PATH = ROOT / "Benchmark.txt"
BLUEPRINT_PATH = ROOT / "graph_blueprint_sample.json"
CONTRACT_PATH = ROOT / "schemas" / "stage1_graph_blueprint_contract.v1.json"
BASELINE_REPORT_PATH = ROOT / "PIPELINE_VS_BENCHMARK_REPORT.md"


@dataclass
class GateResult:
    name: str
    ok: bool
    detail: str


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(read_text(path))


def http_get_json(base_url: str, path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    url = base_url.rstrip("/") + path
    if params:
        clean = {k: v for k, v in params.items() if v is not None}
        url = f"{url}?{urlencode(clean)}"
    req = Request(url, method="GET")
    try:
        with urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"HTTP {exc.code} for {url}: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Request failed for {url}: {exc}") from exc


def infer_slot_coverage(nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]], required_slots: List[str]) -> Dict[str, bool]:
    node_types = {
        str((node.get("properties") or {}).get("type") or "")
        for node in nodes
    }
    rel_types = {str(edge.get("type") or "") for edge in edges}
    slots = {
        "primary_anchor_node": "Person" in node_types or "Organization" in node_types or "Institution" in node_types,
        "identity_surface": bool(node_types & {"ContactPoint", "Website", "Domain", "Email", "Phone", "Handle"}),
        "related_identity_surface": "Person" in node_types and ("COAUTHORED_WITH" in rel_types or "COLLABORATED_WITH" in rel_types or "ADVISED_BY" in rel_types),
        "relationship_surface": bool(rel_types & {"COAUTHORED_WITH", "COLLABORATED_WITH", "AFFILIATED_WITH", "WORKS_AT", "STUDIED_AT", "HAS_AFFILIATION"}),
        "timeline_surface": bool(node_types & {"TimelineEvent", "TimeNode"}) or bool(rel_types & {"HAS_TIMELINE_EVENT", "IN_TIME_NODE", "NEXT_TIME_NODE", "MENTIONS_TIMELINE_EVENT"}),
        "timeline_mention_surface": "MENTIONS_TIMELINE_EVENT" in rel_types,
        "time_node_surface": "TimeNode" in node_types or bool(rel_types & {"IN_TIME_NODE", "NEXT_TIME_NODE"}),
        "topic_surface": "Topic" in node_types or bool(rel_types & {"HAS_TOPIC", "FOCUSES_ON", "RESEARCHES", "HAS_SKILL_TOPIC", "HAS_HOBBY_TOPIC", "HAS_INTEREST_TOPIC"}),
        "evidence_surface": bool(rel_types & {"MENTIONS", "HAS_DOCUMENT", "APPEARS_IN_ARCHIVE", "FILED"}) or "Document" in node_types,
    }
    for slot in required_slots:
        slots.setdefault(slot, False)
    return slots


def ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def run_comparator(run_id: str, api_base: str) -> Tuple[str, List[GateResult]]:
    benchmark_text = read_text(BENCHMARK_PATH)
    blueprint = read_json(BLUEPRINT_PATH)
    contract = read_json(CONTRACT_PATH)

    report = http_get_json(api_base, f"/runs/{run_id}/report")
    graph = http_get_json(
        api_base,
        f"/runs/{run_id}/graph",
        {"scope": "run", "nodeLimit": 2000, "edgeLimit": 4000},
    )

    nodes = list(graph.get("nodes") or [])
    edges = list(graph.get("edges") or [])
    citations = list(report.get("citations") or [])
    report_json = report.get("json") or {}
    evidence_refs = list((report_json.get("evidenceRefs") if isinstance(report_json, dict) else []) or [])
    section_drafts = list((report_json.get("sectionDrafts") if isinstance(report_json, dict) else []) or [])
    markdown = str(report.get("markdown") or "")

    contract_entity_types = {str(item) for item in (contract.get("entity_types") or [])}
    contract_relation_types = {str(item) for item in (contract.get("relation_types") or [])}
    required_slots = [str(item) for item in (contract.get("required_slots_balanced") or [])]

    node_type_hits = [
        str((node.get("properties") or {}).get("type") or "")
        for node in nodes
    ]
    edge_type_hits = [str(edge.get("type") or "") for edge in edges]

    node_contract_ok = sum(1 for item in node_type_hits if item in contract_entity_types)
    edge_contract_ok = sum(1 for item in edge_type_hits if item in contract_relation_types)

    foreign_nodes = [
        node for node in nodes
        if str((node.get("properties") or {}).get("run_id") or "") != run_id
    ]
    foreign_edges = [
        edge for edge in edges
        if str((edge.get("properties") or {}).get("run_id") or "") != run_id
    ]

    citation_urls = [str(item.get("sourceUrl") or "").strip() for item in citations]
    citation_urls = [url for url in citation_urls if url]
    citation_domains = sorted({re.sub(r"^www\.", "", re.sub(r"^https?://", "", url).split("/")[0].lower()) for url in citation_urls})
    evidence_with_source = sum(1 for row in evidence_refs if str(row.get("sourceUrl") or "").strip())
    evidence_with_object_key = sum(
        1
        for row in evidence_refs
        if (
            isinstance(row.get("objectRef"), dict)
            and (
                row["objectRef"].get("objectKey")
                or row["objectRef"].get("object_key")
                or row["objectRef"].get("documentId")
            )
        )
    )

    section_ids = [str(item.get("sectionId") or "") for item in section_drafts if str(item.get("sectionId") or "")]
    section_has_evidence = {
        section_id: False for section_id in section_ids
    }
    for row in evidence_refs:
        section_id = str(row.get("sectionId") or "")
        if not section_id:
            continue
        has_link = bool(
            str(row.get("sourceUrl") or "").strip()
            or (
                isinstance(row.get("objectRef"), dict)
                and (
                    row["objectRef"].get("objectKey")
                    or row["objectRef"].get("object_key")
                    or row["objectRef"].get("documentId")
                )
            )
        )
        if has_link:
            section_has_evidence[section_id] = True
    sections_missing_evidence = sorted([section_id for section_id, ok in section_has_evidence.items() if not ok])

    slot_coverage = infer_slot_coverage(nodes, edges, required_slots)
    missing_slots = [slot for slot in required_slots if not slot_coverage.get(slot, False)]

    benchmark_url_count = len(URL_RE.findall(benchmark_text))
    benchmark_sections = len(SECTION_RE.findall(benchmark_text))
    run_url_count = len(URL_RE.findall(markdown))
    run_sections = len(SECTION_RE.findall(markdown))

    blueprint_nodes = len(blueprint.get("nodes") or [])
    blueprint_edges = len(blueprint.get("edges") or [])

    gates: List[GateResult] = [
        GateResult(
            name="contamination_zero",
            ok=(len(foreign_nodes) == 0 and len(foreign_edges) == 0),
            detail=f"foreign_nodes={len(foreign_nodes)}, foreign_edges={len(foreign_edges)}",
        ),
        GateResult(
            name="contract_conformance",
            ok=(
                (len(node_type_hits) == 0 or node_contract_ok == len(node_type_hits))
                and (len(edge_type_hits) == 0 or edge_contract_ok == len(edge_type_hits))
            ),
            detail=(
                f"node_ok={node_contract_ok}/{len(node_type_hits)}, "
                f"edge_ok={edge_contract_ok}/{len(edge_type_hits)}"
            ),
        ),
        GateResult(
            name="citation_traceability",
            ok=(
                len(evidence_refs) > 0
                and ratio(evidence_with_source, len(evidence_refs)) >= 0.95
                and ratio(evidence_with_object_key, len(evidence_refs)) >= 1.0
            ),
            detail=(
                f"source_url_ratio={ratio(evidence_with_source, len(evidence_refs)):.2f}, "
                f"object_ref_ratio={ratio(evidence_with_object_key, len(evidence_refs)):.2f}, total={len(evidence_refs)}"
            ),
        ),
        GateResult(
            name="retrieval_diversity",
            ok=(len(set(citation_urls)) >= 30 and len(citation_domains) >= 10),
            detail=f"unique_urls={len(set(citation_urls))}, unique_domains={len(citation_domains)}",
        ),
        GateResult(
            name="section_evidence_presence",
            ok=(len(sections_missing_evidence) == 0),
            detail=f"missing_sections={sections_missing_evidence or 'none'}",
        ),
        GateResult(
            name="blueprint_slots",
            ok=(len(missing_slots) == 0),
            detail=f"missing_slots={missing_slots or 'none'}",
        ),
    ]

    gate_table = "\n".join(
        f"| `{gate.name}` | {'PASS' if gate.ok else 'FAIL'} | {gate.detail} |"
        for gate in gates
    )
    now = datetime.now(timezone.utc).isoformat()
    markdown_out = "\n".join(
        [
            f"# Benchmark Comparison for Run `{run_id}`",
            "",
            f"- Generated at: `{now}`",
            f"- API base: `{api_base}`",
            "",
            "## Gate Results",
            "| Gate | Status | Detail |",
            "|---|---|---|",
            gate_table,
            "",
            "## Graph Metrics",
            f"- Run graph (scope=run): nodes={len(nodes)}, edges={len(edges)}",
            f"- Blueprint sample: nodes={blueprint_nodes}, edges={blueprint_edges}",
            f"- Contract node conformity: {node_contract_ok}/{len(node_type_hits)}",
            f"- Contract edge conformity: {edge_contract_ok}/{len(edge_type_hits)}",
            "",
            "## Evidence Metrics",
            f"- Citation URLs: total={len(citation_urls)}, unique={len(set(citation_urls))}, unique_domains={len(citation_domains)}",
            f"- Evidence refs: total={len(evidence_refs)}, with_source_url={evidence_with_source}, with_object_key_or_doc={evidence_with_object_key}",
            f"- Sections with drafts: {len(section_ids)}; sections missing linked evidence: {sections_missing_evidence or 'none'}",
            "",
            "## Benchmark Delta",
            f"- Benchmark sections={benchmark_sections}; run sections={run_sections}",
            f"- Benchmark URL count={benchmark_url_count}; run URL count={run_url_count}",
            "",
        ]
    )
    return markdown_out, gates


def append_baseline_delta(path: Path, run_id: str, gates: List[GateResult]) -> None:
    if not path.exists():
        return
    status = "PASS" if all(gate.ok for gate in gates) else "FAIL"
    lines = [
        "",
        f"## Delta Update ({datetime.now(timezone.utc).isoformat()})",
        f"- Run: `{run_id}`",
        f"- Gate status: `{status}`",
    ]
    for gate in gates:
        lines.append(f"- `{gate.name}`: {'PASS' if gate.ok else 'FAIL'} ({gate.detail})")
    path.write_text(path.read_text(encoding="utf-8") + "\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate run-vs-benchmark comparison markdown with quality gates.")
    parser.add_argument("--run-id", required=True, help="Run UUID")
    parser.add_argument("--api-base", default="http://localhost:3000", help="API base URL")
    parser.add_argument("--out", default=None, help="Output markdown path")
    parser.add_argument("--append-baseline", action="store_true", help="Append gate delta to PIPELINE_VS_BENCHMARK_REPORT.md")
    parser.add_argument("--fail-on-gates", action="store_true", help="Exit non-zero when any hard gate fails")
    args = parser.parse_args()

    markdown_out, gates = run_comparator(args.run_id, args.api_base)
    out_path = Path(args.out) if args.out else ROOT / "reports" / "benchmark_comparison" / f"{args.run_id}.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(markdown_out, encoding="utf-8")
    print(f"Wrote comparison report: {out_path}")

    if args.append_baseline:
        append_baseline_delta(BASELINE_REPORT_PATH, args.run_id, gates)
        print(f"Appended baseline delta: {BASELINE_REPORT_PATH}")

    if args.fail_on_gates and any(not gate.ok for gate in gates):
        failed = ", ".join(gate.name for gate in gates if not gate.ok)
        print(f"Hard gate failure: {failed}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
