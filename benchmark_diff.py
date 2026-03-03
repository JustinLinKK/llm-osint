from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List


SECTION_HEADER_RE = re.compile(r"^(#{1,6}\s+.+)$", re.MULTILINE)
URL_RE = re.compile(r"https?://\S+")
YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b")
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def split_sections(text: str) -> List[str]:
    headers = list(SECTION_HEADER_RE.finditer(text))
    if not headers:
        return [text.strip()] if text.strip() else []
    sections: List[str] = []
    for index, match in enumerate(headers):
        start = match.start()
        end = headers[index + 1].start() if index + 1 < len(headers) else len(text)
        chunk = text[start:end].strip()
        if chunk:
            sections.append(chunk)
    return sections


def detect_entity_types(text: str) -> Dict[str, int]:
    lowered = text.lower()
    patterns = {
        "Person": [r"\b(alias|advisor|coauthor|researcher|author|founder|director)\b"],
        "Org": [r"\b(company|organization|corp|llc|inc)\b"],
        "Institution": [r"\b(university|college|institute|department|lab|laboratory|school)\b"],
        "Publication": [r"\b(publication|paper|preprint|journal|conference paper|thesis)\b"],
        "Domain": [r"\b[a-z0-9.-]+\.[a-z]{2,}\b"],
        "Document": [r"\.pdf\b", r"\b(thesis|dissertation|cv|resume|report)\b"],
        "Conference": [r"\b(conference|workshop|symposium|neurips|icml|iclr|acl|emnlp)\b"],
    }
    result: Dict[str, int] = {}
    for label, regexes in patterns.items():
        result[label] = int(any(re.search(regex, lowered) for regex in regexes))
    return result


def detect_relationship_types(text: str) -> Dict[str, int]:
    lowered = text.lower()
    patterns = {
        "WORKS_AT": [r"\bworks at\b", r"\bjoined\b", r"\bemployer\b"],
        "AFFILIATED_WITH": [r"\baffiliat", r"\bmember of\b", r"\bat\b"],
        "COAUTHORED_WITH": [r"\bcoauthor", r"\bco-auth", r"\bcollaborat"],
        "HAS_EMAIL": [EMAIL_RE.pattern],
        "HAS_PROFILE": [r"linkedin\.com/", r"github\.com/", r"orcid\.org/", r"openreview\.net/"],
        "STUDIED_AT": [r"\bgraduated\b", r"\bph\.?d\b", r"\bm\.?s\.?\b", r"\bb\.?s\.?\b"],
        "APPEARS_IN_ARCHIVE": [r"\bwayback\b", r"\barchived\b", r"\bsnapshot\b"],
    }
    result: Dict[str, int] = {}
    for label, regexes in patterns.items():
        result[label] = int(any(re.search(regex, lowered, re.IGNORECASE) for regex in regexes))
    return result


def hard_anchor_score(text: str) -> Dict[str, int]:
    lowered = text.lower()
    return {
        "institutional_email_domains": int(bool(re.search(r"@[A-Z0-9.-]+\.(edu|ac\.[a-z]{2,}|org)\b", text, re.IGNORECASE))),
        "stable_profile_ids": int(any(token in lowered for token in ("orcid.org", "openreview.net", "scholar.google", "semantic scholar", "dblp"))),
        "official_docs": int(".pdf" in lowered or "thesis" in lowered or "dissertation" in lowered),
        "concrete_dates": int(bool(YEAR_RE.search(text))),
        "collaborator_clustering": int("collaboration group" in lowered or "cluster" in lowered),
    }


def rubric_score(text: str, rubric: Dict[str, int]) -> Dict[str, int]:
    lowered = text.lower()
    score = dict(rubric)
    score["identity"] = int(any(token in lowered for token in ("canonical identity", "profile", "orcid", "linkedin.com/in/", "github.com/")))
    score["aliases"] = int(any(token in lowered for token in ("alias", "aka", "username", "handle")))
    score["academic_history"] = int(any(token in lowered for token in ("university", "degree", "phd", "advisor", "thesis")))
    score["employment_history"] = int(any(token in lowered for token in ("worked at", "joined", "employer", "company", "lab")))
    score["publications"] = int(any(token in lowered for token in ("publication", "paper", "preprint", "journal", "conference")))
    score["relationships"] = int(any(token in lowered for token in ("coauthor", "advisor", "collaborator", "works at", "member of")))
    score["contacts"] = int(bool(EMAIL_RE.search(text)) or any(token in lowered for token in ("phone", "contact page", "contact us")))
    score["code_presence"] = int(any(token in lowered for token in ("github", "gitlab", "repository", "repositories", "pypi", "npm")))
    score["business_roles"] = int(any(token in lowered for token in ("director", "officer", "founder", "board", "sec filing")))
    score["archived_history"] = int(any(token in lowered for token in ("wayback", "archived", "snapshot")))
    return score


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare pipeline output against a benchmark report.")
    parser.add_argument("--pipeline-report", required=True)
    parser.add_argument("--benchmark-report", required=True)
    parser.add_argument("--rubric", default="benchmark_rubric.json")
    args = parser.parse_args()

    pipeline_text = read_text(Path(args.pipeline_report))
    benchmark_text = read_text(Path(args.benchmark_report))
    rubric = json.loads(read_text(Path(args.rubric)))

    pipeline_sections = split_sections(pipeline_text)
    benchmark_sections = split_sections(benchmark_text)
    pipeline_entities = detect_entity_types(pipeline_text)
    benchmark_entities = detect_entity_types(benchmark_text)
    pipeline_relationships = detect_relationship_types(pipeline_text)
    benchmark_relationships = detect_relationship_types(benchmark_text)
    pipeline_anchors = hard_anchor_score(pipeline_text)
    benchmark_anchors = hard_anchor_score(benchmark_text)
    pipeline_rubric = rubric_score(pipeline_text, rubric)
    benchmark_rubric = rubric_score(benchmark_text, rubric)

    output = {
        "section_coverage_diff": {
            "pipeline_sections": len(pipeline_sections),
            "benchmark_sections": len(benchmark_sections),
            "delta": len(benchmark_sections) - len(pipeline_sections),
        },
        "entity_type_coverage_diff": {
            key: {"pipeline": pipeline_entities.get(key, 0), "benchmark": benchmark_entities.get(key, 0)}
            for key in sorted(set(pipeline_entities) | set(benchmark_entities))
        },
        "relationship_type_coverage_diff": {
            key: {"pipeline": pipeline_relationships.get(key, 0), "benchmark": benchmark_relationships.get(key, 0)}
            for key in sorted(set(pipeline_relationships) | set(benchmark_relationships))
        },
        "hard_anchors": {
            key: {"pipeline": pipeline_anchors.get(key, 0), "benchmark": benchmark_anchors.get(key, 0)}
            for key in sorted(set(pipeline_anchors) | set(benchmark_anchors))
        },
        "rubric_scorecard": {
            key: {"pipeline": pipeline_rubric.get(key, 0), "benchmark": benchmark_rubric.get(key, 0)}
            for key in sorted(set(pipeline_rubric) | set(benchmark_rubric))
        },
        "pipeline_urls": len(URL_RE.findall(pipeline_text)),
        "benchmark_urls": len(URL_RE.findall(benchmark_text)),
    }
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
