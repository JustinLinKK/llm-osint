#!/usr/bin/env python3
"""Fetch one arXiv paper, download the PDF, and extract paper-level metadata."""

from __future__ import annotations

import argparse
import gzip
import io
import json
import re
import sys
import tarfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from search_arxiv_and_download import (
    DEFAULT_BASE_URL,
    download_pdf,
    parse_feed,
    request_feed,
    sanitize_fragment,
)

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover - optional dependency fallback
    PdfReader = None  # type: ignore[assignment]


DEFAULT_USER_AGENT = "mcp-tool-arxiv-paper/1.0 (mailto:replace-with-your-email@example.com)"
STANDARD_EMAIL_REGEX = re.compile(
    r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b",
    re.IGNORECASE,
)
GROUPED_EMAIL_REGEXES = [
    re.compile(r"[\{\[]\s*([A-Z0-9._%+\- ,;/]+?)\s*[\}\]]\s*@\s*([A-Z0-9.-]+\.[A-Z]{2,})", re.IGNORECASE),
    re.compile(r"\b([A-Z0-9._%+\-]+(?:\s*[,;/]\s*[A-Z0-9._%+\-]+)+)\s*@\s*([A-Z0-9.-]+\.[A-Z]{2,})\b", re.IGNORECASE),
]
ARXIV_ID_REGEX = re.compile(r"arxiv\.org/(?:abs|pdf)/([^?#/]+)", re.IGNORECASE)
AFFILIATION_KEYWORDS = (
    "university",
    "institute",
    "college",
    "school",
    "department",
    "laboratory",
    "laboratories",
    "research",
    "hospital",
    "center",
    "centre",
)
TOPIC_STOPWORDS = {
    "about",
    "across",
    "after",
    "among",
    "analysis",
    "approach",
    "based",
    "between",
    "beyond",
    "detection",
    "efficient",
    "framework",
    "from",
    "improved",
    "large",
    "learning",
    "method",
    "methods",
    "model",
    "models",
    "paper",
    "study",
    "system",
    "their",
    "through",
    "toward",
    "using",
    "with",
}
PLACEHOLDER_EMAIL_TOKENS = {
    "example",
    "first",
    "first1",
    "first2",
    "last",
    "last1",
    "last2",
    "name",
    "test",
    "user",
    "www",
    "xxx",
    "your",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def dedupe_strings(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = " ".join(str(value or "").split()).strip()
        if not text:
            continue
        lowered = text.casefold()
        if lowered in seen:
            continue
        seen.add(lowered)
        output.append(text)
    return output


def clean_text(value: Any, max_len: int = 4000) -> str:
    if not isinstance(value, str):
        return ""
    compact = re.sub(r"\s+", " ", value).strip()
    if len(compact) <= max_len:
        return compact
    return compact[: max_len - 1].rstrip() + "..."


def normalize_arxiv_id(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = ARXIV_ID_REGEX.search(text)
    if match:
        text = match.group(1)
    text = text.rstrip("/")
    if text.lower().endswith(".pdf"):
        text = text[:-4]
    if text.startswith("arXiv:") or text.startswith("arxiv:"):
        text = text.split(":", 1)[1]
    return text.strip()


def resolve_arxiv_id(args: argparse.Namespace) -> str:
    for value in (args.arxiv_id, args.paper_url, args.pdf_url, args.url):
        normalized = normalize_arxiv_id(value or "")
        if normalized:
            return normalized
    return ""


def fetch_paper_entry(
    *,
    base_url: str,
    arxiv_id: str,
    timeout: float,
    user_agent: str,
    max_retries: int,
    retry_backoff: float,
) -> tuple[str, dict[str, Any]]:
    request_url, feed_xml = request_feed(
        base_url=base_url,
        params={"id_list": arxiv_id},
        timeout=timeout,
        user_agent=user_agent,
        max_retries=max_retries,
        retry_backoff=retry_backoff,
    )
    _, entries = parse_feed(feed_xml)
    if not entries:
        raise RuntimeError(f"No arXiv entry found for id={arxiv_id}")
    entry = dict(entries[0])
    entry["request_url"] = request_url
    entry["abs_url"] = entry.get("id_url") or f"https://arxiv.org/abs/{arxiv_id}"
    return request_url, entry


def download_url_bytes(url: str, timeout: float, user_agent: str) -> bytes:
    req = Request(url, headers={"User-Agent": user_agent}, method="GET")
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()


def extract_pdf_text(pdf_path: Path, max_pages: int, max_chars: int) -> tuple[str, str]:
    if PdfReader is None:
        return "", "pypdf_unavailable"

    try:
        reader = PdfReader(str(pdf_path))
        parts: list[str] = []
        for page in reader.pages[: max(1, max_pages)]:
            try:
                page_text = page.extract_text() or ""
            except Exception:
                page_text = ""
            if page_text.strip():
                parts.append(page_text)
            joined = "\n\n".join(parts)
            if len(joined) >= max_chars:
                return joined[:max_chars], "ok_truncated"
        return "\n\n".join(parts), "ok"
    except Exception as exc:  # pragma: no cover - PDF parsing failures are environment dependent
        return "", f"error: {exc}"


def extract_source_text(archive_bytes: bytes, max_chars: int) -> tuple[str, str]:
    text_candidates: list[tuple[int, int, str]] = []
    try:
        with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:*") as archive:
            members = [
                member
                for member in archive.getmembers()
                if member.isfile() and member.name.lower().endswith((".tex", ".txt", ".bbl", ".md"))
            ]
            for member in members[:20]:
                extracted = archive.extractfile(member)
                if extracted is None:
                    continue
                blob = extracted.read(min(member.size, 400_000))
                text = blob.decode("utf-8", errors="ignore")
                text = "\n".join(
                    line for line in text.splitlines() if not line.lstrip().startswith("%")
                )
                marker_positions = [
                    position
                    for position in [
                        text.find("\\title"),
                        text.find("\\author"),
                        text.find("\\begin{abstract}"),
                    ]
                    if position >= 0
                ]
                if marker_positions:
                    text = text[min(marker_positions):]
                if not text.strip():
                    continue
                basename = Path(member.name).name.lower()
                score = 0
                if basename in {"main.tex", "paper.tex", "ms.tex", "article.tex"}:
                    score += 8
                if "\\title" in text:
                    score += 4
                if "\\author" in text:
                    score += 4
                if "\\begin{abstract}" in text or "\\abstract" in text:
                    score += 4
                if "\\begin{document}" in text:
                    score += 2
                if "\\section" in text:
                    score += 1
                if basename.startswith(("template", "style", "supplement")):
                    score -= 2
                text_candidates.append((score, len(text), text))
        if text_candidates:
            ordered_text = [item[2] for item in sorted(text_candidates, key=lambda item: (item[0], item[1]), reverse=True)]
            joined = "\n\n".join(ordered_text)
            if len(joined) >= max_chars:
                return joined[:max_chars], "source_tar_truncated"
            return joined, "source_tar"
    except tarfile.TarError:
        pass

    try:
        text = gzip.decompress(archive_bytes).decode("utf-8", errors="ignore")
        if text.strip():
            return text[:max_chars], "source_gzip"
    except Exception:
        pass

    return "", "source_unavailable"


def _split_grouped_locals(local_blob: str) -> list[str]:
    raw_parts = re.split(r"\s*[,;/]\s*", local_blob)
    values = []
    for item in raw_parts:
        text = re.sub(r"[^A-Z0-9._%+\-]", "", item, flags=re.IGNORECASE).strip()
        if text:
            values.append(text)
    return values


def extract_emails(text: str) -> list[str]:
    if not text.strip():
        return []

    emails: list[str] = []
    for pattern in GROUPED_EMAIL_REGEXES:
        for local_blob, domain in pattern.findall(text):
            for local_part in _split_grouped_locals(local_blob):
                emails.append(f"{local_part}@{domain}")

    emails.extend(STANDARD_EMAIL_REGEX.findall(text))
    output: list[str] = []
    for email in dedupe_strings(emails):
        local_part, _, domain = email.lower().partition("@")
        local_tokens = [token for token in re.split(r"[^a-z0-9]+", local_part) if token]
        domain_tokens = [token for token in re.split(r"[^a-z0-9]+", domain) if token]
        if any(token in PLACEHOLDER_EMAIL_TOKENS for token in [*local_tokens, *domain_tokens]):
            continue
        output.append(email)
    return output


def extract_affiliations(text: str, authors: list[str], title: str) -> list[str]:
    if not text.strip():
        return []

    author_tokens = {re.sub(r"[^a-z0-9]+", " ", name.lower()).strip() for name in authors if name.strip()}
    affiliations: list[str] = []
    for raw_line in text.splitlines()[:60]:
        line = " ".join(raw_line.split()).strip(" ,;")
        lowered = line.lower()
        if not line or len(line) < 8 or len(line) > 160:
            continue
        if lowered == title.lower():
            continue
        if lowered.startswith("abstract"):
            continue
        if any(token and token in lowered for token in author_tokens):
            continue
        if not any(keyword in lowered for keyword in AFFILIATION_KEYWORDS):
            continue
        line = re.sub(r"^[0-9*#,+\s]+", "", line).strip(" ,;")
        affiliations.append(line)
    return dedupe_strings(affiliations)[:10]


def infer_topics(title: str, summary: str, categories: list[str], topic_hint: str | None = None) -> list[str]:
    topics: list[str] = []
    if topic_hint and topic_hint.strip():
        topics.append(topic_hint.strip())
    topics.extend([item for item in categories if isinstance(item, str) and item.strip()])

    for segment in re.split(r"[:;,-]", title):
        cleaned = clean_text(segment, max_len=80)
        if cleaned and len(cleaned) >= 6:
            topics.append(cleaned)

    token_counts: Counter[str] = Counter()
    for token in re.findall(r"[A-Za-z][A-Za-z-]{4,}", f"{title} {summary}"):
        lowered = token.lower()
        if lowered in TOPIC_STOPWORDS:
            continue
        token_counts[lowered] += 1
    for token, _ in token_counts.most_common(6):
        topics.append(token.replace("-", " "))

    return dedupe_strings(topics)[:12]


def normalize_local_part(email: str) -> str:
    local_part = str(email or "").split("@", 1)[0].split("+", 1)[0]
    return re.sub(r"[^a-z0-9]", "", local_part.lower())


def author_name_features(name: str) -> dict[str, str]:
    parts = [re.sub(r"[^a-z0-9]", "", item.lower()) for item in re.split(r"[\s.-]+", name) if item.strip()]
    parts = [item for item in parts if item]
    first = parts[0] if parts else ""
    last = parts[-1] if parts else ""
    initials = "".join(item[0] for item in parts if item)
    return {"first": first, "last": last, "initials": initials}


def score_author_email_match(author_name: str, email: str) -> float:
    local_part = normalize_local_part(email)
    if not local_part:
        return 0.0
    features = author_name_features(author_name)
    first = features["first"]
    last = features["last"]
    initials = features["initials"]

    score = 0.0
    if last and last in local_part:
        score += 0.65
    if first and first in local_part:
        score += 0.25
    if first and last and f"{first}{last}" in local_part:
        score += 0.35
    if first and last and f"{first[:1]}{last}" in local_part:
        score += 0.4
    if first and last and f"{last}{first[:1]}" in local_part:
        score += 0.25
    if initials and initials in local_part:
        score += 0.15
    return min(score, 1.0)


def build_author_contacts(authors: list[str], emails: list[str]) -> list[dict[str, Any]]:
    scored_pairs: list[tuple[float, str, str]] = []
    for author in authors:
        for email in emails:
            score = score_author_email_match(author, email)
            if score > 0:
                scored_pairs.append((score, author, email))

    scored_pairs.sort(key=lambda item: item[0], reverse=True)
    assigned_authors: set[str] = set()
    assigned_emails: set[str] = set()
    matches: dict[str, tuple[str, float]] = {}

    for score, author, email in scored_pairs:
        if score < 0.55:
            continue
        if author in assigned_authors or email in assigned_emails:
            continue
        assigned_authors.add(author)
        assigned_emails.add(email)
        matches[author] = (email, round(score, 2))

    contacts: list[dict[str, Any]] = []
    for author in authors:
        email, confidence = matches.get(author, ("", 0.0))
        contacts.append(
            {
                "name": author,
                "email": email or None,
                "match_confidence": confidence if email else 0.0,
            }
        )
    return contacts


def build_coauthor_contacts(author_contacts: list[dict[str, Any]], primary_author: str | None) -> list[dict[str, Any]]:
    if not primary_author:
        return author_contacts
    output: list[dict[str, Any]] = []
    for item in author_contacts:
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        if name.casefold() == primary_author.casefold():
            continue
        output.append(item)
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch one arXiv paper, download the PDF, and extract paper metadata."
    )
    parser.add_argument("--arxiv-id", help="arXiv identifier, e.g. 2402.04333 or 2402.04333v1")
    parser.add_argument("--paper-url", help="arXiv abstract URL, e.g. https://arxiv.org/abs/2402.04333")
    parser.add_argument("--pdf-url", help="Direct arXiv PDF URL, e.g. https://arxiv.org/pdf/2402.04333")
    parser.add_argument("--url", help="Generic URL alias for paper-url/pdf-url")
    parser.add_argument("--author-hint", help="Optional primary author name used to separate coauthors")
    parser.add_argument("--topic-hint", help="Optional topic hint to seed topic extraction")
    parser.add_argument(
        "-o",
        "--output-dir",
        default="results/arxiv_paper",
        help="Output directory (default: results/arxiv_paper)",
    )
    parser.add_argument(
        "--metadata-file",
        default="metadata.json",
        help="Metadata JSON filename inside output dir (default: metadata.json)",
    )
    parser.add_argument(
        "--text-file",
        default="paper_text.txt",
        help="Extracted text filename inside output dir (default: paper_text.txt)",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"arXiv API endpoint (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="HTTP User-Agent header value",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Retry count for 429/5xx/temporary URL errors (default: 5)",
    )
    parser.add_argument(
        "--retry-backoff",
        type=float,
        default=5.0,
        help="Initial retry backoff in seconds; doubles each retry (default: 5.0)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=4,
        help="Max PDF pages to extract text from (default: 4)",
    )
    parser.add_argument(
        "--max-text-chars",
        type=int,
        default=20000,
        help="Max extracted text characters to keep (default: 20000)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite already-downloaded PDFs and extracted text files",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    arxiv_id = resolve_arxiv_id(args)
    if not arxiv_id:
        print("Provide --arxiv-id, --paper-url, --pdf-url, or --url.", file=sys.stderr)
        return 2

    output_dir = Path(args.output_dir)
    pdf_dir = output_dir / "pdfs"
    output_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir.mkdir(parents=True, exist_ok=True)

    try:
        request_url, entry = fetch_paper_entry(
            base_url=args.base_url,
            arxiv_id=arxiv_id,
            timeout=args.timeout,
            user_agent=args.user_agent,
            max_retries=args.max_retries,
            retry_backoff=args.retry_backoff,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Paper lookup failed: {exc}", file=sys.stderr)
        return 1

    title = str(entry.get("title") or "").strip()
    summary = str(entry.get("summary") or "").strip()
    authors = [str(item).strip() for item in entry.get("authors", []) if isinstance(item, str) and str(item).strip()]
    categories = [str(item).strip() for item in entry.get("categories", []) if isinstance(item, str) and str(item).strip()]
    pdf_url = str(entry.get("pdf_url") or "").strip()

    safe_id = sanitize_fragment(str(entry.get("arxiv_id") or arxiv_id))
    pdf_path = pdf_dir / f"{safe_id}.pdf"
    download_status = "not_requested"
    if pdf_url:
        if pdf_path.exists() and not args.overwrite:
            download_status = "exists"
        else:
            try:
                download_pdf(pdf_url, pdf_path, timeout=args.timeout, user_agent=args.user_agent)
                download_status = "downloaded"
            except Exception as exc:  # noqa: BLE001
                download_status = f"error: {exc}"
    else:
        download_status = "no_pdf_url"

    extracted_text = ""
    text_status = "skipped"
    text_path = output_dir / args.text_file
    if pdf_path.exists():
        extracted_text, text_status = extract_pdf_text(
            pdf_path,
            max_pages=max(1, args.max_pages),
            max_chars=max(1000, args.max_text_chars),
        )
        if extracted_text:
            text_path.write_text(extracted_text, encoding="utf-8")
    elif text_path.exists() and args.overwrite:
        text_path.unlink()

    source_url = f"https://arxiv.org/e-print/{entry.get('arxiv_id') or arxiv_id}"
    source_text = ""
    source_status = "skipped"
    if not extracted_text or text_status != "ok":
        try:
            source_bytes = download_url_bytes(source_url, timeout=args.timeout, user_agent=args.user_agent)
            source_text, source_status = extract_source_text(source_bytes, max_chars=max(1000, args.max_text_chars))
        except Exception as exc:  # noqa: BLE001
            source_status = f"error: {exc}"

    analysis_text = "\n\n".join([value for value in [extracted_text, source_text] if value]).strip()
    if not extracted_text and source_text:
        extracted_text = source_text
        text_status = source_status
        text_path.write_text(extracted_text, encoding="utf-8")

    emails = extract_emails(analysis_text)
    author_contacts = build_author_contacts(authors, emails)
    author_hint = str(args.author_hint or "").strip()
    primary_author = ""
    if author_hint:
        primary_author = next(
            (author for author in authors if author.casefold() == author_hint.casefold()),
            authors[0] if authors else author_hint,
        )
    elif authors:
        primary_author = authors[0]
    coauthors = build_coauthor_contacts(author_contacts, primary_author)
    affiliations = extract_affiliations(analysis_text, authors, title)
    topics = infer_topics(title, summary, categories, topic_hint=args.topic_hint)

    paper = {
        "arxiv_id": entry.get("arxiv_id") or arxiv_id,
        "title": title,
        "published": entry.get("published"),
        "updated": entry.get("updated"),
        "summary": summary,
        "abstract": summary,
        "authors": authors,
        "categories": categories,
        "topics": topics,
        "affiliations": affiliations,
        "emails": emails,
        "author_contacts": author_contacts,
        "coauthors": coauthors,
        "pdf_url": pdf_url,
        "abs_url": entry.get("abs_url"),
        "id_url": entry.get("id_url"),
        "request_url": request_url,
        "source_url": source_url,
        "download_status": download_status,
        "pdf_file": str(pdf_path) if pdf_path.exists() else "",
        "paper_text_path": str(text_path) if text_path.exists() else "",
        "text_extraction_status": text_status,
        "source_text_extraction_status": source_status,
        "text_excerpt": clean_text(extracted_text, max_len=4000),
    }

    metadata = {
        "generated_at_utc": utc_now_iso(),
        "query": {
            "arxiv_id": arxiv_id,
            "paper_url": args.paper_url,
            "pdf_url": args.pdf_url,
            "url": args.url,
            "author_hint": args.author_hint,
            "topic_hint": args.topic_hint,
        },
        "paper": paper,
    }
    metadata_path = output_dir / args.metadata_file
    metadata_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"[paper] {paper['arxiv_id']} {title}")
    print(f"[download] {download_status}")
    print(f"[emails] {len(emails)}")
    print(f"[saved] Metadata written to {metadata_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
