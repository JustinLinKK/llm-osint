#!/usr/bin/env python3
"""
Search arXiv by author name or topic, then download PDFs for the matched results.

Examples:
    python search_arxiv_and_download.py --author "Geoffrey Hinton" -n 5
    python search_arxiv_and_download.py --topic "large language model" -n 20
    python search_arxiv_and_download.py --author "Yoshua Bengio" --topic "diffusion model" -n 10
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET


DEFAULT_BASE_URL = "https://export.arxiv.org/api/query"
MAX_API_RESULTS = 30000
MAX_PAGE_SIZE = 2000
RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}
NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "opensearch": "http://a9.com/-/spec/opensearch/1.1/",
}


def normalize_term(term: str) -> str:
    term = " ".join(term.strip().split())
    if not term:
        return term
    if " " in term:
        term = '"' + term.replace('"', "") + '"'
    return term


def build_search_query(author: str | None, topic: str | None) -> str:
    clauses = []
    if author:
        clauses.append(f"au:{normalize_term(author)}")
    if topic:
        clauses.append(f"all:{normalize_term(topic)}")
    return " AND ".join(clauses)


def sanitize_fragment(value: str, max_len: int = 80) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "_", value).strip("_")
    if not cleaned:
        cleaned = "paper"
    return cleaned[:max_len]


def text_or_empty(node: ET.Element | None, path: str) -> str:
    if node is None:
        return ""
    value = node.findtext(path, default="", namespaces=NS)
    return " ".join(value.split())


def extract_pdf_url(entry: ET.Element) -> str:
    for link in entry.findall("atom:link", NS):
        if link.get("title") == "pdf" and link.get("rel") == "related":
            href = link.get("href")
            if href:
                return href
    entry_id = text_or_empty(entry, "atom:id")
    if "/abs/" in entry_id:
        return entry_id.replace("/abs/", "/pdf/")
    return ""


def parse_feed(feed_xml: str) -> tuple[int, list[dict[str, Any]]]:
    root = ET.fromstring(feed_xml)
    total_text = root.findtext("opensearch:totalResults", default="0", namespaces=NS)
    try:
        total_results = int(total_text)
    except ValueError:
        total_results = 0

    entries: list[dict[str, Any]] = []
    for entry in root.findall("atom:entry", NS):
        entry_id_url = text_or_empty(entry, "atom:id")
        arxiv_id = entry_id_url.split("/abs/")[-1] if "/abs/" in entry_id_url else entry_id_url
        authors = [
            " ".join((a.findtext("atom:name", default="", namespaces=NS) or "").split())
            for a in entry.findall("atom:author", NS)
        ]
        categories = [c.get("term", "") for c in entry.findall("atom:category", NS) if c.get("term")]

        entries.append(
            {
                "title": text_or_empty(entry, "atom:title"),
                "id_url": entry_id_url,
                "arxiv_id": arxiv_id,
                "published": text_or_empty(entry, "atom:published"),
                "updated": text_or_empty(entry, "atom:updated"),
                "summary": text_or_empty(entry, "atom:summary"),
                "authors": authors,
                "categories": categories,
                "pdf_url": extract_pdf_url(entry),
                "download_status": "pending",
                "pdf_file": "",
            }
        )
    return total_results, entries


def request_feed(
    base_url: str,
    params: dict[str, Any],
    timeout: float,
    user_agent: str,
    max_retries: int,
    retry_backoff: float,
) -> tuple[str, str]:
    url = f"{base_url}?{urlencode(params)}"
    req = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "application/atom+xml",
        },
        method="GET",
    )
    attempts = max_retries + 1
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(req, timeout=timeout) as resp:
                body = resp.read()
                charset = resp.headers.get_content_charset() or "utf-8"
                return url, body.decode(charset, errors="replace")
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            code = int(exc.code)
            if code in RETRYABLE_HTTP_CODES and attempt < attempts:
                wait_seconds = retry_backoff * (2 ** (attempt - 1))
                print(
                    f"[retry] HTTP {code} on attempt {attempt}/{attempts}. "
                    f"Sleeping {wait_seconds:.1f}s...",
                    file=sys.stderr,
                )
                time.sleep(wait_seconds)
                continue
            raise RuntimeError(f"HTTP {exc.code} for {url}: {body[:500]}") from exc
        except URLError as exc:
            if attempt < attempts:
                wait_seconds = retry_backoff * (2 ** (attempt - 1))
                print(
                    f"[retry] URL error on attempt {attempt}/{attempts}: {exc.reason}. "
                    f"Sleeping {wait_seconds:.1f}s...",
                    file=sys.stderr,
                )
                time.sleep(wait_seconds)
                continue
            raise RuntimeError(f"URL error for {url}: {exc.reason}") from exc

    raise RuntimeError(f"Request failed for {url}")


def query_arxiv(
    *,
    base_url: str,
    search_query: str,
    start: int,
    target_count: int,
    page_size: int,
    sort_by: str,
    sort_order: str,
    api_delay: float,
    timeout: float,
    user_agent: str,
    max_retries: int,
    retry_backoff: float,
) -> tuple[list[dict[str, Any]], int]:
    collected: list[dict[str, Any]] = []
    total_available = 0
    current_start = start

    while len(collected) < target_count:
        batch_size = min(page_size, target_count - len(collected), MAX_PAGE_SIZE)
        params = {
            "search_query": search_query,
            "start": current_start,
            "max_results": batch_size,
            "sortBy": sort_by,
            "sortOrder": sort_order,
        }
        request_url, feed_xml = request_feed(
            base_url=base_url,
            params=params,
            timeout=timeout,
            user_agent=user_agent,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
        )
        total_available, batch_entries = parse_feed(feed_xml)
        print(f"[query] {request_url}")

        if not batch_entries:
            break

        collected.extend(batch_entries)
        current_start += len(batch_entries)

        if len(batch_entries) < batch_size:
            break
        if total_available and current_start >= total_available:
            break
        if len(collected) < target_count and api_delay > 0:
            time.sleep(api_delay)

    return collected[:target_count], total_available


def download_pdf(
    url: str, out_path: Path, timeout: float, user_agent: str
) -> None:
    req = Request(url, headers={"User-Agent": user_agent}, method="GET")
    with urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    out_path.write_bytes(data)


def download_entries(
    entries: list[dict[str, Any]],
    pdf_dir: Path,
    timeout: float,
    user_agent: str,
    overwrite: bool,
    download_delay: float,
) -> tuple[int, int]:
    downloaded = 0
    failed = 0

    for i, entry in enumerate(entries, start=1):
        pdf_url = entry.get("pdf_url", "")
        arxiv_id = str(entry.get("arxiv_id", "paper"))
        safe_id = sanitize_fragment(arxiv_id)
        out_path = pdf_dir / f"{i:04d}_{safe_id}.pdf"
        entry["pdf_file"] = str(out_path)

        if not pdf_url:
            entry["download_status"] = "no_pdf_url"
            failed += 1
            continue
        if out_path.exists() and not overwrite:
            entry["download_status"] = "exists"
            continue

        try:
            download_pdf(pdf_url, out_path, timeout=timeout, user_agent=user_agent)
            entry["download_status"] = "downloaded"
            downloaded += 1
            print(f"[downloaded] {out_path.name}")
        except Exception as exc:  # noqa: BLE001
            entry["download_status"] = f"error: {exc}"
            failed += 1
            print(f"[failed] {arxiv_id}: {exc}", file=sys.stderr)

        if i < len(entries) and download_delay > 0:
            time.sleep(download_delay)

    return downloaded, failed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Search arXiv by author/topic and download matching PDFs."
    )
    parser.add_argument("--author", help='Author name (e.g. "Yann LeCun")')
    parser.add_argument("--topic", help='Topic terms (e.g. "computer vision")')
    parser.add_argument(
        "-n",
        "--max-results",
        type=int,
        default=10,
        help="Number of results to fetch and download (default: 10)",
    )
    parser.add_argument(
        "--start",
        type=int,
        default=0,
        help="0-based start index in the result set (default: 0)",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help=f"Results fetched per API call (max {MAX_PAGE_SIZE}, default: 100)",
    )
    parser.add_argument(
        "--sort-by",
        choices=["relevance", "lastUpdatedDate", "submittedDate"],
        default="relevance",
        help="arXiv API sortBy value",
    )
    parser.add_argument(
        "--sort-order",
        choices=["ascending", "descending"],
        default="descending",
        help="arXiv API sortOrder value",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default="results/arxiv_downloads",
        help="Output directory (default: results/arxiv_downloads)",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"arXiv API endpoint (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--api-delay",
        type=float,
        default=3.0,
        help="Delay in seconds between API page requests (default: 3.0)",
    )
    parser.add_argument(
        "--download-delay",
        type=float,
        default=0.0,
        help="Delay in seconds between PDF downloads (default: 0.0)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--user-agent",
        default="mcp-tool-arxiv-client/1.0 (mailto:replace-with-your-email@example.com)",
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
        "--metadata-file",
        default="metadata.json",
        help="Metadata JSON filename inside output dir (default: metadata.json)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite already-downloaded PDFs",
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Search only; do not download PDFs",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.author and not args.topic:
        print("Provide at least one of --author or --topic.", file=sys.stderr)
        return 2
    if args.max_results <= 0:
        print("--max-results must be >= 1", file=sys.stderr)
        return 2
    if args.start < 0:
        print("--start must be >= 0", file=sys.stderr)
        return 2
    if args.page_size <= 0 or args.page_size > MAX_PAGE_SIZE:
        print(f"--page-size must be in [1, {MAX_PAGE_SIZE}].", file=sys.stderr)
        return 2
    if args.start >= MAX_API_RESULTS:
        print(
            f"--start must be < {MAX_API_RESULTS} due to arXiv API limits.",
            file=sys.stderr,
        )
        return 2
    if args.max_retries < 0:
        print("--max-retries must be >= 0", file=sys.stderr)
        return 2
    if args.retry_backoff < 0:
        print("--retry-backoff must be >= 0", file=sys.stderr)
        return 2

    max_allowed = MAX_API_RESULTS - args.start
    target_count = min(args.max_results, max_allowed)
    if target_count < args.max_results:
        print(
            f"[warn] Requested {args.max_results}, capped to {target_count} by arXiv API limit.",
            file=sys.stderr,
        )

    search_query = build_search_query(args.author, args.topic)
    print(f"[search_query] {search_query}")

    output_dir = Path(args.output_dir)
    pdf_dir = output_dir / "pdfs"
    output_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir.mkdir(parents=True, exist_ok=True)

    try:
        entries, total_available = query_arxiv(
            base_url=args.base_url,
            search_query=search_query,
            start=args.start,
            target_count=target_count,
            page_size=args.page_size,
            sort_by=args.sort_by,
            sort_order=args.sort_order,
            api_delay=args.api_delay,
            timeout=args.timeout,
            user_agent=args.user_agent,
            max_retries=args.max_retries,
            retry_backoff=args.retry_backoff,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Search failed: {exc}", file=sys.stderr)
        return 1

    print(
        f"[summary] Retrieved {len(entries)} entries (requested={target_count}, "
        f"total_available={total_available})."
    )

    downloaded = 0
    failed = 0
    if not args.no_download:
        downloaded, failed = download_entries(
            entries=entries,
            pdf_dir=pdf_dir,
            timeout=args.timeout,
            user_agent=args.user_agent,
            overwrite=args.overwrite,
            download_delay=args.download_delay,
        )

    metadata = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "search_query": search_query,
        "author": args.author,
        "topic": args.topic,
        "requested_results": args.max_results,
        "effective_requested_results": target_count,
        "retrieved_results": len(entries),
        "total_available": total_available,
        "downloaded_count": downloaded,
        "download_failed_count": failed,
        "output_dir": str(output_dir),
        "pdf_dir": str(pdf_dir),
        "entries": entries,
    }
    metadata_path = output_dir / args.metadata_file
    metadata_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[saved] Metadata written to {metadata_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
