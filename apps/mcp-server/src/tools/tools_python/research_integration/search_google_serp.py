#!/usr/bin/env python3
"""
Search a target person with the Google SERP service API and save result pages as HTML.

Usage:
    python search_google_serp.py "Elon Musk" --max-results 10
    python search_google_serp.py "Sam Altman" -n 25 -o results/sam_search
    python search_google_serp.py "Sam Altman" --print-agent-prompt

Environment (via .env file or shell):
    GOOGLE_SERP_BASE_URL: API base URL
    GOOGLE_SERP_API_KEY: API key value (optional)
    GOOGLE_SERP_API_KEY_HEADER: API key header name (default: X-API-Key)
    GOOGLE_SERP_BEARER_TOKEN: Bearer token (optional)
    GOOGLE_SERP_MAX_RETRIES: default retry count for rate-limited/temporary failures
    GOOGLE_SERP_RETRY_BACKOFF: initial backoff in seconds
    GOOGLE_SERP_RETRY_JITTER: random jitter seconds added to retries
    GOOGLE_SERP_API_DELAY: minimum delay between API attempts (seconds)
"""

import argparse
import html
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlparse
from urllib.request import Request, urlopen


def load_env_file():
    """Load environment variables from .env file."""
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return

    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if (value.startswith('"') and value.endswith('"')) or (
                value.startswith("'") and value.endswith("'")
            ):
                value = value[1:-1]
            if key not in os.environ:
                os.environ[key] = value


load_env_file()


RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}


def env_int(name, default):
    """Read integer env var with safe fallback."""
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def env_float(name, default):
    """Read float env var with safe fallback."""
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


GOOGLE_DORK_OPERATORS = [
    {
        "operator": '"..."',
        "description": "Match an exact phrase.",
        "example": '"OpenAI CEO Sam Altman"',
    },
    {
        "operator": "intext:",
        "description": "Require term(s) in page body text.",
        "example": 'intext:"Sam Altman"',
    },
    {
        "operator": "allintext:",
        "description": "Require all listed terms in page body text.",
        "example": "allintext:sam altman openai",
    },
    {
        "operator": "site:",
        "description": "Restrict results to one domain or subdomain.",
        "example": 'site:linkedin.com/in "Sam Altman"',
    },
    {
        "operator": "intitle:",
        "description": "Require term(s) in page title.",
        "example": 'intitle:"Sam Altman" site:news.ycombinator.com',
    },
    {
        "operator": "inurl:",
        "description": "Require term(s) in URL path.",
        "example": 'inurl:about "Sam Altman"',
    },
    {
        "operator": "allinurl:",
        "description": "Require all listed terms in URL.",
        "example": "allinurl:sam altman profile",
    },
    {
        "operator": "filetype:",
        "description": "Restrict to document types such as pdf/docx/ppt.",
        "example": 'filetype:pdf "Sam Altman" resume',
    },
    {
        "operator": "before:/after:",
        "description": "Constrain results by date range.",
        "example": '"Sam Altman" after:2023-01-01 before:2025-01-01',
    },
    {
        "operator": "numrange:",
        "description": "Find pages containing numbers in a range.",
        "example": '"Sam Altman" numrange:2020-2025',
    },
    {
        "operator": "OR",
        "description": "Broaden query by adding alternatives.",
        "example": '"Sam Altman" OR "Samuel Altman"',
    },
    {
        "operator": "-term",
        "description": "Exclude noisy terms.",
        "example": '"Sam Altman" -podcast -youtube',
    },
    {
        "operator": "()",
        "description": "Group boolean logic for precision.",
        "example": '("Sam Altman" OR "Samuel Altman") (email OR contact)',
    },
    {
        "operator": "*",
        "description": "Wildcard placeholder in phrase queries.",
        "example": '"Sam * Altman"',
    },
]

FEW_SHOT_DORK_EXAMPLES = [
    {
        "input": 'Target: "Sam Altman". Goal: find official profiles and interviews.',
        "output": {
            "queries": [
                'site:linkedin.com/in "Sam Altman"',
                'site:x.com "Sam Altman" (profile OR bio)',
                '("Sam Altman" OR "Samuel Altman") (interview OR keynote) -jobs',
            ],
            "rationale": "Starts with authoritative profile domains, then broadens to public interviews while excluding job spam.",
        },
    },
    {
        "input": 'Target: "Andrew Ng". Goal: find publications/slides.',
        "output": {
            "queries": [
                '("Andrew Ng" OR "Andrew Yan-Tak Ng") (paper OR publication) filetype:pdf',
                'site:stanford.edu "Andrew Ng" (slides OR lecture) filetype:pdf',
                'site:arxiv.org "Andrew Ng"',
            ],
            "rationale": "Uses aliases + filetype filters to bias toward primary documents.",
        },
    },
]


def build_agent_system_prompt(max_queries):
    """Build a system prompt that enforces explicit operator-aware dork generation."""
    operator_lines = "\n".join(
        [
            f"- {item['operator']} {item['description']} Example: {item['example']}"
            for item in GOOGLE_DORK_OPERATORS
        ]
    )
    return (
        "You are a Google dork query planner for an authorized OSINT workflow.\n"
        "Generate focused queries that maximize relevance and minimize noise.\n"
        "Use the operators below deliberately; do not invent operators.\n\n"
        f"Operators:\n{operator_lines}\n\n"
        "Output rules:\n"
        f"1) Return valid JSON only with keys: queries, rationale.\n"
        f"2) queries must be a list of 1 to {max_queries} strings.\n"
        "3) Each query must include at least one operator from the list.\n"
        "4) Prefer high-signal domains first (official sites, reputable sources).\n"
        "5) Remove duplicates and overly broad queries.\n"
        "6) Avoid exploit-seeking patterns (credentials, exposed directories, vulnerability hunting).\n"
        "7) Prefer stable operators first: quotes, site:, intitle:, inurl:, filetype:, OR, and -term.\n"
    )


def build_agent_prompt_bundle(target_name, max_queries=6):
    """Return a prompt bundle (system + few-shot + user prompt) for external agents."""
    clean_target = target_name.strip()
    return {
        "system_prompt": build_agent_system_prompt(max_queries=max_queries),
        "few_shot_examples": FEW_SHOT_DORK_EXAMPLES,
        "user_prompt": (
            f'Target: "{clean_target}". '
            "Create Google dork queries for discovery of high-confidence public sources. "
            "Prioritize official profiles, interviews, and primary documents."
        ),
    }


def parse_retry_after_seconds(value):
    """Parse Retry-After header into seconds."""
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        seconds = float(text)
        return max(0.0, seconds)
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(text)
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        delta = (when - datetime.now(timezone.utc)).total_seconds()
        return max(0.0, delta)
    except Exception:
        return None


def throttle_request(min_delay_seconds, last_request_at):
    """Enforce a minimum delay between API attempts."""
    delay = max(0.0, float(min_delay_seconds or 0.0))
    if delay > 0 and last_request_at is not None:
        elapsed = time.monotonic() - last_request_at
        remaining = delay - elapsed
        if remaining > 0:
            time.sleep(remaining)
    return time.monotonic()


def request_json(
    method,
    base_url,
    endpoint,
    headers,
    params=None,
    payload=None,
    timeout=20,
    max_retries=3,
    retry_backoff=2.0,
    retry_jitter=0.5,
    min_delay=0.0,
):
    """Perform HTTP request with retry/backoff and parse JSON."""
    base = base_url.rstrip("/") + "/"
    path = endpoint.lstrip("/")
    url = urljoin(base, path)
    req_headers = dict(headers)

    if method.upper() == "GET":
        query = urlencode(params or {})
        if query:
            url = f"{url}?{query}"
        req = Request(url, headers=req_headers, method="GET")
    elif method.upper() == "POST":
        req_headers["Content-Type"] = "application/json"
        body = json.dumps(payload or {}).encode("utf-8")
        req = Request(url, headers=req_headers, data=body, method="POST")
    else:
        raise ValueError(f"Unsupported method: {method}")

    attempts = max(0, int(max_retries)) + 1
    last_request_at = None

    for attempt in range(attempts):
        last_request_at = throttle_request(min_delay, last_request_at)
        try:
            with urlopen(req, timeout=timeout) as resp:
                body = resp.read()
                charset = resp.headers.get_content_charset() or "utf-8"
                text = body.decode(charset, errors="replace")
                return url, json.loads(text)
        except HTTPError as e:
            error_body = e.read().decode("utf-8", errors="replace")
            status_code = int(getattr(e, "code", 0) or 0)
            can_retry = status_code in RETRYABLE_HTTP_CODES and attempt < attempts - 1
            if can_retry:
                retry_after = parse_retry_after_seconds(e.headers.get("Retry-After"))
                fallback = float(retry_backoff) * (2 ** attempt) + random.uniform(
                    0.0, max(0.0, float(retry_jitter))
                )
                sleep_seconds = retry_after if retry_after is not None else fallback
                print(
                    f"Retryable HTTP {status_code} for {url}. "
                    f"Attempt {attempt + 1}/{attempts}. Sleeping {sleep_seconds:.2f}s..."
                )
                time.sleep(max(0.0, sleep_seconds))
                continue
            raise RuntimeError(f"HTTP {status_code} for {url}: {error_body[:800]}")
        except URLError as e:
            can_retry = attempt < attempts - 1
            if can_retry:
                sleep_seconds = float(retry_backoff) * (2 ** attempt) + random.uniform(
                    0.0, max(0.0, float(retry_jitter))
                )
                print(
                    f"Network error for {url}: {e.reason}. "
                    f"Attempt {attempt + 1}/{attempts}. Sleeping {sleep_seconds:.2f}s..."
                )
                time.sleep(max(0.0, sleep_seconds))
                continue
            raise RuntimeError(f"URL error for {url}: {e.reason}")
        except json.JSONDecodeError as e:
            raise RuntimeError(f"JSON decode failed for {url}: {e}")


def build_auth_headers():
    """Build auth headers from environment variables."""
    headers = {
        "Accept": "application/json",
        "User-Agent": "Python Google SERP Client",
    }

    api_key = os.environ.get("GOOGLE_SERP_API_KEY")
    if api_key:
        header_name = os.environ.get("GOOGLE_SERP_API_KEY_HEADER", "X-API-Key")
        headers[header_name] = api_key

    bearer_token = os.environ.get("GOOGLE_SERP_BEARER_TOKEN")
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"

    return headers


def request_json_get(
    base_url,
    endpoint,
    params,
    headers,
    timeout=20,
    max_retries=3,
    retry_backoff=2.0,
    retry_jitter=0.5,
    min_delay=0.0,
):
    """Perform GET request and parse JSON."""
    return request_json(
        method="GET",
        base_url=base_url,
        endpoint=endpoint,
        params=params,
        headers=headers,
        timeout=timeout,
        max_retries=max_retries,
        retry_backoff=retry_backoff,
        retry_jitter=retry_jitter,
        min_delay=min_delay,
    )


def request_json_post(
    base_url,
    endpoint,
    payload,
    headers,
    timeout=20,
    max_retries=3,
    retry_backoff=2.0,
    retry_jitter=0.5,
    min_delay=0.0,
):
    """Perform POST request with JSON body and parse JSON response."""
    return request_json(
        method="POST",
        base_url=base_url,
        endpoint=endpoint,
        payload=payload,
        headers=headers,
        timeout=timeout,
        max_retries=max_retries,
        retry_backoff=retry_backoff,
        retry_jitter=retry_jitter,
        min_delay=min_delay,
    )


def find_results_list(payload):
    """Find result item list from API payload."""
    if isinstance(payload, list):
        return payload

    if not isinstance(payload, dict):
        return []

    for key in ("results", "items", "organic_results", "search_results", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return value

    best = []

    def looks_like_result(item):
        if not isinstance(item, dict):
            return False
        keys = {k.lower() for k in item.keys()}
        return any(k in keys for k in ("url", "link", "href"))

    def walk(node):
        nonlocal best
        if isinstance(node, dict):
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            if node and all(isinstance(x, dict) for x in node):
                score = sum(1 for x in node if looks_like_result(x))
                if score > 0 and score >= len(best):
                    best = node
            for value in node:
                walk(value)

    walk(payload)
    return best


def pick_url(item):
    """Get URL field from a result dict."""
    for key in ("url", "link", "href", "target_url", "result_url"):
        value = item.get(key)
        if isinstance(value, str) and value.startswith(("http://", "https://")):
            return value
    return None


def normalize_result(item, rank):
    """Normalize raw result object to a stable structure."""
    url = pick_url(item)
    if not url:
        return None

    title = ""
    for key in ("title", "name", "headline"):
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            title = value.strip()
            break

    snippet = ""
    for key in ("snippet", "description", "summary", "body"):
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            snippet = value.strip()
            break

    return {
        "rank": rank,
        "title": title,
        "url": url,
        "snippet": snippet,
        "raw": item,
    }


def sanitize_name(text, max_len=60):
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "_", text).strip("_")
    if not cleaned:
        cleaned = "result"
    return cleaned[:max_len]


def html_wrapper_for_non_html(url, content_type, status_code):
    safe_url = html.escape(url)
    safe_type = html.escape(content_type or "unknown")
    safe_status = html.escape(str(status_code) if status_code is not None else "unknown")
    return f"""<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\">
    <title>Non-HTML result</title>
  </head>
  <body>
    <h1>Non-HTML content skipped</h1>
    <p>URL: <a href=\"{safe_url}\">{safe_url}</a></p>
    <p>Status: {safe_status}</p>
    <p>Content-Type: {safe_type}</p>
  </body>
</html>
"""


def fetch_and_save_html(url, output_path, timeout=20):
    """Download one result URL and save HTML locally."""
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; PersonSearchBot/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    req = Request(url, headers=headers, method="GET")

    try:
        with urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", None)
            content_type = resp.headers.get("Content-Type", "")
            body = resp.read()

            is_html = "text/html" in content_type.lower() or b"<html" in body[:2048].lower()
            if is_html:
                output_path.write_bytes(body)
            else:
                output_path.write_text(
                    html_wrapper_for_non_html(url, content_type, status),
                    encoding="utf-8",
                )

            return {
                "saved": True,
                "status_code": status,
                "content_type": content_type,
                "error": None,
            }
    except Exception as e:
        return {
            "saved": False,
            "status_code": None,
            "content_type": "",
            "error": str(e),
        }


def build_index_html(target_name, records):
    rows = []
    for record in records:
        url = html.escape(record["url"])
        title = html.escape(record.get("title") or "(no title)")
        local_file = html.escape(record.get("local_file") or "")
        status = html.escape(record.get("status", ""))
        snippet = html.escape(record.get("snippet", ""))
        rank = record.get("rank", "")
        local_link = f'<a href="{local_file}">{local_file}</a>' if local_file else "-"
        rows.append(
            f"<tr><td>{rank}</td><td>{title}</td><td><a href=\"{url}\">{url}</a></td>"
            f"<td>{local_link}</td><td>{status}</td><td>{snippet}</td></tr>"
        )

    safe_name = html.escape(target_name)
    body_rows = "\n".join(rows)
    return f"""<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\">
    <title>Search Results - {safe_name}</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 24px; }}
      table {{ border-collapse: collapse; width: 100%; }}
      th, td {{ border: 1px solid #ccc; padding: 8px; vertical-align: top; }}
      th {{ background: #f2f2f2; text-align: left; }}
      td {{ font-size: 13px; }}
    </style>
  </head>
  <body>
    <h1>Search Results: {safe_name}</h1>
    <table>
      <thead>
        <tr>
          <th>Rank</th>
          <th>Title</th>
          <th>URL</th>
          <th>Local HTML</th>
          <th>Status</th>
          <th>Snippet</th>
        </tr>
      </thead>
      <tbody>
        {body_rows}
      </tbody>
    </table>
  </body>
</html>
"""


def main():
    parser = argparse.ArgumentParser(
        description="Search a target person with Google SERP API and save result pages as HTML."
    )
    parser.add_argument("target_name", help="Person name to search for")
    parser.add_argument(
        "--print-agent-prompt",
        action="store_true",
        help="Print a JSON bundle with system prompt + few-shot examples for dork planning and exit.",
    )
    parser.add_argument(
        "--agent-max-queries",
        type=int,
        default=6,
        help="Maximum number of dork queries allowed in the prompt bundle (default: 6).",
    )
    parser.add_argument(
        "--max-results",
        "-n",
        type=int,
        default=10,
        help="Maximum number of search results to save (default: 10)",
    )
    parser.add_argument(
        "--start",
        type=int,
        default=1,
        help="Starting position for results (default: 1)",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        type=str,
        default=None,
        help="Output directory. Default: results/search_<name>_<timestamp>",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default=os.environ.get("GOOGLE_SERP_BASE_URL"),
        help=f"SERP API base URL",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="HTTP timeout in seconds (default: 20)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=env_int("GOOGLE_SERP_MAX_RETRIES", 3),
        help="Retry count for 429/5xx/network errors (default: env GOOGLE_SERP_MAX_RETRIES or 3).",
    )
    parser.add_argument(
        "--retry-backoff",
        type=float,
        default=env_float("GOOGLE_SERP_RETRY_BACKOFF", 2.0),
        help="Initial retry backoff seconds; doubles each retry (default: env GOOGLE_SERP_RETRY_BACKOFF or 2.0).",
    )
    parser.add_argument(
        "--retry-jitter",
        type=float,
        default=env_float("GOOGLE_SERP_RETRY_JITTER", 0.5),
        help="Max random jitter added to retries in seconds (default: env GOOGLE_SERP_RETRY_JITTER or 0.5).",
    )
    parser.add_argument(
        "--api-delay",
        type=float,
        default=env_float("GOOGLE_SERP_API_DELAY", 0.0),
        help="Minimum delay between API attempts in seconds (default: env GOOGLE_SERP_API_DELAY or 0.0).",
    )

    args = parser.parse_args()

    if args.print_agent_prompt:
        bundle = build_agent_prompt_bundle(
            target_name=args.target_name,
            max_queries=max(1, args.agent_max_queries),
        )
        print(json.dumps(bundle, indent=2, ensure_ascii=False))
        return

    if args.max_results <= 0:
        print("Error: --max-results must be positive.", file=sys.stderr)
        sys.exit(1)
    if args.start <= 0:
        print("Error: --start must be >= 1.", file=sys.stderr)
        sys.exit(1)
    if args.max_retries < 0:
        print("Error: --max-retries must be >= 0.", file=sys.stderr)
        sys.exit(1)
    if args.retry_backoff < 0:
        print("Error: --retry-backoff must be >= 0.", file=sys.stderr)
        sys.exit(1)
    if args.retry_jitter < 0:
        print("Error: --retry-jitter must be >= 0.", file=sys.stderr)
        sys.exit(1)
    if args.api_delay < 0:
        print("Error: --api-delay must be >= 0.", file=sys.stderr)
        sys.exit(1)
    if not args.base_url:
        print(
            "Error: missing --base-url (or GOOGLE_SERP_BASE_URL in environment).",
            file=sys.stderr,
        )
        sys.exit(1)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_out = Path("results") / f"search_{sanitize_name(args.target_name)}_{timestamp}"
    output_dir = Path(args.output_dir) if args.output_dir else default_out
    output_dir.mkdir(parents=True, exist_ok=True)

    headers = build_auth_headers()

    payload = None
    used_url = None
    used_params = None
    used_method = None
    used_endpoint = None
    last_error = None

    print(f"Searching for: {args.target_name}")
    print(f"Base URL: {args.base_url}")

    if args.max_results <= 10:
        params = {"q": args.target_name, "num": args.max_results, "start": args.start}
        try:
            used_url, payload = request_json_get(
                base_url=args.base_url,
                endpoint="/search",
                params=params,
                headers=headers,
                timeout=args.timeout,
                max_retries=args.max_retries,
                retry_backoff=args.retry_backoff,
                retry_jitter=args.retry_jitter,
                min_delay=args.api_delay,
            )
            used_params = params
            used_method = "GET"
            used_endpoint = "/search"
            print(f"Search request succeeded: GET /search with params {params}")
        except RuntimeError as e:
            last_error = str(e)
    else:
        capped_total = min(args.max_results, 100)
        if args.max_results > 100:
            print("Warning: API max is 100 results. Capping --max-results to 100.")
        paged_body = {
            "q": args.target_name,
            "start": args.start,
            "num": 10,
            "per_request": 10,
            "total_results": capped_total,
        }
        try:
            used_url, payload = request_json_post(
                base_url=args.base_url,
                endpoint="/search/paged",
                payload=paged_body,
                headers=headers,
                timeout=args.timeout,
                max_retries=args.max_retries,
                retry_backoff=args.retry_backoff,
                retry_jitter=args.retry_jitter,
                min_delay=args.api_delay,
            )
            used_params = paged_body
            used_method = "POST"
            used_endpoint = "/search/paged"
            print("Search request succeeded: POST /search/paged")
        except RuntimeError as e:
            last_error = str(e)

    if payload is None:
        print("Error: search request failed for the OpenAPI-defined endpoint.", file=sys.stderr)
        if last_error:
            print(last_error, file=sys.stderr)
        sys.exit(1)

    raw_response_path = output_dir / "api_response.json"
    with open(raw_response_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"Saved raw API response: {raw_response_path}")

    raw_results = find_results_list(payload)
    normalized = []
    seen_urls = set()

    for item in raw_results:
        if len(normalized) >= args.max_results:
            break
        if not isinstance(item, dict):
            continue
        record = normalize_result(item, rank=len(normalized) + 1)
        if not record:
            continue
        if record["url"] in seen_urls:
            continue
        seen_urls.add(record["url"])
        normalized.append(record)

    if not normalized:
        print("Warning: no URLs found in API response. Check api_response.json.")

    results_dir = output_dir / "html_results"
    results_dir.mkdir(parents=True, exist_ok=True)

    saved_count = 0
    for record in normalized:
        parsed = urlparse(record["url"])
        domain = sanitize_name(parsed.netloc or "unknown")
        name = f"{record['rank']:03d}_{domain}.html"
        output_path = results_dir / name

        status = fetch_and_save_html(record["url"], output_path, timeout=args.timeout)
        record["local_file"] = str(Path("html_results") / name)
        record["status"] = "saved" if status["saved"] else "failed"
        record["fetch_error"] = status["error"]
        record["status_code"] = status["status_code"]
        record["content_type"] = status["content_type"]

        if status["saved"]:
            saved_count += 1
            print(f"[{record['rank']:02d}] saved -> {output_path}")
        else:
            print(f"[{record['rank']:02d}] failed -> {record['url']} ({status['error']})")

    index_html_path = output_dir / "index.html"
    index_html_path.write_text(build_index_html(args.target_name, normalized), encoding="utf-8")

    summary_path = output_dir / "search_results.json"
    summary = {
        "target_name": args.target_name,
        "requested_max_results": args.max_results,
        "results_found": len(normalized),
        "results_saved": saved_count,
        "used_search_method": used_method,
        "used_search_endpoint": used_endpoint,
        "used_search_url": used_url,
        "used_search_params": used_params,
        "retry_settings": {
            "max_retries": args.max_retries,
            "retry_backoff": args.retry_backoff,
            "retry_jitter": args.retry_jitter,
            "api_delay": args.api_delay,
        },
        "generated_at": datetime.now().isoformat(),
        "results": normalized,
    }
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print("\nDone.")
    print(f"Output directory: {output_dir}")
    print(f"Summary JSON: {summary_path}")
    print(f"Local index HTML: {index_html_path}")
    print(f"Saved pages: {saved_count}/{len(normalized)}")


if __name__ == "__main__":
    main()
