#!/usr/bin/env python3
"""Unified MCP wrapper for person search + research integration tools."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
import time
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any, Callable
from urllib.request import Request, urlopen

_THIS_DIR = Path(__file__).resolve().parent
_INTEGRATION_DIR = _THIS_DIR / "research_integration"
_ACADEMIC_DIR = _THIS_DIR / "academic"
_TECHNICAL_DIR = _THIS_DIR / "technical"
_BUSINESS_DIR = _THIS_DIR / "business"
_ARCHIVE_DIR = _THIS_DIR / "archive"
_IDENTITY_DIR = _THIS_DIR / "identity"
_SAFETY_DIR = _THIS_DIR / "safety"
_CONTACT_DIR = _THIS_DIR / "contact"
_SOCIAL_DIR = _THIS_DIR / "social"
_RELATIONSHIP_DIR = _THIS_DIR / "relationship"

if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

TAVILY_SCORE_SCALE_MAX = 10.0
TAVILY_SEARCH_CHUNKS_PER_SOURCE_DEFAULT = 5
TAVILY_SEARCH_CHUNKS_PER_SOURCE_MAX = 3
TAVILY_EXTRACT_CHUNKS_PER_SOURCE_DEFAULT = 5
TAVILY_EXTRACT_CHUNKS_PER_SOURCE_MAX = 5


def _find_repo_root(start: Path) -> Path | None:
    for parent in [start, *start.parents]:
        if (parent / "apps").exists() and (parent / "infra").exists():
            return parent
    return None


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        key = key.strip()
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


def _load_env_candidates() -> None:
    repo_root = _find_repo_root(_THIS_DIR)
    if repo_root:
        _load_env_file(repo_root / ".env")
        agent_langgraph_src = repo_root / "services" / "agent-langgraph" / "src"
        if str(agent_langgraph_src) not in sys.path:
            sys.path.insert(0, str(agent_langgraph_src))
    _load_env_file(_THIS_DIR / ".env")
    _load_env_file(_INTEGRATION_DIR / ".env")
    _load_env_file(_ACADEMIC_DIR / ".env")
    _load_env_file(_TECHNICAL_DIR / ".env")
    _load_env_file(_BUSINESS_DIR / ".env")
    _load_env_file(_ARCHIVE_DIR / ".env")
    _load_env_file(_IDENTITY_DIR / ".env")
    _load_env_file(_SAFETY_DIR / ".env")
    _load_env_file(_CONTACT_DIR / ".env")
    _load_env_file(_SOCIAL_DIR / ".env")
    _load_env_file(_RELATIONSHIP_DIR / ".env")


def _emit(ok: bool, result: Any = None, error: str | None = None) -> None:
    payload: dict[str, Any] = {"ok": ok}
    if ok:
        payload["result"] = result
    else:
        payload["error"] = error or "Unknown error"
    sys.stdout.write(json.dumps(payload, ensure_ascii=True))
    sys.stdout.flush()


def _read_payload() -> dict[str, Any]:
    payload = json.load(sys.stdin)
    if not isinstance(payload, dict):
        raise RuntimeError("Invalid payload: expected JSON object")
    return payload


def _run_cmd(cmd: list[str], timeout_seconds: int = 600) -> tuple[str, str]:
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout_seconds,
    )
    stdout = proc.stdout.strip()
    stderr = proc.stderr.strip()
    if proc.returncode != 0:
        msg = f"Command failed with code {proc.returncode}: {' '.join(cmd)}"
        details = []
        if stderr:
            details.append(f"stderr={stderr[:4000]}")
        if stdout:
            details.append(f"stdout={stdout[:4000]}")
        if details:
            msg = f"{msg}; {'; '.join(details)}"
        raise RuntimeError(msg)
    return stdout, stderr


def _serialize_person_search(results: list[Any]) -> list[dict[str, Any]]:
    return [
        {
            "url": item.url,
            "title": item.title,
            "snippet": item.snippet,
            "main_text": item.main_text,
            "extracted_text": _clean_text(item.main_text, max_len=4000),
            "error": item.error,
            "html_path": item.html_path,
            "skipped": item.skipped,
        }
        for item in results
    ]


def _clean_text(value: Any, max_len: int = 1000) -> str:
    if not isinstance(value, str):
        return ""
    compact = " ".join(value.split())
    if len(compact) <= max_len:
        return compact
    return compact[: max_len - 1].rstrip() + "…"


def _extract_text_from_html(html_value: str, max_len: int = 2000) -> str:
    if not isinstance(html_value, str) or not html_value:
        return ""
    text = re.sub(r"(?is)<script\b[^>]*>.*?</script>", " ", html_value)
    text = re.sub(r"(?is)<style\b[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?is)<!--.*?-->", " ", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    return _clean_text(text, max_len=max_len)


def _read_text_file(path: Path, max_bytes: int = 1_500_000) -> str:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            return f.read(max_bytes)
    except Exception:
        return ""


def _http_json_request(
    url: str,
    payload: dict[str, Any] | None = None,
    timeout_seconds: int = 60,
    method: str = "POST",
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = Request(url, data=body, method=method.upper())
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    if headers:
        for key, value in headers.items():
            req.add_header(key, value)
    with urlopen(req, timeout=timeout_seconds) as response:
        raw = response.read().decode("utf-8", errors="replace")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError("Unexpected JSON response shape")
    return parsed


def _write_search_index(output_dir: Path, title: str, results: list[dict[str, Any]]) -> Path:
    rows: list[str] = []
    for item in results:
        url = str(item.get("url") or "").strip()
        item_title = _clean_text(item.get("title"), max_len=160)
        content = _clean_text(item.get("extracted_text") or item.get("content"), max_len=400)
        score = item.get("score")
        rows.append(
            "<li>"
            f"<a href=\"{url}\">{item_title or url}</a>"
            f"<div>score={score}</div>"
            f"<p>{content}</p>"
            "</li>"
        )
    html = (
        "<!doctype html><html><head><meta charset=\"utf-8\">"
        f"<title>{title}</title></head><body>"
        f"<h1>{title}</h1><ol>{''.join(rows)}</ol></body></html>"
    )
    index_path = output_dir / "index.html"
    index_path.write_text(html, encoding="utf-8")
    return index_path


def _slugify(value: str, max_len: int = 80) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    if not text:
        return "item"
    return text[:max_len].strip("-") or "item"


def _normalize_url_list(input_data: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    single_url = input_data.get("url")
    if isinstance(single_url, str) and single_url.strip():
        urls.append(single_url.strip())

    multi_urls = input_data.get("urls")
    if isinstance(multi_urls, list):
        for item in multi_urls:
            if isinstance(item, str) and item.strip():
                urls.append(item.strip())

    deduped: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def _normalize_tavily_include_raw_content(value: Any, default: str | bool = "text") -> str | bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if not normalized:
            return default
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
        return normalized
    return bool(value)


def _normalize_optional_str_list(value: Any) -> list[str] | None:
    if not isinstance(value, list):
        return None
    normalized = [str(item).strip() for item in value if str(item).strip()]
    return normalized or None


def _normalize_tavily_instructions(input_data: dict[str, Any]) -> str:
    return str(input_data.get("instructions") or input_data.get("instruction") or "").strip()


def _normalize_positive_int(value: Any, default: int, *, minimum: int = 1, maximum: int | None = None) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        normalized = default
    normalized = max(minimum, normalized)
    if maximum is not None:
        normalized = min(maximum, normalized)
    return normalized


def _normalize_tavily_score(value: Any) -> tuple[float | None, float | None]:
    if not isinstance(value, (int, float)):
        return None, None
    raw_score = float(value)
    if 0.0 <= raw_score <= 1.0:
        return round(raw_score * TAVILY_SCORE_SCALE_MAX, 4), round(raw_score, 4)
    return round(raw_score, 4), round(raw_score, 4)


def _build_tavily_search_payload(
    *,
    query: str,
    max_results: int,
    search_depth: str,
    topic: str | None,
    include_raw_content: str | bool,
    include_answer: bool | str | None,
    chunks_per_source: int | None,
    include_images: bool,
    include_domains: list[str] | None = None,
    exclude_domains: list[str] | None = None,
    time_range: str | None = None,
    days: int | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "query": query,
        "search_depth": search_depth,
        "max_results": max(1, min(max_results, 20)),
        "include_raw_content": include_raw_content,
        "include_images": include_images,
    }
    if topic:
        payload["topic"] = topic
    if include_answer is not None:
        payload["include_answer"] = include_answer
    if chunks_per_source is not None and chunks_per_source > 0:
        payload["chunks_per_source"] = chunks_per_source
    if include_domains:
        payload["include_domains"] = include_domains
    if exclude_domains:
        payload["exclude_domains"] = exclude_domains
    if time_range:
        payload["time_range"] = time_range
    if days is not None and days > 0:
        payload["days"] = days
    return payload


def _extract_tavily_search_results(raw_response: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    raw_results = raw_response.get("results")
    raw_rows = [item for item in raw_results if isinstance(item, dict)] if isinstance(raw_results, list) else []
    result_rows: list[dict[str, Any]] = []
    for item in raw_rows:
        normalized_row = dict(item)
        normalized_score, raw_score = _normalize_tavily_score(item.get("score"))
        if normalized_score is not None:
            normalized_row["score"] = normalized_score
            normalized_row["score_scale"] = "0-10"
            if raw_score is not None and normalized_score != raw_score:
                normalized_row["score_raw"] = raw_score
        result_rows.append(normalized_row)

    extracted_results: list[dict[str, Any]] = []
    for index, item in enumerate(result_rows[:20], start=1):
        extracted_results.append(
            {
                "rank": index,
                "title": _clean_text(item.get("title"), max_len=220),
                "url": item.get("url"),
                "content": _clean_text(item.get("content"), max_len=700),
                "raw_content": _clean_text(item.get("raw_content"), max_len=2000),
                "extracted_text": _clean_text(item.get("content") or item.get("raw_content"), max_len=700),
                "score": item.get("score"),
                "favicon": item.get("favicon"),
                "images": item.get("images") if isinstance(item.get("images"), list) else [],
            }
        )
    return result_rows, extracted_results


def _tavily_api_key() -> str:
    api_key = str(
        os.getenv("TAVILY_SEARCH_API_KEY")
        or os.getenv("TAVILY_RESEARCH_API_KEY")
        or os.getenv("TAVILY_API_KEY")
        or ""
    ).strip()
    if not api_key:
        raise RuntimeError("Missing required environment variable: TAVILY_SEARCH_API_KEY, TAVILY_RESEARCH_API_KEY, or TAVILY_API_KEY")
    return api_key


def _tavily_json_request(
    api_url: str,
    payload: dict[str, Any] | None,
    *,
    timeout_seconds: int,
    method: str = "POST",
) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {_tavily_api_key()}"}
    return _http_json_request(
        api_url,
        payload,
        timeout_seconds=timeout_seconds,
        method=method,
        headers=headers,
    )


def _write_json_file(path: Path, payload: Any) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return path


def _write_page_exports(
    output_dir: Path,
    pages: list[dict[str, Any]],
    *,
    prefix: str,
    extension: str = ".md",
) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for index, page in enumerate(pages, start=1):
        url = str(page.get("url") or "").strip()
        title = _clean_text(page.get("title"), max_len=160) or url or f"{prefix}-{index}"
        raw_content = page.get("raw_content")
        content = raw_content if isinstance(raw_content, str) and raw_content.strip() else str(page.get("content") or "")
        if not content.strip():
            continue
        file_name = f"{index:02d}_{_slugify(title)}{extension}"
        file_path = output_dir / file_name
        file_path.write_text(content, encoding="utf-8")
        artifacts.append(
            {
                "path": str(file_path),
                "url": url or None,
                "title": title,
                "content_type": "text/markdown" if extension == ".md" else "text/plain",
            }
        )
    return artifacts


def _tool_person_search(input_data: dict[str, Any]) -> dict[str, Any]:
    from person_search.workflow import run_workflow

    name = str(input_data.get("name") or input_data.get("query") or "").strip()
    if not name:
        raise RuntimeError("Missing required input: name (or query)")

    max_results = int(input_data.get("max_results", 5))
    delay = float(input_data.get("delay", 1.0))
    request_timeout = float(input_data.get("request_timeout", 10.0))
    download_dir_input = input_data.get("download_dir")
    seen_urls_file = input_data.get("seen_urls")
    no_cache = bool(input_data.get("no_cache", False))
    temp_download_dir = False
    if download_dir_input:
        download_dir = str(Path(str(download_dir_input)).expanduser().resolve())
        Path(download_dir).mkdir(parents=True, exist_ok=True)
    else:
        temp_download_dir = True
        download_dir = str(Path(tempfile.mkdtemp(prefix="person_search_html_")))

    with redirect_stdout(sys.stderr):
        results = run_workflow(
            name,
            max_search_results=max_results,
            fetch_delay_seconds=delay,
            request_timeout=request_timeout,
            download_dir=download_dir,
            seen_urls_file=seen_urls_file,
            use_seen_cache=not no_cache,
        )

    html_files = [item.html_path for item in results if getattr(item, "html_path", None)]
    return {
        "name": name,
        "count": len(results),
        "download_dir": download_dir,
        "results": _serialize_person_search(results),
        "html_files": html_files,
        "raw_files": html_files,
        "note": (
            "download_dir was not provided; HTML files were written to a temporary directory."
            if temp_download_dir
            else None
        ),
    }


def _tool_x_get_user_posts_api(input_data: dict[str, Any]) -> dict[str, Any]:
    username = str(input_data.get("username") or "").strip().lstrip("@")
    if not username:
        raise RuntimeError("Missing required input: username")

    max_results = int(input_data.get("max_results", 10))
    raw = bool(input_data.get("raw", False))
    download_media = bool(input_data.get("download_media", False))
    max_video_bitrate = int(input_data.get("max_video_bitrate", 800000))
    media_dir = input_data.get("media_dir")
    timeout_seconds = int(input_data.get("timeout_seconds", 600))

    out_input = input_data.get("output")
    temp_output = False
    if out_input:
        output_path = Path(str(out_input)).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        temp_output = True
        output_path = Path(tempfile.mkdtemp(prefix="x_posts_")) / "tweets.json"

    cmd = [
        sys.executable,
        str(_INTEGRATION_DIR / "get_user_posts_api.py"),
        username,
        "--max-results",
        str(max_results),
        "--output",
        str(output_path),
    ]
    if raw:
        cmd.append("--raw")
    if download_media:
        cmd.append("--download-media")
        cmd.extend(["--max-video-bitrate", str(max_video_bitrate)])
        if media_dir:
            cmd.extend(["--media-dir", str(media_dir)])

    stdout, stderr = _run_cmd(cmd, timeout_seconds=timeout_seconds)
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    tweets = payload.get("tweets") if isinstance(payload, dict) else None
    extracted_posts: list[dict[str, Any]] = []
    if isinstance(tweets, list):
        for tweet in tweets[:20]:
            if not isinstance(tweet, dict):
                continue
            extracted_posts.append(
                {
                    "id": tweet.get("id"),
                    "created_at": tweet.get("created_at"),
                    "extracted_text": _clean_text(tweet.get("text"), max_len=500),
                }
            )

    result = {
        "username": username,
        "output_path": str(output_path),
        "raw_files": [str(output_path)],
        "result": payload,
        "extracted_posts": extracted_posts,
        "stdout": stdout[:2000],
        "stderr": stderr[:2000],
    }
    if temp_output:
        result["note"] = "Output was written to a temp path. Pass output to control persistence."
    return result


def _tool_linkedin_download_html_ocr(input_data: dict[str, Any]) -> dict[str, Any]:
    reset_session = bool(input_data.get("reset_session", False))
    profile = str(input_data.get("profile") or "").strip()
    output_dir = str(input_data.get("output_dir") or "linkedin_html")
    timeout_seconds = int(input_data.get("timeout_seconds", 1200))

    if reset_session and not profile:
        context_file = _INTEGRATION_DIR / ".linkedin_context_id"
        if context_file.exists():
            context_file.unlink()
            return {"reset_session": True, "session_reset": True}
        return {"reset_session": True, "session_reset": False}

    if not profile:
        raise RuntimeError("Missing required input: profile (unless reset_session=true)")

    output_path = Path(output_dir).expanduser().resolve()
    output_path.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        str(_INTEGRATION_DIR / "download_linkedin_html_ocr.py"),
        profile,
        "--output-dir",
        str(output_path),
    ]
    if reset_session:
        cmd.append("--reset-session")

    stdout, stderr = _run_cmd(cmd, timeout_seconds=timeout_seconds)

    html_files = sorted(str(p) for p in output_path.glob("*.html") if p.is_file())
    json_files = sorted(str(p) for p in output_path.glob("*.json") if p.is_file())
    contact_info: dict[str, Any] = {}
    contact_info_path = ""
    for json_file in reversed(json_files):
        candidate_path = Path(json_file)
        if not candidate_path.name.startswith("contact_info_"):
            continue
        try:
            parsed = json.loads(candidate_path.read_text(encoding="utf-8", errors="replace"))
        except Exception:
            continue
        if isinstance(parsed, dict):
            contact_info = parsed
            contact_info_path = json_file
            break

    extracted_pages: list[dict[str, Any]] = []
    for file_path in html_files[:5]:
        path = Path(file_path)
        html_value = _read_text_file(path)
        extracted_pages.append(
            {
                "file": file_path,
                "extracted_text": _extract_text_from_html(html_value, max_len=2500),
            }
        )

    return {
        "profile": profile,
        "output_dir": str(output_path),
        "html_files": html_files,
        "json_files": json_files,
        "raw_files": [*html_files, *json_files],
        "file_count": len(html_files),
        "extracted_pages": extracted_pages,
        "contact_info": contact_info,
        "contact_info_path": contact_info_path,
        "stdout": stdout[:3000],
        "stderr": stderr[:3000],
    }


def _tool_google_serp_person_search(input_data: dict[str, Any]) -> dict[str, Any]:
    target_name = str(input_data.get("target_name") or input_data.get("query") or "").strip()
    if not target_name:
        raise RuntimeError("Missing required input: target_name (or query)")

    max_results = int(input_data.get("max_results", 10))
    start = int(input_data.get("start", 1))
    timeout = int(input_data.get("timeout", 20))
    timeout_seconds = int(input_data.get("timeout_seconds", 600))

    out_input = input_data.get("output_dir")
    temp_output = False
    if out_input:
        output_dir = Path(str(out_input)).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        temp_output = True
        output_dir = Path(tempfile.mkdtemp(prefix="google_serp_"))

    cmd = [
        sys.executable,
        str(_INTEGRATION_DIR / "search_google_serp.py"),
        target_name,
        "--max-results",
        str(max_results),
        "--start",
        str(start),
        "--timeout",
        str(timeout),
        "--output-dir",
        str(output_dir),
    ]

    if "base_url" in input_data and input_data.get("base_url"):
        cmd.extend(["--base-url", str(input_data["base_url"])])
    if "max_retries" in input_data:
        cmd.extend(["--max-retries", str(int(input_data["max_retries"]))])
    if "retry_backoff" in input_data:
        cmd.extend(["--retry-backoff", str(float(input_data["retry_backoff"]))])
    if "retry_jitter" in input_data:
        cmd.extend(["--retry-jitter", str(float(input_data["retry_jitter"]))])
    if "api_delay" in input_data:
        cmd.extend(["--api-delay", str(float(input_data["api_delay"]))])

    stdout, stderr = _run_cmd(cmd, timeout_seconds=timeout_seconds)

    summary_path = output_dir / "search_results.json"
    api_response_path = output_dir / "api_response.json"
    index_path = output_dir / "index.html"

    summary = {}
    if summary_path.exists():
        summary = json.loads(summary_path.read_text(encoding="utf-8"))

    html_result_files: list[str] = []
    if isinstance(summary, dict):
        results = summary.get("results")
        if isinstance(results, list):
            for item in results:
                if not isinstance(item, dict):
                    continue
                local_file = item.get("local_file")
                if isinstance(local_file, str) and local_file.strip():
                    html_result_files.append(str((output_dir / local_file).resolve()))

    raw_files = [
        str(summary_path),
        str(api_response_path),
        str(index_path),
        *html_result_files,
    ]
    extracted_results: list[dict[str, Any]] = []
    if isinstance(summary, dict):
        items = summary.get("results")
        if isinstance(items, list):
            for item in items[:20]:
                if not isinstance(item, dict):
                    continue
                extracted_results.append(
                    {
                        "rank": item.get("rank"),
                        "title": _clean_text(item.get("title"), max_len=200),
                        "url": item.get("url"),
                        "extracted_text": _clean_text(item.get("snippet"), max_len=500),
                    }
                )

    result = {
        "target_name": target_name,
        "output_dir": str(output_dir),
        "summary_path": str(summary_path),
        "api_response_path": str(api_response_path),
        "index_path": str(index_path),
        "html_files": html_result_files,
        "raw_files": raw_files,
        "summary": summary,
        "extracted_results": extracted_results,
        "stdout": stdout[:3000],
        "stderr": stderr[:3000],
    }

    if temp_output:
        result["note"] = "Output was written to a temporary directory. Pass output_dir to persist files."

    return result


def _tool_web_search(input_data: dict[str, Any]) -> dict[str, Any]:
    query = str(input_data.get("query") or input_data.get("target_name") or input_data.get("input") or "").strip()
    if not query:
        raise RuntimeError("Missing required input: query (or target_name/input)")

    api_url = str(os.getenv("TAVILY_API_URL") or "https://api.tavily.com/search").strip()
    max_results = int(input_data.get("max_results", 10))
    timeout_seconds = int(input_data.get("timeout_seconds", 120))
    search_depth = str(input_data.get("search_depth") or "advanced").strip() or "advanced"
    topic = str(input_data.get("topic") or "general").strip() or "general"
    include_raw_content = _normalize_tavily_include_raw_content(input_data.get("include_raw_content"), default="text")
    include_answer = input_data.get("include_answer", "advanced")
    requested_chunks_per_source = input_data.get("chunks_per_source", TAVILY_SEARCH_CHUNKS_PER_SOURCE_DEFAULT)
    chunks_per_source = _normalize_positive_int(
        requested_chunks_per_source,
        TAVILY_SEARCH_CHUNKS_PER_SOURCE_DEFAULT,
        maximum=TAVILY_SEARCH_CHUNKS_PER_SOURCE_MAX,
    )
    include_images = bool(input_data.get("include_images", False))
    include_domains = _normalize_optional_str_list(input_data.get("include_domains"))
    exclude_domains = _normalize_optional_str_list(input_data.get("exclude_domains"))
    time_range = str(input_data.get("time_range") or "").strip() or None
    days_value = input_data.get("days")
    days = int(days_value) if days_value not in (None, "") else None

    out_input = input_data.get("output_dir")
    temp_output = False
    if out_input:
        output_dir = Path(str(out_input)).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        temp_output = True
        output_dir = Path(tempfile.mkdtemp(prefix="tavily_web_search_"))

    payload = _build_tavily_search_payload(
        query=query,
        max_results=max_results,
        search_depth=search_depth,
        topic=topic,
        include_raw_content=include_raw_content,
        include_answer=include_answer,
        chunks_per_source=chunks_per_source,
        include_images=include_images,
        include_domains=include_domains,
        exclude_domains=exclude_domains,
        time_range=time_range,
        days=days,
    )
    raw_response = _tavily_json_request(api_url, payload, timeout_seconds=timeout_seconds)
    raw_path = _write_json_file(output_dir / "tavily_search_response.json", raw_response)

    result_rows, extracted_results = _extract_tavily_search_results(raw_response)

    summary = {
        "query": query,
        "topic": topic,
        "follow_up_questions": raw_response.get("follow_up_questions"),
        "answer": raw_response.get("answer"),
        "images": raw_response.get("images") if isinstance(raw_response.get("images"), list) else [],
        "results_found": len(result_rows),
        "search_depth": search_depth,
        "include_raw_content": include_raw_content,
        "include_answer": include_answer,
        "chunks_per_source": chunks_per_source,
        "chunks_per_source_requested": requested_chunks_per_source,
        "score_scale": "0-10 normalized from Tavily 0-1 scores",
        "response_time": raw_response.get("response_time"),
        "request_id": raw_response.get("request_id"),
        "results": result_rows,
    }
    summary_path = _write_json_file(output_dir / "search_results.json", summary)
    index_path = _write_search_index(output_dir, f"Tavily search results for {query}", extracted_results)

    result = {
        "query": query,
        "topic": topic,
        "follow_up_questions": raw_response.get("follow_up_questions"),
        "answer": raw_response.get("answer"),
        "images": raw_response.get("images") if isinstance(raw_response.get("images"), list) else [],
        "results": result_rows,
        "response_time": raw_response.get("response_time"),
        "request_id": raw_response.get("request_id"),
        "results_found": len(result_rows),
        "output_dir": str(output_dir),
        "summary_path": str(summary_path),
        "api_response_path": str(raw_path),
        "index_path": str(index_path),
        "raw_files": [str(summary_path), str(raw_path), str(index_path)],
        "summary": summary,
        "extracted_results": extracted_results,
        "payload": payload,
    }
    if temp_output:
        result["note"] = "Output was written to a temporary directory. Pass output_dir to persist files."
    return result


def _tool_tavily_person_search(input_data: dict[str, Any]) -> dict[str, Any]:
    target_name = str(input_data.get("target_name") or input_data.get("name") or "").strip()
    requested_query = str(input_data.get("query") or "").strip()
    search_query = requested_query or target_name
    if not target_name and requested_query:
        target_name = requested_query
    if not target_name:
        raise RuntimeError("Missing required input: target_name (or query/name)")

    api_url = str(os.getenv("TAVILY_API_URL") or "https://api.tavily.com/search").strip()
    max_results = int(input_data.get("max_results", 10))
    timeout_seconds = int(input_data.get("timeout_seconds", 120))
    search_depth = str(input_data.get("search_depth") or "advanced").strip() or "advanced"
    include_raw_content = _normalize_tavily_include_raw_content(input_data.get("include_raw_content"), default="text")
    include_answer = input_data.get("include_answer", "advanced")
    requested_chunks_per_source = input_data.get("chunks_per_source", TAVILY_SEARCH_CHUNKS_PER_SOURCE_DEFAULT)
    chunks_per_source = _normalize_positive_int(
        requested_chunks_per_source,
        TAVILY_SEARCH_CHUNKS_PER_SOURCE_DEFAULT,
        maximum=TAVILY_SEARCH_CHUNKS_PER_SOURCE_MAX,
    )
    include_images = bool(input_data.get("include_images", False))
    include_domains = _normalize_optional_str_list(input_data.get("include_domains"))
    exclude_domains = _normalize_optional_str_list(input_data.get("exclude_domains"))
    topic = str(input_data.get("topic") or "general").strip() or "general"
    time_range = str(input_data.get("time_range") or "").strip() or None
    days_value = input_data.get("days")
    days = int(days_value) if days_value not in (None, "") else None

    out_input = input_data.get("output_dir")
    temp_output = False
    if out_input:
        output_dir = Path(str(out_input)).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        temp_output = True
        output_dir = Path(tempfile.mkdtemp(prefix="tavily_search_"))

    payload = _build_tavily_search_payload(
        query=search_query,
        max_results=max_results,
        search_depth=search_depth,
        topic=topic,
        include_raw_content=include_raw_content,
        include_answer=include_answer,
        chunks_per_source=chunks_per_source,
        include_images=include_images,
        include_domains=include_domains,
        exclude_domains=exclude_domains,
        time_range=time_range,
        days=days,
    )
    raw_response = _tavily_json_request(api_url, payload, timeout_seconds=timeout_seconds)
    raw_path = output_dir / "tavily_response.json"
    raw_path.write_text(json.dumps(raw_response, ensure_ascii=True, indent=2), encoding="utf-8")

    result_rows, extracted_results = _extract_tavily_search_results(raw_response)

    summary = {
        "target_name": target_name,
        "query": raw_response.get("query") or search_query,
        "requested_query": requested_query,
        "topic": topic,
        "follow_up_questions": raw_response.get("follow_up_questions"),
        "answer": raw_response.get("answer"),
        "images": raw_response.get("images") if isinstance(raw_response.get("images"), list) else [],
        "results_found": len(result_rows),
        "search_depth": search_depth,
        "include_raw_content": include_raw_content,
        "include_answer": include_answer,
        "chunks_per_source": chunks_per_source,
        "chunks_per_source_requested": requested_chunks_per_source,
        "score_scale": "0-10 normalized from Tavily 0-1 scores",
        "response_time": raw_response.get("response_time"),
        "request_id": raw_response.get("request_id"),
        "results": result_rows,
    }
    summary_path = output_dir / "search_results.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")
    index_path = _write_search_index(output_dir, f"Tavily search results for {target_name}", extracted_results)

    result = {
        "target_name": target_name,
        "query": raw_response.get("query") or search_query,
        "requested_query": requested_query,
        "topic": topic,
        "follow_up_questions": raw_response.get("follow_up_questions"),
        "answer": raw_response.get("answer"),
        "images": raw_response.get("images") if isinstance(raw_response.get("images"), list) else [],
        "results": result_rows,
        "response_time": raw_response.get("response_time"),
        "request_id": raw_response.get("request_id"),
        "results_found": len(result_rows),
        "output_dir": str(output_dir),
        "summary_path": str(summary_path),
        "api_response_path": str(raw_path),
        "index_path": str(index_path),
        "html_files": [],
        "raw_files": [str(summary_path), str(raw_path), str(index_path)],
        "summary": summary,
        "extracted_results": extracted_results,
        "payload": payload,
    }
    if temp_output:
        result["note"] = "Output was written to a temporary directory. Pass output_dir to persist files."
    return result


def _tool_extract_webpage(input_data: dict[str, Any]) -> dict[str, Any]:
    urls = _normalize_url_list(input_data)
    if not urls:
        raise RuntimeError("Missing required input: url or urls")

    api_url = str(os.getenv("TAVILY_EXTRACT_API_URL") or "https://api.tavily.com/extract").strip()
    timeout_seconds = int(input_data.get("timeout_seconds", 180))
    query = str(input_data.get("query") or input_data.get("input") or "").strip()
    include_images = bool(input_data.get("include_images", False))
    include_favicon = bool(input_data.get("include_favicon", True))
    extract_depth = str(input_data.get("extract_depth") or "advanced").strip() or "advanced"
    format_value = str(input_data.get("format") or "text").strip().lower() or "text"
    chunks_per_source = _normalize_positive_int(
        input_data.get("chunks_per_source", TAVILY_EXTRACT_CHUNKS_PER_SOURCE_DEFAULT),
        TAVILY_EXTRACT_CHUNKS_PER_SOURCE_DEFAULT,
        maximum=TAVILY_EXTRACT_CHUNKS_PER_SOURCE_MAX,
    )

    out_input = input_data.get("output_dir")
    temp_output = False
    if out_input:
        output_dir = Path(str(out_input)).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        temp_output = True
        output_dir = Path(tempfile.mkdtemp(prefix="tavily_extract_"))

    payload = {
        "urls": urls,
        "extract_depth": extract_depth,
        "format": format_value,
        "chunks_per_source": chunks_per_source,
        "include_images": include_images,
        "include_favicon": include_favicon,
    }
    if query:
        payload["query"] = query
    raw_response = _tavily_json_request(api_url, payload, timeout_seconds=timeout_seconds)
    raw_path = _write_json_file(output_dir / "extract_response.json", raw_response)

    raw_results = raw_response.get("results")
    result_rows = [item for item in raw_results if isinstance(item, dict)] if isinstance(raw_results, list) else []
    extracted_pages: list[dict[str, Any]] = []
    for item in result_rows:
        content = item.get("raw_content") or item.get("content")
        extracted_pages.append(
            {
                "title": _clean_text(item.get("title"), max_len=220),
                "url": item.get("url"),
                "extracted_text": _clean_text(content, max_len=1200),
                "images": item.get("images") if isinstance(item.get("images"), list) else [],
                "favicon": item.get("favicon"),
            }
        )

    page_files = _write_page_exports(
        output_dir,
        result_rows,
        prefix="extract",
        extension=".md" if format_value == "markdown" else ".txt",
    )
    page_manifest_path = _write_json_file(output_dir / "page_manifest.json", page_files)
    summary = {
        "urls": urls,
        "query": query,
        "results_found": len(result_rows),
        "failed_results_count": max(0, len(urls) - len(result_rows)),
        "chunks_per_source": chunks_per_source,
        "extract_depth": extract_depth,
        "format": format_value,
        "results": extracted_pages,
    }
    summary_path = _write_json_file(output_dir / "extract_summary.json", summary)
    index_path = _write_search_index(output_dir, "Tavily extract results", extracted_pages)

    result = {
        "url": urls[0] if len(urls) == 1 else None,
        "urls": urls,
        "query": query,
        "output_dir": str(output_dir),
        "summary_path": str(summary_path),
        "api_response_path": str(raw_path),
        "index_path": str(index_path),
        "page_manifest_path": str(page_manifest_path),
        "page_files": page_files,
        "raw_files": [str(summary_path), str(raw_path), str(index_path), str(page_manifest_path), *[item["path"] for item in page_files]],
        "results_found": len(result_rows),
        "failed_results_count": max(0, len(urls) - len(result_rows)),
        "summary": summary,
        "extracted_pages": extracted_pages,
        "payload": payload,
    }
    if temp_output:
        result["note"] = "Output was written to a temporary directory. Pass output_dir to persist files."
    return result


def _tool_crawl_webpage(input_data: dict[str, Any]) -> dict[str, Any]:
    url = str(input_data.get("url") or "").strip()
    if not url:
        raise RuntimeError("Missing required input: url")

    api_url = str(os.getenv("TAVILY_CRAWL_API_URL") or "https://api.tavily.com/crawl").strip()
    timeout_seconds = int(input_data.get("timeout_seconds", 300))
    max_depth = int(input_data.get("max_depth", 2))
    max_breadth = int(input_data.get("max_breadth", 20))
    limit = int(input_data.get("limit", 20))
    instructions = _normalize_tavily_instructions(input_data)
    select_paths = input_data.get("select_paths")
    exclude_paths = input_data.get("exclude_paths")
    format_value = str(input_data.get("format") or "markdown").strip().lower() or "markdown"

    out_input = input_data.get("output_dir")
    temp_output = False
    if out_input:
        output_dir = Path(str(out_input)).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        temp_output = True
        output_dir = Path(tempfile.mkdtemp(prefix="tavily_crawl_"))

    payload: dict[str, Any] = {
        "url": url,
        "max_depth": max(1, max_depth),
        "max_breadth": max(1, max_breadth),
        "limit": max(1, limit),
        "format": format_value,
    }
    if instructions:
        payload["instructions"] = instructions
    if isinstance(select_paths, list) and select_paths:
        payload["select_paths"] = [item for item in select_paths if isinstance(item, str) and item.strip()]
    if isinstance(exclude_paths, list) and exclude_paths:
        payload["exclude_paths"] = [item for item in exclude_paths if isinstance(item, str) and item.strip()]

    raw_response = _tavily_json_request(api_url, payload, timeout_seconds=timeout_seconds)
    raw_path = _write_json_file(output_dir / "crawl_response.json", raw_response)

    raw_results = raw_response.get("results")
    result_rows = [item for item in raw_results if isinstance(item, dict)] if isinstance(raw_results, list) else []
    extracted_pages: list[dict[str, Any]] = []
    for item in result_rows:
        content = item.get("raw_content") or item.get("content")
        extracted_pages.append(
            {
                "title": _clean_text(item.get("title"), max_len=220),
                "url": item.get("url"),
                "extracted_text": _clean_text(content, max_len=1200),
                "favicon": item.get("favicon"),
            }
        )

    page_files = _write_page_exports(
        output_dir,
        result_rows,
        prefix="crawl",
        extension=".md" if format_value == "markdown" else ".txt",
    )
    page_manifest_path = _write_json_file(output_dir / "page_manifest.json", page_files)
    summary = {
        "url": url,
        "results_found": len(result_rows),
        "max_depth": max_depth,
        "max_breadth": max_breadth,
        "limit": limit,
        "results": extracted_pages,
    }
    summary_path = _write_json_file(output_dir / "crawl_summary.json", summary)
    index_path = _write_search_index(output_dir, f"Tavily crawl results for {url}", extracted_pages)

    result = {
        "url": url,
        "output_dir": str(output_dir),
        "summary_path": str(summary_path),
        "api_response_path": str(raw_path),
        "index_path": str(index_path),
        "page_manifest_path": str(page_manifest_path),
        "page_files": page_files,
        "raw_files": [str(summary_path), str(raw_path), str(index_path), str(page_manifest_path), *[item["path"] for item in page_files]],
        "results_found": len(result_rows),
        "summary": summary,
        "extracted_pages": extracted_pages,
    }
    if temp_output:
        result["note"] = "Output was written to a temporary directory. Pass output_dir to persist files."
    return result


def _tool_map_webpage(input_data: dict[str, Any]) -> dict[str, Any]:
    url = str(input_data.get("url") or "").strip()
    if not url:
        raise RuntimeError("Missing required input: url")

    api_url = str(os.getenv("TAVILY_MAP_API_URL") or "https://api.tavily.com/map").strip()
    timeout_seconds = int(input_data.get("timeout_seconds", 120))
    max_depth = int(input_data.get("max_depth", 2))
    max_breadth = int(input_data.get("max_breadth", 50))
    limit = int(input_data.get("limit", 100))
    instructions = _normalize_tavily_instructions(input_data)
    select_paths = input_data.get("select_paths")
    exclude_paths = input_data.get("exclude_paths")

    out_input = input_data.get("output_dir")
    temp_output = False
    if out_input:
        output_dir = Path(str(out_input)).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        temp_output = True
        output_dir = Path(tempfile.mkdtemp(prefix="tavily_map_"))

    payload: dict[str, Any] = {
        "url": url,
        "max_depth": max(1, max_depth),
        "max_breadth": max(1, max_breadth),
        "limit": max(1, limit),
    }
    if instructions:
        payload["instructions"] = instructions
    if isinstance(select_paths, list) and select_paths:
        payload["select_paths"] = [item for item in select_paths if isinstance(item, str) and item.strip()]
    if isinstance(exclude_paths, list) and exclude_paths:
        payload["exclude_paths"] = [item for item in exclude_paths if isinstance(item, str) and item.strip()]

    raw_response = _tavily_json_request(api_url, payload, timeout_seconds=timeout_seconds)
    raw_path = _write_json_file(output_dir / "map_response.json", raw_response)

    raw_results = raw_response.get("results")
    urls = [item for item in raw_results if isinstance(item, str) and item.strip()] if isinstance(raw_results, list) else []
    extracted_results = [{"title": candidate, "url": candidate, "extracted_text": ""} for candidate in urls[:100]]
    summary = {
        "url": url,
        "results_found": len(urls),
        "max_depth": max_depth,
        "max_breadth": max_breadth,
        "limit": limit,
        "results": extracted_results,
    }
    summary_path = _write_json_file(output_dir / "map_summary.json", summary)
    index_path = _write_search_index(output_dir, f"Tavily site map for {url}", extracted_results)

    result = {
        "url": url,
        "output_dir": str(output_dir),
        "summary_path": str(summary_path),
        "api_response_path": str(raw_path),
        "index_path": str(index_path),
        "raw_files": [str(summary_path), str(raw_path), str(index_path)],
        "results_found": len(urls),
        "urls": urls,
        "summary": summary,
    }
    if temp_output:
        result["note"] = "Output was written to a temporary directory. Pass output_dir to persist files."
    return result


def _tool_tavily_research(input_data: dict[str, Any]) -> dict[str, Any]:
    research_input = str(input_data.get("input") or input_data.get("query") or input_data.get("target_name") or "").strip()
    if not research_input:
        raise RuntimeError("Missing required input: input (or query/target_name)")

    api_url = str(os.getenv("TAVILY_RESEARCH_API_URL") or "https://api.tavily.com/research").strip()
    timeout_seconds = int(input_data.get("timeout_seconds", 300))
    poll_interval_seconds = float(input_data.get("poll_interval_seconds", 2.0))
    model = str(input_data.get("model") or "auto").strip() or "auto"
    citation_format = str(input_data.get("citation_format") or "numbered").strip() or "numbered"
    output_schema = input_data.get("output_schema")

    out_input = input_data.get("output_dir")
    temp_output = False
    if out_input:
        output_dir = Path(str(out_input)).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        temp_output = True
        output_dir = Path(tempfile.mkdtemp(prefix="tavily_research_"))

    create_payload: dict[str, Any] = {
        "input": research_input,
        "model": model,
        "stream": False,
        "citation_format": citation_format,
    }
    if isinstance(output_schema, dict) and output_schema.get("properties"):
        create_payload["output_schema"] = output_schema

    create_response = _tavily_json_request(
        api_url,
        create_payload,
        timeout_seconds=min(timeout_seconds, 120),
        method="POST",
    )
    create_path = output_dir / "tavily_research_create_response.json"
    create_path.write_text(json.dumps(create_response, ensure_ascii=True, indent=2), encoding="utf-8")

    request_id = str(create_response.get("request_id") or "").strip()
    if not request_id:
        raise RuntimeError("Tavily research response did not include request_id")

    status = str(create_response.get("status") or "").strip().lower()
    deadline = time.time() + timeout_seconds
    status_response = create_response

    while status not in {"completed", "failed"} and time.time() < deadline:
        time.sleep(max(0.5, poll_interval_seconds))
        status_response = _tavily_json_request(
            f"{api_url.rstrip('/')}/{request_id}",
            payload=None,
            timeout_seconds=min(120, timeout_seconds),
            method="GET",
        )
        status = str(status_response.get("status") or "").strip().lower()

    if status not in {"completed", "failed"}:
        raise RuntimeError(f"Tavily research task {request_id} did not finish within {timeout_seconds} seconds")

    status_path = output_dir / "tavily_research_status.json"
    status_path.write_text(json.dumps(status_response, ensure_ascii=True, indent=2), encoding="utf-8")

    sources = status_response.get("sources")
    source_rows = [item for item in sources if isinstance(item, dict)] if isinstance(sources, list) else []
    extracted_results: list[dict[str, Any]] = []
    for index, item in enumerate(source_rows[:50], start=1):
        extracted_results.append(
            {
                "rank": index,
                "title": _clean_text(item.get("title"), max_len=220),
                "url": item.get("url"),
                "extracted_text": _clean_text(item.get("content") or item.get("snippet"), max_len=700),
                "favicon": item.get("favicon"),
            }
        )

    content = status_response.get("content")
    content_text = json.dumps(content, ensure_ascii=True, indent=2) if isinstance(content, (dict, list)) else str(content or "")
    report_path = output_dir / "research_report.md"
    report_path.write_text(content_text, encoding="utf-8")

    summary = {
        "input": research_input,
        "request_id": request_id,
        "status": status,
        "model": status_response.get("model") or create_response.get("model") or model,
        "citation_format": citation_format,
        "response_time": status_response.get("response_time") or create_response.get("response_time"),
        "sources_found": len(source_rows),
        "report_preview": _clean_text(content_text, max_len=1200),
        "results": extracted_results,
    }
    summary_path = output_dir / "research_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")
    index_path = _write_search_index(output_dir, f"Tavily research sources for {research_input}", extracted_results)

    result = {
        "input": research_input,
        "request_id": request_id,
        "status": status,
        "output_dir": str(output_dir),
        "summary_path": str(summary_path),
        "create_response_path": str(create_path),
        "status_response_path": str(status_path),
        "report_path": str(report_path),
        "index_path": str(index_path),
        "html_files": [],
        "raw_files": [str(summary_path), str(create_path), str(status_path), str(report_path), str(index_path)],
        "summary": summary,
        "report_content": content,
        "sources": source_rows,
        "extracted_results": extracted_results,
    }
    if temp_output:
        result["note"] = "Output was written to a temporary directory. Pass output_dir to persist files."
    if status == "failed":
        result["error"] = status_response.get("error") or "Tavily research task failed"
    return result


def _tool_arxiv_search_and_download(input_data: dict[str, Any]) -> dict[str, Any]:
    author = str(input_data.get("author") or "").strip()
    topic = str(input_data.get("topic") or "").strip()
    if not author and not topic:
        raise RuntimeError("Missing required input: author or topic")

    timeout_seconds = int(input_data.get("timeout_seconds", 1200))
    metadata_file = str(input_data.get("metadata_file") or "metadata.json")

    out_input = input_data.get("output_dir")
    temp_output = False
    if out_input:
        output_dir = Path(str(out_input)).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        temp_output = True
        output_dir = Path(tempfile.mkdtemp(prefix="arxiv_search_"))

    cmd = [
        sys.executable,
        str(_INTEGRATION_DIR / "search_arxiv_and_download.py"),
        "--output-dir",
        str(output_dir),
        "--metadata-file",
        metadata_file,
    ]

    if author:
        cmd.extend(["--author", author])
    if topic:
        cmd.extend(["--topic", topic])

    mapping: dict[str, str] = {
        "max_results": "--max-results",
        "start": "--start",
        "page_size": "--page-size",
        "sort_by": "--sort-by",
        "sort_order": "--sort-order",
        "base_url": "--base-url",
        "api_delay": "--api-delay",
        "download_delay": "--download-delay",
        "timeout": "--timeout",
        "user_agent": "--user-agent",
        "max_retries": "--max-retries",
        "retry_backoff": "--retry-backoff",
    }
    for key, flag in mapping.items():
        if key in input_data and input_data[key] is not None:
            cmd.extend([flag, str(input_data[key])])

    if bool(input_data.get("overwrite", False)):
        cmd.append("--overwrite")
    if bool(input_data.get("no_download", False)):
        cmd.append("--no-download")

    stdout, stderr = _run_cmd(cmd, timeout_seconds=timeout_seconds)

    metadata_path = output_dir / metadata_file
    metadata: dict[str, Any] = {}
    if metadata_path.exists():
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    pdf_files: list[str] = []
    entries = metadata.get("entries") if isinstance(metadata, dict) else None
    if isinstance(entries, list):
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            pdf_file = entry.get("pdf_file")
            if isinstance(pdf_file, str) and pdf_file.strip():
                pdf_files.append(pdf_file)

    raw_files = [str(metadata_path), *pdf_files]
    extracted_entries: list[dict[str, Any]] = []
    if isinstance(entries, list):
        for entry in entries[:20]:
            if not isinstance(entry, dict):
                continue
            extracted_entries.append(
                {
                    "arxiv_id": entry.get("arxiv_id"),
                    "title": _clean_text(entry.get("title"), max_len=250),
                    "published": entry.get("published"),
                    "authors": entry.get("authors"),
                    "affiliations": entry.get("affiliations"),
                    "pdf_url": entry.get("pdf_url"),
                    "extracted_text": _clean_text(entry.get("summary"), max_len=700),
                }
            )

    result = {
        "output_dir": str(output_dir),
        "metadata_path": str(metadata_path),
        "pdf_files": pdf_files,
        "raw_files": raw_files,
        "metadata": metadata,
        "extracted_entries": extracted_entries,
        "stdout": stdout[:3000],
        "stderr": stderr[:3000],
    }

    if temp_output:
        result["note"] = "Output was written to a temporary directory. Pass output_dir to persist files."

    return result


def _tool_arxiv_paper_ingest(input_data: dict[str, Any]) -> dict[str, Any]:
    arxiv_id = str(input_data.get("arxiv_id") or "").strip()
    paper_url = str(input_data.get("paper_url") or input_data.get("url") or "").strip()
    pdf_url = str(input_data.get("pdf_url") or "").strip()
    if not (arxiv_id or paper_url or pdf_url):
        raise RuntimeError("Missing required input: arxiv_id, paper_url/url, or pdf_url")

    timeout_seconds = int(input_data.get("timeout_seconds", 1200))
    metadata_file = str(input_data.get("metadata_file") or "metadata.json")
    text_file = str(input_data.get("text_file") or "paper_text.txt")

    out_input = input_data.get("output_dir")
    temp_output = False
    if out_input:
        output_dir = Path(str(out_input)).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        temp_output = True
        output_dir = Path(tempfile.mkdtemp(prefix="arxiv_paper_"))

    cmd = [
        sys.executable,
        str(_INTEGRATION_DIR / "arxiv_paper_ingest.py"),
        "--output-dir",
        str(output_dir),
        "--metadata-file",
        metadata_file,
        "--text-file",
        text_file,
    ]

    if arxiv_id:
        cmd.extend(["--arxiv-id", arxiv_id])
    if paper_url:
        cmd.extend(["--paper-url", paper_url])
    if pdf_url:
        cmd.extend(["--pdf-url", pdf_url])

    author_hint = str(
        input_data.get("author_hint")
        or input_data.get("person_name")
        or input_data.get("author")
        or input_data.get("name")
        or ""
    ).strip()
    topic_hint = str(input_data.get("topic_hint") or input_data.get("topic") or "").strip()
    if author_hint:
        cmd.extend(["--author-hint", author_hint])
    if topic_hint:
        cmd.extend(["--topic-hint", topic_hint])

    mapping: dict[str, str] = {
        "base_url": "--base-url",
        "timeout": "--timeout",
        "user_agent": "--user-agent",
        "max_retries": "--max-retries",
        "retry_backoff": "--retry-backoff",
        "max_pages": "--max-pages",
        "max_text_chars": "--max-text-chars",
    }
    for key, flag in mapping.items():
        if key in input_data and input_data[key] is not None:
            cmd.extend([flag, str(input_data[key])])

    if bool(input_data.get("overwrite", False)):
        cmd.append("--overwrite")

    stdout, stderr = _run_cmd(cmd, timeout_seconds=timeout_seconds)

    metadata_path = output_dir / metadata_file
    metadata: dict[str, Any] = {}
    if metadata_path.exists():
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    paper = metadata.get("paper") if isinstance(metadata, dict) and isinstance(metadata.get("paper"), dict) else {}
    pdf_file = str(paper.get("pdf_file") or "").strip()
    paper_text_path = str(paper.get("paper_text_path") or "").strip()
    raw_files = [
        str(path)
        for path in [metadata_path, Path(pdf_file) if pdf_file else None, Path(paper_text_path) if paper_text_path else None]
        if path and Path(path).exists()
    ]

    extracted_entry = {
        "arxiv_id": paper.get("arxiv_id"),
        "title": _clean_text(paper.get("title"), max_len=250),
        "published": paper.get("published"),
        "authors": paper.get("authors"),
        "affiliations": paper.get("affiliations"),
        "pdf_url": paper.get("pdf_url"),
        "topics": paper.get("topics"),
        "emails": paper.get("emails"),
        "extracted_text": _clean_text(paper.get("summary") or paper.get("text_excerpt"), max_len=700),
    }

    result = {
        "output_dir": str(output_dir),
        "metadata_path": str(metadata_path),
        "paper_text_path": paper_text_path,
        "pdf_file": pdf_file,
        "pdf_files": [pdf_file] if pdf_file else [],
        "raw_files": raw_files,
        "metadata": metadata,
        "paper": paper,
        "papers": [paper] if paper else [],
        "topics": paper.get("topics") if isinstance(paper.get("topics"), list) else [],
        "emails": paper.get("emails") if isinstance(paper.get("emails"), list) else [],
        "author_contacts": paper.get("author_contacts") if isinstance(paper.get("author_contacts"), list) else [],
        "coauthors": paper.get("coauthors") if isinstance(paper.get("coauthors"), list) else [],
        "extracted_entries": [extracted_entry] if paper else [],
        "stdout": stdout[:3000],
        "stderr": stderr[:3000],
    }

    if temp_output:
        result["note"] = "Output was written to a temporary directory. Pass output_dir to persist files."

    return result


def _run_academic_tool(module_name: str, input_data: dict[str, Any]) -> dict[str, Any]:
    module = __import__(f"academic.{module_name}", fromlist=["run"])
    handler = getattr(module, "run", None)
    if handler is None:
        raise RuntimeError(f"Academic tool module missing run(): {module_name}")
    result = handler(input_data)
    if not isinstance(result, dict):
        raise RuntimeError(f"Academic tool returned non-dict result: {module_name}")
    return result


def _run_technical_tool(module_name: str, input_data: dict[str, Any]) -> dict[str, Any]:
    module = __import__(f"technical.{module_name}", fromlist=["run"])
    handler = getattr(module, "run", None)
    if handler is None:
        raise RuntimeError(f"Technical tool module missing run(): {module_name}")
    result = handler(input_data)
    if not isinstance(result, dict):
        raise RuntimeError(f"Technical tool returned non-dict result: {module_name}")
    return result


def _run_business_tool(module_name: str, input_data: dict[str, Any]) -> dict[str, Any]:
    module = __import__(f"business.{module_name}", fromlist=["run"])
    handler = getattr(module, "run", None)
    if handler is None:
        raise RuntimeError(f"Business tool module missing run(): {module_name}")
    result = handler(input_data)
    if not isinstance(result, dict):
        raise RuntimeError(f"Business tool returned non-dict result: {module_name}")
    return result


def _run_archive_tool(module_name: str, input_data: dict[str, Any]) -> dict[str, Any]:
    module = __import__(f"archive.{module_name}", fromlist=["run"])
    handler = getattr(module, "run", None)
    if handler is None:
        raise RuntimeError(f"Archive tool module missing run(): {module_name}")
    result = handler(input_data)
    if not isinstance(result, dict):
        raise RuntimeError(f"Archive tool returned non-dict result: {module_name}")
    return result


def _run_identity_tool(module_name: str, input_data: dict[str, Any]) -> dict[str, Any]:
    module = __import__(f"identity.{module_name}", fromlist=["run"])
    handler = getattr(module, "run", None)
    if handler is None:
        raise RuntimeError(f"Identity tool module missing run(): {module_name}")
    result = handler(input_data)
    if not isinstance(result, dict):
        raise RuntimeError(f"Identity tool returned non-dict result: {module_name}")
    return result


def _run_safety_tool(module_name: str, input_data: dict[str, Any]) -> dict[str, Any]:
    module = __import__(f"safety.{module_name}", fromlist=["run"])
    handler = getattr(module, "run", None)
    if handler is None:
        raise RuntimeError(f"Safety tool module missing run(): {module_name}")
    result = handler(input_data)
    if not isinstance(result, dict):
        raise RuntimeError(f"Safety tool returned non-dict result: {module_name}")
    return result


def _run_contact_tool(module_name: str, input_data: dict[str, Any]) -> dict[str, Any]:
    module = __import__(f"contact.{module_name}", fromlist=["run"])
    handler = getattr(module, "run", None)
    if handler is None:
        raise RuntimeError(f"Contact tool module missing run(): {module_name}")
    result = handler(input_data)
    if not isinstance(result, dict):
        raise RuntimeError(f"Contact tool returned non-dict result: {module_name}")
    return result


def _run_social_tool(module_name: str, input_data: dict[str, Any]) -> dict[str, Any]:
    module = __import__(f"social.{module_name}", fromlist=["run"])
    handler = getattr(module, "run", None)
    if handler is None:
        raise RuntimeError(f"Social tool module missing run(): {module_name}")
    result = handler(input_data)
    if not isinstance(result, dict):
        raise RuntimeError(f"Social tool returned non-dict result: {module_name}")
    return result


def _run_relationship_tool(module_name: str, input_data: dict[str, Any]) -> dict[str, Any]:
    module = __import__(f"relationship.{module_name}", fromlist=["run"])
    handler = getattr(module, "run", None)
    if handler is None:
        raise RuntimeError(f"Relationship tool module missing run(): {module_name}")
    result = handler(input_data)
    if not isinstance(result, dict):
        raise RuntimeError(f"Relationship tool returned non-dict result: {module_name}")
    return result


def main() -> None:
    _load_env_candidates()

    payload = _read_payload()
    tool_name = str(payload.get("tool", "")).strip()
    input_data = payload.get("input", {}) or {}

    if not isinstance(input_data, dict):
        _emit(False, error="Invalid input payload")
        return

    handlers: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
        "web_search": _tool_web_search,
        "extract_webpage": _tool_extract_webpage,
        "crawl_webpage": _tool_crawl_webpage,
        "map_webpage": _tool_map_webpage,
        "person_search": _tool_person_search,
        "tavily_person_search": _tool_tavily_person_search,
        "tavily_research": _tool_tavily_research,
        "x_get_user_posts_api": _tool_x_get_user_posts_api,
        "linkedin_download_html_ocr": _tool_linkedin_download_html_ocr,
        "google_serp_person_search": _tool_google_serp_person_search,
        "arxiv_search_and_download": _tool_arxiv_search_and_download,
        "arxiv_paper_ingest": _tool_arxiv_paper_ingest,
        "github_identity_search": lambda input_data: _run_technical_tool("github_identity_search", input_data),
        "gitlab_identity_search": lambda input_data: _run_technical_tool("gitlab_identity_search", input_data),
        "personal_site_search": lambda input_data: _run_technical_tool("personal_site_search", input_data),
        "npm_author_search": lambda input_data: _run_technical_tool("npm_author_search", input_data),
        "package_registry_search": lambda input_data: _run_technical_tool("package_registry_search", input_data),
        "crates_author_search": lambda input_data: _run_technical_tool("crates_author_search", input_data),
        "wayback_fetch_url": lambda input_data: _run_technical_tool("wayback_fetch_url", input_data),
        "open_corporates_search": lambda input_data: _run_business_tool("open_corporates_search", input_data),
        "company_officer_search": lambda input_data: _run_business_tool("company_officer_search", input_data),
        "company_filing_search": lambda input_data: _run_business_tool("company_filing_search", input_data),
        "sec_person_search": lambda input_data: _run_business_tool("sec_person_search", input_data),
        "director_disclosure_search": lambda input_data: _run_business_tool("director_disclosure_search", input_data),
        "domain_whois_search": lambda input_data: _run_business_tool("domain_whois_search", input_data),
        "wayback_domain_timeline_search": lambda input_data: _run_archive_tool("wayback_domain_timeline_search", input_data),
        "historical_bio_diff": lambda input_data: _run_archive_tool("historical_bio_diff", input_data),
        "sanctions_watchlist_search": lambda input_data: _run_safety_tool("sanctions_watchlist_search", input_data),
        "alias_variant_generator": lambda input_data: _run_identity_tool("alias_variant_generator", input_data),
        "username_permutation_search": lambda input_data: _run_identity_tool("username_permutation_search", input_data),
        "cross_platform_profile_resolver": lambda input_data: _run_identity_tool("cross_platform_profile_resolver", input_data),
        "institution_directory_search": lambda input_data: _run_identity_tool("institution_directory_search", input_data),
        "email_pattern_inference": lambda input_data: _run_contact_tool("email_pattern_inference", input_data),
        "contact_page_extractor": lambda input_data: _run_contact_tool("contact_page_extractor", input_data),
        "reddit_user_search": lambda input_data: _run_social_tool("reddit_user_search", input_data),
        "mastodon_profile_search": lambda input_data: _run_social_tool("mastodon_profile_search", input_data),
        "substack_author_search": lambda input_data: _run_social_tool("substack_author_search", input_data),
        "medium_author_search": lambda input_data: _run_social_tool("medium_author_search", input_data),
        "coauthor_graph_search": lambda input_data: _run_relationship_tool("coauthor_graph_search", input_data),
        "org_staff_page_search": lambda input_data: _run_relationship_tool("org_staff_page_search", input_data),
        "board_member_overlap_search": lambda input_data: _run_relationship_tool("board_member_overlap_search", input_data),
        "shared_contact_pivot_search": lambda input_data: _run_relationship_tool("shared_contact_pivot_search", input_data),
        "orcid_search": lambda input_data: _run_academic_tool("orcid_search", input_data),
        "semantic_scholar_search": lambda input_data: _run_academic_tool("semantic_scholar_search", input_data),
        "dblp_author_search": lambda input_data: _run_academic_tool("dblp_author_search", input_data),
        "pubmed_author_search": lambda input_data: _run_academic_tool("pubmed_author_search", input_data),
        "grant_search_person": lambda input_data: _run_academic_tool("grant_search_person", input_data),
        # Temporarily disabled until PatentSearch API integration is implemented.
        # "patent_search_person": lambda input_data: _run_academic_tool("patent_search_person", input_data),
        "conference_profile_search": lambda input_data: _run_academic_tool("conference_profile_search", input_data),
        # Temporarily disabled until non-stub implementations exist.
        # "google_scholar_profile_search": lambda input_data: _run_academic_tool("google_scholar_profile_search", input_data),
        # "researchgate_profile_search": lambda input_data: _run_academic_tool("researchgate_profile_search", input_data),
        # "ssrn_author_search": lambda input_data: _run_academic_tool("ssrn_author_search", input_data),
    }

    handler = handlers.get(tool_name)
    if handler is None:
        _emit(False, error=f"Unsupported tool: {tool_name}")
        return

    try:
        result = handler(input_data)
        _emit(True, result=result)
    except subprocess.TimeoutExpired as exc:
        _emit(False, error=f"Tool timed out: {exc}")
    except Exception as exc:  # noqa: BLE001
        _emit(False, error=str(exc))


if __name__ == "__main__":
    main()
