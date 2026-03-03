#!/usr/bin/env python3
"""MCP bridge wrapper for person_search workflow."""

from __future__ import annotations

import argparse
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path

# Make local `person_search` package importable
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

from person_search.workflow import run_workflow  # noqa: E402


def read_stdin_payload() -> dict | None:
    if sys.stdin.isatty():
        return None

    raw = sys.stdin.read()
    if not raw or not raw.strip():
        return None

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None

    return payload if isinstance(payload, dict) else None


def emit_mcp_result(ok: bool, result: dict | list | None = None, error: str | None = None) -> None:
    payload: dict[str, object] = {"ok": ok}
    if ok:
        payload["result"] = result
    else:
        payload["error"] = error or "Unknown error"

    sys.stdout.write(json.dumps(payload, ensure_ascii=True))
    sys.stdout.flush()


def _serialize_results(results):
    return [
        {
            "url": r.url,
            "title": r.title,
            "snippet": r.snippet,
            "main_text": r.main_text,
            "error": r.error,
            "html_path": r.html_path,
            "skipped": r.skipped,
        }
        for r in results
    ]


def mcp_main(payload: dict) -> None:
    input_data = payload.get("input", {}) if isinstance(payload, dict) else {}
    if not isinstance(input_data, dict):
        emit_mcp_result(False, error="Invalid input payload")
        return

    name = str(input_data.get("name") or input_data.get("query") or "").strip()
    if not name:
        emit_mcp_result(False, error="Missing required input: name (or query)")
        return

    max_results = int(input_data.get("max_results", 5))
    delay = float(input_data.get("delay", 1.0))
    request_timeout = float(input_data.get("request_timeout", 10.0))

    download_dir = input_data.get("download_dir")
    seen_urls_file = input_data.get("seen_urls")
    no_cache = bool(input_data.get("no_cache", False))

    try:
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

        response = {
            "name": name,
            "count": len(results),
            "results": _serialize_results(results),
        }
        emit_mcp_result(True, result=response)
    except Exception as exc:
        emit_mcp_result(False, error=str(exc))


def cli_main() -> None:
    parser = argparse.ArgumentParser(description="MCP wrapper for person_search")
    parser.add_argument("name", help="Person name to search for")
    parser.add_argument("--max-results", type=int, default=5)
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--request-timeout", type=float, default=10.0)
    parser.add_argument("--download-dir", default=None)
    parser.add_argument("--seen-urls", default=None)
    parser.add_argument("--no-cache", action="store_true")
    args = parser.parse_args()

    results = run_workflow(
        args.name,
        max_search_results=args.max_results,
        fetch_delay_seconds=args.delay,
        request_timeout=args.request_timeout,
        download_dir=args.download_dir,
        seen_urls_file=args.seen_urls,
        use_seen_cache=not args.no_cache,
    )

    output = {
        "name": args.name,
        "count": len(results),
        "results": _serialize_results(results),
    }
    json.dump(output, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    payload = read_stdin_payload()
    if payload and "input" in payload:
        mcp_main(payload)
    else:
        cli_main()
