"""
CLI: python -m person_search "Person Name"
"""
import argparse
import json
import sys

from .workflow import run_workflow


def main() -> None:
    parser = argparse.ArgumentParser(description="Search for pages about a person and extract content.")
    parser.add_argument("name", help="Person's name to search for")
    parser.add_argument("-n", "--max-results", type=int, default=5, help="Max search results to fetch (default 5)")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between fetches in seconds (default 1)")
    parser.add_argument("-o", "--download-dir", metavar="DIR", help="Download raw HTML of each page into DIR")
    parser.add_argument("--seen-urls", metavar="FILE", default=None, help="File to record seen URLs (default: person_search_seen_urls.txt in cwd)")
    parser.add_argument("--no-cache", action="store_true", help="Do not skip URLs seen in previous runs")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    try:
        results = run_workflow(
            args.name,
            max_search_results=args.max_results,
            fetch_delay_seconds=args.delay,
            download_dir=args.download_dir,
            seen_urls_file=args.seen_urls,
            use_seen_cache=not args.no_cache,
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        out = [
            {
                "url": r.url,
                "title": r.title,
                "snippet": r.snippet,
                "main_text": r.main_text[:2000] if r.main_text else None,
                "error": r.error,
                "html_path": r.html_path,
                "skipped": r.skipped,
            }
            for r in results
        ]
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return

    print(f"Found {len(results)} page(s) for: {args.name}\n")
    for r in results:
        print(f"  {r.title}")
        print(f"  {r.url}")
        if r.skipped:
            print(f"  [skipped: already fetched]")
        if r.html_path:
            print(f"  HTML saved: {r.html_path}")
        if r.error and not r.skipped:
            print(f"  [fetch error: {r.error}]")
        elif r.main_text:
            preview = (r.main_text[:400] + "...") if len(r.main_text) > 400 else r.main_text
            print(f"  Content: {preview.strip()}")
        elif not r.skipped:
            print(f"  Snippet: {r.snippet[:200]}...")
        print()


if __name__ == "__main__":
    main()
