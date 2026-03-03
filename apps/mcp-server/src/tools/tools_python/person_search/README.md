# Person search workflow

Finds web pages that mention a person’s name, then fetches each page and extracts the main text.

## Setup

```bash
pip install -r requirements.txt
```

## Usage

```bash
# Search for a person and show related pages + extracted content
python -m person_search "Person Name"

# Limit to 5 results, 1s delay between fetches (default)
python -m person_search "Barbara Liskov" -n 5

# Output as JSON
python -m person_search "Barbara Liskov" -n 3 --json
```

## MCP usage

Use the wrapper script:

`apps/mcp-server/src/tools/tools_python/unified_research_mcp.py`

Example `MCP_PYTHON_TOOLS` entry:

```bash
MCP_PYTHON_TOOLS='[
  {
    "name": "person_search",
    "description": "Search for pages about a person and extract main text",
    "scriptPath": "apps/mcp-server/src/tools/tools_python/unified_research_mcp.py",
    "timeoutMs": 120000
  }
]'
```

Expected MCP input:

```json
{
  "runId": "uuid",
  "name": "Barbara Liskov",
  "max_results": 5,
  "delay": 1.0,
  "request_timeout": 10.0,
  "download_dir": null,
  "seen_urls": null,
  "no_cache": false
}
```

## How it works

1. **Search** – Uses [ddgs](https://pypi.org/project/ddgs/) to get URLs for the given name.
2. **Fetch** – Downloads each page with `httpx` (with a short delay between requests).
3. **Extract** – Uses [trafilatura](https://trafilatura.readthedocs.io/) to get the main article text from the HTML.

Some sites (e.g. Wikipedia) may return 403 for simple clients; others will return full extracted content.
