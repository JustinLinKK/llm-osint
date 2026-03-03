from __future__ import annotations

from typing import Any, Dict, List

from technical.common import http_json_or_list_request


WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    domain = str(input_data.get("domain") or "").strip().lower()
    if not domain:
        raise RuntimeError("Missing required input: domain")
    max_results = max(1, min(int(input_data.get("max_results", 20)), 100))
    raw = http_json_or_list_request(
        WAYBACK_CDX_URL,
        params={
            "url": domain,
            "output": "json",
            "fl": "timestamp,statuscode",
            "filter": "statuscode:200",
            "limit": str(max_results),
        },
        timeout=20,
    )
    rows = raw if isinstance(raw, list) else []
    snapshots: List[Dict[str, Any]] = []
    for row in rows[1:]:
        if not isinstance(row, list) or len(row) < 2:
            continue
        snapshots.append({"timestamp": str(row[0] or "").strip(), "status": int(row[1] or 0)})
    return {
        "tool": "wayback_domain_timeline_search",
        "domain": domain,
        "snapshots": snapshots,
        "snapshot_count": len(snapshots),
    }
