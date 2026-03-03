from __future__ import annotations

from typing import Any, Dict, List

from archive.common import fetch_text
from technical.common import build_evidence, clean_text, http_json_or_list_request


WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    target_url = str(input_data.get("url") or input_data.get("profile_url") or "").strip()
    if not target_url.startswith(("http://", "https://")):
        raise RuntimeError("Missing required input: url")

    max_results = max(1, min(int(input_data.get("max_results", 5)), 10))
    raw = http_json_or_list_request(
        WAYBACK_CDX_URL,
        params={
            "url": target_url,
            "output": "json",
            "fl": "timestamp,original,statuscode,mimetype,digest",
            "filter": "statuscode:200",
            "collapse": "digest",
            "limit": str(max_results),
        },
        timeout=20,
    )

    rows = raw if isinstance(raw, list) else []
    snapshots: List[Dict[str, Any]] = []
    for row in rows[1:]:
        if not isinstance(row, list) or len(row) < 5:
            continue
        timestamp, original, statuscode, mimetype, digest = [str(item or "").strip() for item in row[:5]]
        if not timestamp or not original:
            continue
        archived_url = f"https://web.archive.org/web/{timestamp}/{original}"
        snapshots.append(
            {
                "timestamp": timestamp,
                "original_url": original,
                "archived_url": archived_url,
                "status_code": statuscode,
                "mime_type": mimetype,
                "digest": digest,
            }
        )

    latest_snapshot = snapshots[0] if snapshots else {}
    earliest_snapshot = snapshots[-1] if snapshots else {}
    latest_text = ""
    earliest_text = ""
    if latest_snapshot.get("archived_url"):
        try:
            latest_text, _ = fetch_text(str(latest_snapshot["archived_url"]), timeout=20, max_len=5000)
        except Exception:
            latest_text = ""
    if earliest_snapshot.get("archived_url") and earliest_snapshot != latest_snapshot:
        try:
            earliest_text, _ = fetch_text(str(earliest_snapshot["archived_url"]), timeout=20, max_len=5000)
        except Exception:
            earliest_text = ""
    return {
        "tool": "wayback_fetch_url",
        "original_url": target_url,
        "archived_url": latest_snapshot.get("archived_url"),
        "timestamp": latest_snapshot.get("timestamp"),
        "extracted_text": latest_text,
        "first_archived_at": earliest_snapshot.get("timestamp"),
        "last_archived_at": latest_snapshot.get("timestamp"),
        "earliest_archived_url": earliest_snapshot.get("archived_url"),
        "earliest_extracted_text": earliest_text,
        "latest_archived_url": latest_snapshot.get("archived_url"),
        "latest_extracted_text": latest_text,
        "snapshots": snapshots,
        "evidence": [
            build_evidence(
                str(item.get("archived_url") or ""),
                clean_text(f"Wayback snapshot {item.get('timestamp')} for {item.get('original_url')}"),
                ["url", "timestamp"],
            )
            for item in snapshots[:5]
            if isinstance(item, dict)
        ],
    }
