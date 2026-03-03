from __future__ import annotations

from typing import Any, Dict, List

from archive.common import extract_bio_fields, fetch_text


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    earliest_text = str(input_data.get("earliest_text") or "").strip()
    latest_text = str(input_data.get("latest_text") or "").strip()
    earliest_url = str(input_data.get("earliest_url") or "").strip()
    latest_url = str(input_data.get("latest_url") or "").strip()

    if not earliest_text and earliest_url:
        earliest_text, _ = fetch_text(earliest_url)
    if not latest_text and latest_url:
        latest_text, _ = fetch_text(latest_url)
    if not earliest_text and not latest_text:
        raise RuntimeError("Missing required input: earliest/latest text or URLs")

    earliest_fields = extract_bio_fields(earliest_text)
    latest_fields = extract_bio_fields(latest_text)
    changes: List[Dict[str, Any]] = []
    for field in ("employment", "title", "location"):
        old_value = earliest_fields.get(field, "")
        new_value = latest_fields.get(field, "")
        if old_value and new_value and old_value != new_value:
            changes.append(
                {
                    "field": field,
                    "old": old_value,
                    "new": new_value,
                    "timestamp_range": f"{input_data.get('earliest_timestamp') or ''}..{input_data.get('latest_timestamp') or ''}".strip("."),
                }
            )
    return {
        "tool": "historical_bio_diff",
        "changes": changes,
        "earliest_fields": earliest_fields,
        "latest_fields": latest_fields,
    }
