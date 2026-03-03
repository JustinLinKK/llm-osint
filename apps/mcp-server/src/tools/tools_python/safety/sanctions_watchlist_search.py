from __future__ import annotations

import csv
import io
from typing import Any, Dict, List

from technical.common import http_request


OFAC_SDN_CSV = "https://home.treasury.gov/system/files/126/sdn.csv"


def _normalize(value: str) -> str:
    return " ".join(str(value or "").upper().replace(",", " ").split())


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    person_name = str(input_data.get("person_name") or input_data.get("name") or "").strip()
    if not person_name:
        raise RuntimeError("Missing required input: person_name")
    normalized_query = _normalize(person_name)
    _, _, body, _ = http_request(OFAC_SDN_CSV, timeout=30)
    reader = csv.reader(io.StringIO(body))
    matches: List[Dict[str, Any]] = []
    for row in reader:
        if len(row) < 4:
            continue
        name = str(row[1] or "").strip()
        if _normalize(name) != normalized_query:
            continue
        matches.append(
            {
                "name": name,
                "program": str(row[3] or "").strip(),
                "country": str(row[11] or "").strip() if len(row) > 11 else "",
                "source": "OFAC",
            }
        )
    return {
        "tool": "sanctions_watchlist_search",
        "matches": matches,
        "confidence": 1.0 if matches else 0.0,
    }
