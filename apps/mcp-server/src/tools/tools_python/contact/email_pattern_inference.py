from __future__ import annotations

from typing import Any, Dict, List


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    domain = str(input_data.get("domain") or "").strip().lower()
    person_name = str(input_data.get("person_name") or input_data.get("name") or "").strip()
    if not domain or not person_name:
        raise RuntimeError("Missing required input: domain and person_name")
    parts = [part for part in person_name.lower().split() if part]
    if len(parts) < 2:
        raise RuntimeError("email_pattern_inference requires first and last name")
    first, last = parts[0], parts[-1]
    patterns: List[str] = [
        f"{first}.{last}@{domain}",
        f"{first[0]}.{last}@{domain}",
        f"{first}@{domain}",
        f"{first}{last}@{domain}",
        f"{first[0]}{last}@{domain}",
    ]
    return {"tool": "email_pattern_inference", "patterns": list(dict.fromkeys(patterns))}
