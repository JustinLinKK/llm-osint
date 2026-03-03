from __future__ import annotations

from typing import Any, Dict, List


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    full_name = str(input_data.get("person_name") or input_data.get("name") or "").strip()
    if not full_name:
        raise RuntimeError("Missing required input: person_name")
    parts = [part for part in full_name.replace(",", " ").split() if part]
    if len(parts) < 2:
        return {"tool": "alias_variant_generator", "variants": [full_name]}
    first = parts[0]
    last = parts[-1]
    middle = parts[1:-1]
    variants: List[str] = [
        f"{first} {last}",
        f"{last}, {first}",
        f"{first[0]}{last}".lower(),
        f"{first}.{last}".lower(),
        f"{first}_{last}".lower(),
        f"{first}{last}".lower(),
    ]
    if middle:
        variants.append(f"{first} {' '.join(middle)} {last}")
        variants.append(f"{first} {middle[0][0]} {last}")
    return {"tool": "alias_variant_generator", "variants": list(dict.fromkeys(variants))}
