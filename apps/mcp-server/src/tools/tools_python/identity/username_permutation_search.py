from __future__ import annotations

from typing import Any, Dict, List

from technical.common import http_request


PLATFORMS = {
    "github": "https://github.com/{username}",
    "gitlab": "https://gitlab.com/{username}",
    "reddit": "https://www.reddit.com/user/{username}",
}


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    username = str(input_data.get("username") or "").strip().lstrip("@")
    if not username:
        raise RuntimeError("Missing required input: username")
    hits: List[Dict[str, Any]] = []
    for platform, template in PLATFORMS.items():
        url = template.format(username=username)
        status, _, _, final_url = http_request(url, timeout=15)
        if status == 200:
            hits.append({"platform": platform, "url": final_url, "status": status})
    return {"tool": "username_permutation_search", "platform_hits": hits}
