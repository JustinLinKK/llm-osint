from __future__ import annotations

from typing import Any, Dict, List

from technical.common import clean_text, http_json_request


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    username = str(input_data.get("username") or "").strip().lstrip("@")
    if not username:
        raise RuntimeError("Missing required input: username")

    payload = http_json_request(f"https://www.reddit.com/user/{username}/about.json", timeout=20)
    data = payload.get("data") if isinstance(payload, dict) else {}
    if not isinstance(data, dict):
        data = {}

    subreddit = data.get("subreddit") if isinstance(data.get("subreddit"), dict) else {}
    subreddit_names: List[str] = []
    for key in ("display_name_prefixed", "display_name"):
        value = subreddit.get(key)
        if isinstance(value, str) and value.strip():
            subreddit_names.append(value.strip())

    return {
        "tool": "reddit_user_search",
        "username": username,
        "created_utc": data.get("created_utc"),
        "link_karma": data.get("link_karma"),
        "comment_karma": data.get("comment_karma"),
        "subreddits": list(dict.fromkeys(subreddit_names)),
        "profile_url": f"https://www.reddit.com/user/{username}",
        "bio": clean_text(subreddit.get("public_description") or "", max_len=300),
        "confidence": 0.8 if data else 0.0,
    }

