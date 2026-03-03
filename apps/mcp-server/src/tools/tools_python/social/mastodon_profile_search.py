from __future__ import annotations

import re
from typing import Any, Dict, Tuple
from urllib.parse import urlparse

from technical.common import clean_text, http_json_request


def _strip_html(value: Any) -> str:
    text = re.sub(r"(?is)<[^>]+>", " ", str(value or ""))
    return clean_text(text, max_len=600)


def _parse_profile_input(input_data: Dict[str, Any]) -> Tuple[str, str, str]:
    profile_url = str(input_data.get("profile_url") or input_data.get("url") or "").strip()
    acct = str(input_data.get("acct") or "").strip().lstrip("@")
    username = str(input_data.get("username") or "").strip().lstrip("@")
    instance = str(input_data.get("instance") or "").strip().lower()

    if profile_url.startswith(("http://", "https://")):
        parsed = urlparse(profile_url)
        instance = (parsed.hostname or "").lower()
        path = parsed.path.strip("/")
        if path.startswith("@"):
            username = path[1:].split("/", 1)[0]

    if acct and "@" in acct and not username:
        username = acct.split("@", 1)[0]
    if acct and "@" in acct and not instance:
        instance = acct.split("@", 1)[1].lower()
    if username and instance and not acct:
        acct = f"{username}@{instance}"
    if not acct and username:
        acct = username
    return username, instance, acct


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    username, instance, acct = _parse_profile_input(input_data)
    if not instance or not acct:
        raise RuntimeError("Missing required input: instance plus username/acct/profile_url")

    payload = http_json_request(
        f"https://{instance}/api/v1/accounts/lookup",
        params={"acct": acct},
        timeout=20,
    )
    if not isinstance(payload, dict):
        payload = {}

    profile_url = str(payload.get("url") or input_data.get("profile_url") or "").strip()
    resolved_username = str(payload.get("username") or username or "").strip()
    return {
        "tool": "mastodon_profile_search",
        "instance": instance,
        "username": resolved_username,
        "display_name": clean_text(payload.get("display_name") or "", max_len=200),
        "bio": _strip_html(payload.get("note") or ""),
        "followers_count": payload.get("followers_count"),
        "profile_url": profile_url or f"https://{instance}/@{resolved_username}",
        "confidence": 0.75 if resolved_username else 0.0,
    }

