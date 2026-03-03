from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from technical.crates_author_search import run as run_crates_search
from technical.gitlab_identity_search import run as run_gitlab_search
from technical.github_identity_search import run as run_github_search
from technical.npm_author_search import run as run_npm_search
from technical.package_registry_search import run as run_package_registry_search
from technical.personal_site_search import run as run_personal_site_search
from technical.wayback_fetch_url import run as run_wayback_fetch


def test_github_identity_search_import_bootstraps_tools_root() -> None:
    module_path = (
        Path(__file__).resolve().parents[3]
        / "apps"
        / "mcp-server"
        / "src"
        / "tools"
        / "tools_python"
        / "technical"
        / "github_identity_search.py"
    )
    code = f"""
import importlib.util
import sys

sys.path = [p for p in sys.path if "tools_python" not in p and "agent-langgraph/src" not in p]
spec = importlib.util.spec_from_file_location("github_identity_search_import_test", r"{module_path}")
module = importlib.util.module_from_spec(spec)
assert spec is not None and spec.loader is not None
spec.loader.exec_module(module)
print(callable(getattr(module, "run", None)))
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "True"


def test_github_identity_search_direct_username(monkeypatch) -> None:
    monkeypatch.setattr(
        "technical.github_identity_search._fetch_user",
        lambda username: {
            "login": username,
            "id": 42,
            "name": "Ada Lovelace",
            "bio": "Analytical engine pioneer",
            "blog": "https://ada.dev",
            "email": "ada@example.com",
            "company": "Analytical Engines Inc",
            "location": "London, UK",
            "followers": 1200,
            "public_repos": 2,
            "created_at": "2020-01-01T00:00:00Z",
            "updated_at": "2025-01-01T00:00:00Z",
            "html_url": f"https://github.com/{username}",
        },
    )
    monkeypatch.setattr(
        "technical.github_identity_search._fetch_orgs_list",
        lambda username: [
            {"login": "analytical-engines", "html_url": "https://github.com/analytical-engines"}
        ],
    )
    monkeypatch.setattr(
        "technical.github_identity_search._fetch_repos",
        lambda username: [
            {
                "full_name": f"{username}/engine",
                "html_url": f"https://github.com/{username}/engine",
                "language": "Python",
                "description": "Engine code",
                "stargazers_count": 10,
                "fork": False,
                "pushed_at": "2025-02-01T00:00:00Z",
            },
            {
                "full_name": f"{username}/notes",
                "html_url": f"https://github.com/{username}/notes",
                "language": "Python",
                "description": "Notes",
                "stargazers_count": 5,
                "fork": False,
                "pushed_at": "2025-01-01T00:00:00Z",
            },
        ],
    )

    result = run_github_search({"username": "ada"})

    assert result["stable_id"] == "github:42"
    assert result["profile_url"] == "https://github.com/ada"
    assert result["username"] == "ada"
    assert result["repo_count"] == 2
    assert result["top_languages"] == ["Python"]
    assert result["organizations"][0]["name"] == "analytical-engines"
    assert any(item["value"] == "ada@example.com" for item in result["contact_signals"])


def test_personal_site_search_extracts_links_and_contact(monkeypatch) -> None:
    html = """
    <html>
      <head>
        <title>Ada Lovelace</title>
        <meta name="generator" content="WordPress 6.0" />
      </head>
      <body>
        Contact: ada@example.com
        <a href="https://github.com/ada">GitHub</a>
        <a href="https://www.linkedin.com/in/ada-lovelace">LinkedIn</a>
      </body>
    </html>
    """

    monkeypatch.setattr(
        "technical.personal_site_search.http_request",
        lambda url, timeout=15: (
            200,
            {"content-type": "text/html; charset=utf-8", "server": "nginx"},
            html,
            "https://ada.dev",
        ),
    )

    result = run_personal_site_search({"domain": "ada.dev"})

    assert result["stable_id"] == "site:ada.dev"
    assert result["canonical_url"] == "https://ada.dev"
    assert result["site_title"] == "Ada Lovelace"
    assert "WordPress 6.0" in result["detected_technologies"]
    assert any(item["value"] == "ada@example.com" for item in result["contact_signals"])
    assert any(item["url"] == "https://github.com/ada" for item in result["external_links"])


def test_gitlab_identity_search_direct_username(monkeypatch) -> None:
    monkeypatch.setattr(
        "technical.gitlab_identity_search.http_json_or_list_request",
        lambda url, params=None, timeout=20: (
            [
                {
                    "id": 7,
                    "username": "ada",
                    "name": "Ada Lovelace",
                    "web_url": "https://gitlab.com/ada",
                    "bio": "Computing pioneer",
                    "location": "London",
                    "created_at": "2020-01-01T00:00:00Z",
                }
            ]
            if "users/7/projects" not in url
            else [
                {
                    "path_with_namespace": "ada/engine",
                    "web_url": "https://gitlab.com/ada/engine",
                    "language": "Python",
                    "star_count": 3,
                    "last_activity_at": "2025-02-01T00:00:00Z",
                    "namespace": {"full_path": "ada"},
                }
            ]
        ),
    )

    result = run_gitlab_search({"username": "ada"})

    assert result["stable_id"] == "gitlab:7"
    assert result["profile_url"] == "https://gitlab.com/ada"
    assert result["repo_count"] == 1
    assert result["top_languages"] == ["Python"]


def test_npm_author_search_extracts_repo_urls(monkeypatch) -> None:
    monkeypatch.setattr(
        "technical.npm_author_search.http_json_request",
        lambda url, params=None, timeout=20: {
            "objects": [
                {
                    "package": {
                        "name": "@acme/widget",
                        "version": "1.2.3",
                        "date": "2025-01-01T00:00:00.000Z",
                        "links": {
                            "npm": "https://www.npmjs.com/package/@acme/widget",
                            "repository": "https://github.com/acme/widget",
                        },
                        "maintainers": [{"username": "ada", "email": "ada@example.com"}],
                        "license": "MIT",
                        "description": "Widget package",
                    },
                    "score": {"final": 0.8},
                }
            ]
        },
    )

    result = run_npm_search({"username": "ada"})

    assert result["publications"][0]["name"] == "@acme/widget"
    assert result["repositories"][0]["url"] == "https://github.com/acme/widget"
    assert any(item["name"] == "@acme" for item in result["organizations"])


def test_crates_author_search_extracts_crate_repo(monkeypatch) -> None:
    responses = [
        {"users": [{"id": 10, "login": "ada"}]},
        {
            "crates": [
                {
                    "id": "engine",
                    "newest_version": "0.4.0",
                    "updated_at": "2025-02-01T00:00:00Z",
                    "downloads": 1234,
                    "repository": "https://github.com/ada/engine",
                    "description": "Rust engine",
                }
            ]
        },
    ]
    monkeypatch.setattr(
        "technical.crates_author_search.http_json_request",
        lambda url, params=None, timeout=20: responses.pop(0),
    )

    result = run_crates_search({"username": "ada"})

    assert result["stable_id"] == "crates:10"
    assert result["publications"][0]["name"] == "engine"
    assert result["repositories"][0]["language"] == "Rust"


def test_wayback_fetch_url_parses_snapshots(monkeypatch) -> None:
    monkeypatch.setattr(
        "technical.wayback_fetch_url.http_json_or_list_request",
        lambda url, params=None, timeout=20: [
            ["timestamp", "original", "statuscode", "mimetype", "digest"],
            ["20240101000000", "https://ada.dev", "200", "text/html", "abc"],
            ["20230101000000", "https://ada.dev", "200", "text/html", "def"],
        ],
    )
    monkeypatch.setattr(
        "technical.wayback_fetch_url.fetch_text",
        lambda url, timeout=20, max_len=5000: ("Ada bio text", url),
    )

    result = run_wayback_fetch({"url": "https://ada.dev"})

    assert result["archived_url"] == "https://web.archive.org/web/20240101000000/https://ada.dev"
    assert result["first_archived_at"] == "20230101000000"
    assert len(result["snapshots"]) == 2
    assert result["extracted_text"] == "Ada bio text"


def test_package_registry_search_aggregates_results(monkeypatch) -> None:
    monkeypatch.setattr(
        "technical.package_registry_search.run_npm_author_search",
        lambda input_data: {
            "tool": "npm_author_search",
            "stable_id": "npm:ada",
            "platform": "npm",
            "profile_url": "",
            "created_at": None,
            "last_active": None,
            "organizations": [{"name": "@acme", "url": "https://www.npmjs.com/org/acme", "relation": "owns_namespace"}],
            "repositories": [{"name": "@acme/widget", "url": "https://github.com/acme/widget"}],
            "publications": [{"name": "@acme/widget", "url": "https://www.npmjs.com/package/@acme/widget"}],
            "contact_signals": [{"type": "npm_username", "value": "ada", "source": "npm"}],
            "external_links": [{"type": "npm_package", "url": "https://www.npmjs.com/package/@acme/widget"}],
            "evidence": [{"url": "https://www.npmjs.com/package/@acme/widget"}],
            "confidence": 0.8,
            "match_features": {},
        },
    )
    monkeypatch.setattr(
        "technical.package_registry_search.run_crates_author_search",
        lambda input_data: {
            "tool": "crates_author_search",
            "stable_id": "crates:ada",
            "platform": "crates.io",
            "profile_url": "",
            "created_at": None,
            "last_active": None,
            "organizations": [],
            "repositories": [{"name": "engine", "url": "https://github.com/ada/engine"}],
            "publications": [{"name": "engine", "url": "https://crates.io/crates/engine"}],
            "contact_signals": [{"type": "crates_username", "value": "ada", "source": "crates"}],
            "external_links": [{"type": "crates_package", "url": "https://crates.io/crates/engine"}],
            "evidence": [{"url": "https://crates.io/crates/engine"}],
            "confidence": 0.76,
            "match_features": {},
        },
    )

    result = run_package_registry_search({"username": "ada"})

    assert result["tool"] == "package_registry_search"
    assert len(result["publications"]) == 2
    assert len(result["repositories"]) == 2
    assert result["match_features"]["registries_queried"] == ["npm", "crates.io"]
