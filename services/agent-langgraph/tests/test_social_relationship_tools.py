from __future__ import annotations

from relationship.board_member_overlap_search import run as run_board_member_overlap_search
from relationship.coauthor_graph_search import run as run_coauthor_graph_search
from relationship.org_staff_page_search import run as run_org_staff_page_search
from relationship.shared_contact_pivot_search import run as run_shared_contact_pivot_search
from social.mastodon_profile_search import run as run_mastodon_profile_search
from social.medium_author_search import run as run_medium_author_search
from social.reddit_user_search import run as run_reddit_user_search
from social.substack_author_search import run as run_substack_author_search


def test_reddit_user_search_parses_profile(monkeypatch) -> None:
    monkeypatch.setattr(
        "social.reddit_user_search.http_json_request",
        lambda *args, **kwargs: {
            "data": {
                "created_utc": 1_700_000_000,
                "link_karma": 123,
                "comment_karma": 456,
                "subreddit": {
                    "display_name_prefixed": "u/ada",
                    "public_description": "Engineer and writer",
                },
            }
        },
    )
    result = run_reddit_user_search({"username": "ada"})
    assert result["username"] == "ada"
    assert result["subreddits"] == ["u/ada"]


def test_mastodon_profile_search_parses_lookup(monkeypatch) -> None:
    monkeypatch.setattr(
        "social.mastodon_profile_search.http_json_request",
        lambda *args, **kwargs: {
            "username": "ada",
            "display_name": "Ada Lovelace",
            "note": "<p>Engineer</p>",
            "followers_count": 12,
            "url": "https://mastodon.social/@ada",
        },
    )
    result = run_mastodon_profile_search({"profile_url": "https://mastodon.social/@ada"})
    assert result["instance"] == "mastodon.social"
    assert result["username"] == "ada"
    assert result["bio"] == "Engineer"


def test_substack_author_search_extracts_articles(monkeypatch) -> None:
    def fake_http_request(url: str, timeout: int = 20):
        if url.endswith("/about"):
            body = '<html><body>By Ada Lovelace Contact ada@example.com <a href="https://github.com/ada">GitHub</a></body></html>'
        else:
            body = '<html><body><a href="https://ada.substack.com/p/first-post">post</a></body></html>'
        return 200, {}, body, url

    monkeypatch.setattr("social.substack_author_search.http_request", fake_http_request)
    result = run_substack_author_search({"subdomain": "ada"})
    assert result["subdomain"] == "ada"
    assert result["emails"] == ["ada@example.com"]
    assert result["articles"] == ["https://ada.substack.com/p/first-post"]


def test_medium_author_search_extracts_articles(monkeypatch) -> None:
    monkeypatch.setattr(
        "social.medium_author_search.http_request",
        lambda *args, **kwargs: (
            200,
            {},
            '<html><body><a href="https://medium.com/@ada/first-post">first</a></body></html>',
            "https://medium.com/@ada",
        ),
    )
    result = run_medium_author_search({"username": "ada"})
    assert result["username"] == "ada"
    assert result["articles"] == ["https://medium.com/@ada/first-post"]


def test_coauthor_graph_search_extracts_coauthors() -> None:
    result = run_coauthor_graph_search(
        {
            "person_name": "Ada Lovelace",
            "publication_data": [
                {
                    "title": "Paper",
                    "authors": ["Ada Lovelace", "Grace Hopper", "Alan Turing"],
                    "venue": "NeurIPS",
                }
            ],
        }
    )
    assert result["coauthors"][0]["name"] in {"Grace Hopper", "Alan Turing"}
    assert result["shared_venues"][0]["venue"] == "NeurIPS"
    assert result["collaborationGraph"]["nodes"]
    assert result["collaborationGraph"]["edges"]
    assert result["clusters"][0]["representative_works"] == ["Paper"]


def test_org_staff_page_search_extracts_staff(monkeypatch) -> None:
    monkeypatch.setattr(
        "relationship.org_staff_page_search.fetch_text",
        lambda *args, **kwargs: ("Ada Lovelace - CTO. Grace Hopper - COO.", args[0]),
    )
    result = run_org_staff_page_search({"org_url": "https://example.com"})
    assert len(result["staff"]) >= 2


def test_board_member_overlap_search_finds_shared_people() -> None:
    result = run_board_member_overlap_search(
        {
            "roles": [
                {"name": "Ada Lovelace", "company_name": "Acme", "role": "Director"},
                {"name": "Ada Lovelace", "company_name": "Beta", "role": "Advisor"},
                {"name": "Grace Hopper", "company_name": "Acme", "role": "Director"},
            ]
        }
    )
    assert result["overlaps"][0]["name"] == "Ada Lovelace"


def test_shared_contact_pivot_search_finds_shared_domains() -> None:
    result = run_shared_contact_pivot_search(
        {
            "emails": ["ada@example.com", "grace@example.com"],
            "organizations": ["Example Org", "Example Org"],
        }
    )
    assert result["shared_domains"][0]["domain"] == "example.com"
    assert result["shared_organizations"][0]["organization"] == "example org"
