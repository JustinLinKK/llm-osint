from __future__ import annotations

from archive.historical_bio_diff import run as run_historical_bio_diff
from archive.wayback_domain_timeline_search import run as run_wayback_domain_timeline
from contact.contact_page_extractor import run as run_contact_page_extractor
from contact.email_pattern_inference import run as run_email_pattern_inference
from identity.alias_variant_generator import run as run_alias_variant_generator
from identity.cross_platform_profile_resolver import run as run_cross_platform_profile_resolver
from identity.username_permutation_search import run as run_username_permutation_search
from safety.sanctions_watchlist_search import run as run_sanctions_watchlist_search


def test_wayback_domain_timeline_search_parses_snapshots(monkeypatch) -> None:
    monkeypatch.setattr(
        "archive.wayback_domain_timeline_search.http_json_or_list_request",
        lambda *args, **kwargs: [
            ["timestamp", "statuscode"],
            ["20240101000000", "200"],
            ["20230101000000", "200"],
        ],
    )
    result = run_wayback_domain_timeline({"domain": "ada.dev"})
    assert result["domain"] == "ada.dev"
    assert result["snapshot_count"] == 2


def test_historical_bio_diff_detects_changes() -> None:
    result = run_historical_bio_diff(
        {
            "earliest_text": "Software Engineer at Acme. Based in London.",
            "latest_text": "Principal Engineer at ExampleCorp. Based in New York.",
            "earliest_timestamp": "2020",
            "latest_timestamp": "2025",
        }
    )
    fields = {item["field"] for item in result["changes"]}
    assert "employment" in fields or "title" in fields or "location" in fields


def test_sanctions_watchlist_search_exact_match(monkeypatch) -> None:
    csv_body = "1,ADA LOVELACE,,TESTPROGRAM,,,,,,,,UK\n2,OTHER NAME,,OTHER,,,,,,,,US\n"
    monkeypatch.setattr(
        "safety.sanctions_watchlist_search.http_request",
        lambda *args, **kwargs: (200, {}, csv_body, "https://home.treasury.gov/system/files/126/sdn.csv"),
    )
    result = run_sanctions_watchlist_search({"person_name": "Ada Lovelace"})
    assert len(result["matches"]) == 1
    assert result["matches"][0]["source"] == "OFAC"


def test_alias_variant_generator_returns_variants() -> None:
    result = run_alias_variant_generator({"person_name": "Ada Lovelace"})
    assert "Ada Lovelace" in result["variants"]
    assert "Lovelace, Ada" in result["variants"]


def test_username_permutation_search_returns_hits(monkeypatch) -> None:
    monkeypatch.setattr(
        "identity.username_permutation_search.http_request",
        lambda url, timeout=15: (200 if "github.com" in url else 404, {}, "", url),
    )
    result = run_username_permutation_search({"username": "ada"})
    assert result["platform_hits"][0]["platform"] == "github"


def test_cross_platform_profile_resolver_scores_matches() -> None:
    result = run_cross_platform_profile_resolver(
        {
            "profiles": [
                {"platform": "github", "username": "ada", "bio": "engineer", "site": "https://ada.dev"},
                {"platform": "gitlab", "username": "ada", "bio": "engineer", "site": "https://ada.dev"},
            ]
        }
    )
    assert result["confidence"] == 1.0


def test_cross_platform_profile_resolver_merges_aliases_with_independent_signals() -> None:
    result = run_cross_platform_profile_resolver(
        {
            "profiles": [
                {
                    "platform": "scholar",
                    "name": "Xinyu Pi",
                    "institution": "University of California San Diego",
                    "advisor": "Prof. Jane Doe",
                    "publications": [{"title": "Neural Audio Systems"}],
                },
                {
                    "platform": "linkedin",
                    "name": "Frederick Pi",
                    "institution": "University of California San Diego",
                    "advisor": "Prof. Jane Doe",
                    "publications": [{"title": "Neural Audio Systems"}],
                },
            ]
        }
    )
    assert result["confidence"] >= 0.8
    assert result["canonical_identity"]["aliases"]
    evidence_types = {item["type"] for item in result["disambiguation_evidence"]}
    assert {"affiliation", "advisor", "publication"}.issubset(evidence_types)


def test_email_pattern_inference_builds_patterns() -> None:
    result = run_email_pattern_inference({"domain": "example.edu", "person_name": "Ada Lovelace"})
    assert "ada.lovelace@example.edu" in result["patterns"]


def test_contact_page_extractor_collects_emails(monkeypatch) -> None:
    monkeypatch.setattr(
        "contact.contact_page_extractor.fetch_text",
        lambda url, timeout=15, max_len=4000: ("Contact us at ada@example.edu", url),
    )
    result = run_contact_page_extractor({"site_url": "https://example.edu"})
    assert "ada@example.edu" in result["emails"]
