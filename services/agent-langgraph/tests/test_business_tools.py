from __future__ import annotations

from business.company_officer_search import run as run_company_officer_search
from business.domain_whois_search import run as run_domain_whois_search
from business.open_corporates_search import run as run_open_corporates_search
from business.sec_person_search import run as run_sec_person_search


def test_open_corporates_search_resolves_company(monkeypatch) -> None:
    responses = [
        {
            "results": {
                "companies": [
                    {
                        "company": {
                            "name": "Acme Inc.",
                            "company_number": "123",
                            "jurisdiction_code": "us_ca",
                        }
                    }
                ]
            }
        },
        {
            "results": {
                "company": {
                    "name": "Acme Inc.",
                    "company_number": "123",
                    "jurisdiction_code": "us_ca",
                    "incorporation_date": "2020-01-01",
                    "current_status": "Active",
                    "registered_address_in_full": "123 Main St, San Francisco, CA",
                }
            }
        },
        {
            "results": {
                "officers": [
                    {
                        "officer": {
                            "name": "Ada Lovelace",
                            "position": "Director",
                            "start_date": "2021-01-01",
                            "end_date": None,
                        }
                    }
                ]
            }
        },
    ]
    monkeypatch.setattr(
        "business.open_corporates_search.http_json_request",
        lambda *args, **kwargs: responses.pop(0),
    )

    result = run_open_corporates_search({"company_name": "Acme Inc"})

    assert result["company_number"] == "123"
    assert result["jurisdiction"] == "us_ca"
    assert result["officers"][0]["name"] == "Ada Lovelace"
    assert result["confidence"] == 0.7


def test_company_officer_search_returns_roles(monkeypatch) -> None:
    monkeypatch.setattr(
        "business.company_officer_search.http_json_request",
        lambda *args, **kwargs: {
            "results": {
                "officers": [
                    {
                        "officer": {
                            "name": "Ada Lovelace",
                            "position": "Director",
                            "start_date": "2021-01-01",
                            "company": {
                                "name": "Acme Inc.",
                                "company_number": "123",
                                "jurisdiction_code": "us_ca",
                                "opencorporates_url": "https://opencorporates.com/companies/us_ca/123",
                            },
                        }
                    }
                ]
            }
        },
    )

    result = run_company_officer_search({"person_name": "Ada Lovelace"})

    assert result["roles"][0]["company_name"] == "Acme Inc."
    assert result["roles"][0]["role"] == "Director"
    assert result["confidence"] >= 0.4


def test_domain_whois_search_parses_rdap(monkeypatch) -> None:
    monkeypatch.setattr(
        "business.domain_whois_search.http_json_request",
        lambda *args, **kwargs: {
            "events": [{"eventAction": "registration", "eventDate": "2020-01-01T00:00:00Z"}],
            "entities": [
                {
                    "roles": ["registrant"],
                    "vcardArray": ["vcard", [["org", {}, "text", "Acme Inc."]]],
                },
                {
                    "roles": ["registrar"],
                    "handle": "RegistrarCo",
                },
            ],
            "nameservers": [{"ldhName": "ns1.example.com"}, {"ldhName": "ns2.example.com"}],
        },
    )

    result = run_domain_whois_search({"domain": "acme.com"})

    assert result["domain"] == "acme.com"
    assert result["registrant_org"] == "Acme Inc."
    assert result["registrar"] == "RegistrarCo"
    assert result["name_servers"] == ["ns1.example.com", "ns2.example.com"]


def test_sec_person_search_extracts_roles(monkeypatch) -> None:
    monkeypatch.setattr(
        "business.sec_person_search.http_json_request",
        lambda url, params=None, headers=None, timeout=20: {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "display_names": ["Acme Inc."],
                            "form": "DEF 14A",
                            "file_date": "2024-04-01",
                            "adsh": "00001234-24-000001",
                        }
                    },
                    {
                        "_source": {
                            "display_names": ["Acme Inc."],
                            "form": "4",
                            "file_date": "2024-04-02",
                            "adsh": "00001234-24-000002",
                        }
                    },
                ]
            }
        },
    )

    result = run_sec_person_search({"person_name": "Ada Lovelace"})

    assert result["companies"] == ["Acme Inc."]
    assert result["roles"][0]["form"] == "DEF 14A"
    assert result["insider_filings"][0]["form"] == "4"
