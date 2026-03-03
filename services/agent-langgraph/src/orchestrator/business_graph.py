from __future__ import annotations

from typing import Any, Dict, List


def build_business_graph_entities(tool_name: str, arguments: Dict[str, Any], result: Dict[str, Any]) -> List[Dict[str, Any]]:
    if tool_name == "open_corporates_search":
        return _from_open_corporates(result)
    if tool_name == "company_officer_search":
        return _from_company_officer(arguments, result)
    if tool_name == "company_filing_search":
        return _from_company_filing(arguments, result)
    if tool_name == "sec_person_search":
        return _from_sec_person(arguments, result)
    if tool_name == "director_disclosure_search":
        return _from_director_disclosure(result)
    if tool_name == "domain_whois_search":
        return _from_domain_whois(result)
    return []


def _from_open_corporates(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    company_name = str(result.get("company_name") or "").strip()
    company_number = str(result.get("company_number") or "").strip()
    jurisdiction = str(result.get("jurisdiction") or "").strip().lower()
    if not company_name:
        return []
    entity_id = f"company:{jurisdiction}:{company_number}" if jurisdiction and company_number else f"company:{company_name.lower()}"
    properties = {
        "name": company_name,
        "company_number": company_number or None,
        "jurisdiction": jurisdiction or None,
        "incorporation_date": result.get("incorporation_date"),
        "status": result.get("status"),
        "registered_address": result.get("registered_address"),
        "source_url": result.get("source_url"),
    }
    return [{"entityType": "Company", "entityId": entity_id, "properties": properties, "relations": []}]


def _from_company_officer(arguments: Dict[str, Any], result: Dict[str, Any]) -> List[Dict[str, Any]]:
    person_name = str(arguments.get("person_name") or "").strip()
    if not person_name:
        return []
    relations = []
    for role in result.get("roles", [])[:20] if isinstance(result.get("roles"), list) else []:
        if not isinstance(role, dict):
            continue
        company_name = str(role.get("company_name") or "").strip()
        company_number = str(role.get("company_number") or "").strip()
        jurisdiction = str(role.get("jurisdiction") or "").strip().lower()
        if not company_name:
            continue
        relations.append(
            {
                "type": "OFFICER_OF",
                "targetType": "Company",
                "targetId": f"company:{jurisdiction}:{company_number}" if jurisdiction and company_number else f"company:{company_name.lower()}",
                "targetProperties": {
                    "name": company_name,
                    "company_number": company_number or None,
                    "jurisdiction": jurisdiction or None,
                    "role": role.get("role"),
                    "source_url": role.get("source_url"),
                },
            }
        )
    return [{"entityType": "Person", "entityId": f"person:{person_name.lower()}", "properties": {"name": person_name}, "relations": relations}]


def _from_company_filing(arguments: Dict[str, Any], result: Dict[str, Any]) -> List[Dict[str, Any]]:
    company_number = str(result.get("company_number") or arguments.get("company_number") or "").strip()
    if not company_number:
        return []
    entity_id = f"companyfilings:{company_number}"
    relations = []
    for filing in result.get("filings", [])[:20] if isinstance(result.get("filings"), list) else []:
        if not isinstance(filing, dict):
            continue
        document_url = str(filing.get("document_url") or "").strip()
        if not document_url:
            continue
        relations.append(
            {
                "type": "FILED",
                "targetType": "CorporateFiling",
                "targetId": document_url,
                "targetProperties": {
                    "filing_type": filing.get("filing_type"),
                    "filing_date": filing.get("filing_date"),
                    "description": filing.get("description"),
                    "document_url": document_url,
                },
            }
        )
    return [{"entityType": "Company", "entityId": entity_id, "properties": {"company_number": company_number}, "relations": relations}]


def _from_sec_person(arguments: Dict[str, Any], result: Dict[str, Any]) -> List[Dict[str, Any]]:
    person_name = str(arguments.get("person_name") or "").strip()
    if not person_name:
        return []
    relations = []
    for role in result.get("roles", [])[:20] if isinstance(result.get("roles"), list) else []:
        if not isinstance(role, dict):
            continue
        company_name = str(role.get("company") or "").strip()
        if not company_name:
            continue
        relations.append(
            {
                "type": "DIRECTOR_OF",
                "targetType": "Company",
                "targetId": f"company:{company_name.lower()}",
                "targetProperties": {
                    "name": company_name,
                    "source_url": role.get("source_url"),
                    "filing_date": role.get("filing_date"),
                    "form": role.get("form"),
                },
            }
        )
    return [{"entityType": "Person", "entityId": f"person:{person_name.lower()}", "properties": {"name": person_name, "cik": result.get("cik")}, "relations": relations}]


def _from_director_disclosure(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    directorships = result.get("directorships") if isinstance(result.get("directorships"), list) else []
    entities = []
    for item in directorships[:10]:
        if not isinstance(item, dict):
            continue
        company = str(item.get("company") or "").strip()
        if not company:
            continue
        entities.append(
            {
                "entityType": "DirectorRole",
                "entityId": f"directorrole:{company.lower()}:{str(item.get('tenure_start') or '').lower()}",
                "properties": {
                    "company": company,
                    "committee_roles": item.get("committee_roles"),
                    "tenure_start": item.get("tenure_start"),
                    "tenure_end": item.get("tenure_end"),
                    "compensation": item.get("compensation"),
                },
                "relations": [
                    {
                        "type": "DIRECTOR_OF",
                        "targetType": "Company",
                        "targetId": f"company:{company.lower()}",
                        "targetProperties": {"name": company},
                    }
                ],
            }
        )
    return entities


def _from_domain_whois(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    domain = str(result.get("domain") or "").strip().lower()
    if not domain:
        return []
    relations = []
    registrant_org = str(result.get("registrant_org") or "").strip()
    if registrant_org:
        relations.append(
            {
                "type": "AFFILIATED_WITH",
                "targetType": "Company",
                "targetId": f"company:{registrant_org.lower()}",
                "targetProperties": {"name": registrant_org},
            }
        )
    return [
        {
            "entityType": "Domain",
            "entityId": domain,
            "properties": {
                "domain": domain,
                "registration_date": result.get("registration_date"),
                "registrar": result.get("registrar"),
                "registrant_org": registrant_org or None,
                "name_servers": result.get("name_servers"),
                "source_url": result.get("source_url"),
            },
            "relations": relations,
        }
    ]
