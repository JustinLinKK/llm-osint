from __future__ import annotations

from typing import Any, Dict, List

from technical.common import domain_from_email


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    contacts = input_data.get("contacts") if isinstance(input_data.get("contacts"), list) else []
    emails = [str(item).strip().lower() for item in input_data.get("emails", []) if isinstance(item, str)]
    organizations = [str(item).strip() for item in input_data.get("organizations", []) if isinstance(item, str)]
    addresses = [str(item).strip() for item in input_data.get("addresses", []) if isinstance(item, str)]
    for item in contacts:
        if not isinstance(item, dict):
            continue
        email = item.get("email")
        if isinstance(email, str) and email.strip():
            emails.append(email.strip().lower())
        organization = item.get("organization")
        if isinstance(organization, str) and organization.strip():
            organizations.append(organization.strip())
        address = item.get("address")
        if isinstance(address, str) and address.strip():
            addresses.append(address.strip())

    domain_counts: Dict[str, int] = {}
    for email in emails:
        domain = domain_from_email(email)
        if domain:
            domain_counts[domain] = domain_counts.get(domain, 0) + 1

    organization_counts: Dict[str, int] = {}
    for organization in organizations:
        key = organization.lower()
        if key:
            organization_counts[key] = organization_counts.get(key, 0) + 1

    address_counts: Dict[str, int] = {}
    for address in addresses:
        key = address.lower()
        if key:
            address_counts[key] = address_counts.get(key, 0) + 1

    shared_domains = [
        {"domain": domain, "count": count}
        for domain, count in sorted(domain_counts.items(), key=lambda item: (-item[1], item[0]))
        if count > 1
    ]
    shared_organizations = [
        {"organization": organization, "count": count}
        for organization, count in sorted(organization_counts.items(), key=lambda item: (-item[1], item[0]))
        if count > 1
    ]
    shared_addresses = [
        {"address": address, "count": count}
        for address, count in sorted(address_counts.items(), key=lambda item: (-item[1], item[0]))
        if count > 1
    ]
    return {
        "tool": "shared_contact_pivot_search",
        "contacts": contacts,
        "shared_domains": shared_domains[:20],
        "shared_organizations": shared_organizations[:20],
        "shared_addresses": shared_addresses[:20],
    }
