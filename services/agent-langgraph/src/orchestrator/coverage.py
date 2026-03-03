from __future__ import annotations

from typing import Dict


def empty_coverage_ledger() -> Dict[str, bool]:
    return {
        "identity": False,
        "history": False,
        "contacts": False,
        "relationships": False,
        "code_presence": False,
        "package_publications": False,
        "technical_org_affiliations": False,
        "academic": False,
        "business_roles": False,
        "public_records": False,
        "archived_history": False,
    }


def coverage_led_stop_condition(coverage_ledger: Dict[str, bool]) -> bool:
    has_identity = coverage_ledger.get("identity", False)
    has_history = coverage_ledger.get("history", False)
    has_relationships = coverage_ledger.get("relationships", False)
    has_anchor = bool(
        coverage_ledger.get("code_presence", False)
        or coverage_ledger.get("academic", False)
        or coverage_ledger.get("business_roles", False)
    )
    return bool(has_identity and has_history and has_relationships and has_anchor)
