from __future__ import annotations

from typing import Any, Dict, List

from business.common import build_evidence, clean_text, http_json_request, normalize_query, score_business_confidence


def _extract_registration_date(events: List[Dict[str, Any]]) -> str:
    for item in events:
        if not isinstance(item, dict):
            continue
        if str(item.get("eventAction") or "").lower() in {"registration", "registered"}:
            return str(item.get("eventDate") or "").strip()
    return ""


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    if not query["domain"]:
        raise RuntimeError("Missing required input: domain")
    domain = query["domain"]
    try:
        raw = http_json_request(f"https://rdap.org/domain/{domain}", timeout=20)
        source_url = f"https://rdap.org/domain/{domain}"
    except Exception:
        tld = domain.rsplit(".", 1)[-1]
        raw = http_json_request(f"https://rdap.verisign.com/{tld}/v1/domain/{domain}", timeout=20)
        source_url = f"https://rdap.verisign.com/{tld}/v1/domain/{domain}"

    entities = raw.get("entities") if isinstance(raw.get("entities"), list) else []
    registrant_org = ""
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        roles = entity.get("roles") if isinstance(entity.get("roles"), list) else []
        if "registrant" not in roles:
            continue
        vcard_array = entity.get("vcardArray")
        if isinstance(vcard_array, list) and len(vcard_array) == 2 and isinstance(vcard_array[1], list):
            for item in vcard_array[1]:
                if not isinstance(item, list) or len(item) < 4:
                    continue
                if str(item[0]).lower() in {"org", "fn"}:
                    registrant_org = clean_text(item[3], max_len=160)
                    break
        if registrant_org:
            break

    events = raw.get("events") if isinstance(raw.get("events"), list) else []
    nameservers = raw.get("nameservers") if isinstance(raw.get("nameservers"), list) else []
    registrar = ""
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        roles = entity.get("roles") if isinstance(entity.get("roles"), list) else []
        if "registrar" not in roles:
            continue
        registrar = clean_text(entity.get("handle") or "", max_len=160) or registrar
    return {
        "tool": "domain_whois_search",
        "domain": domain,
        "registration_date": _extract_registration_date([item for item in events if isinstance(item, dict)]),
        "registrar": registrar,
        "registrant_org": registrant_org,
        "name_servers": [str(item.get("ldhName") or "").strip() for item in nameservers if isinstance(item, dict) and str(item.get("ldhName") or "").strip()],
        "source_url": source_url,
        "evidence": [build_evidence(source_url, domain, ["domain"])],
        "confidence": score_business_confidence(address_match=bool(registrant_org), timeline_consistency=bool(events)),
    }
