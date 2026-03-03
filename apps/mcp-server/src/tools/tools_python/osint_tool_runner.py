#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import base64
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[5]


def _run_cmd(cmd: List[str], timeout_seconds: int) -> Dict[str, Any]:
    process = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    return {
        "returncode": process.returncode,
        "stdout": process.stdout,
        "stderr": process.stderr,
    }


def _find_cmd(candidates: List[str]) -> Optional[str]:
    for name in candidates:
        path = shutil.which(name)
        if path:
            return path

        # In our Kali container, many tools are installed in this virtualenv.
        venv_cmd = Path("/opt/osint-venv/bin") / name
        if venv_cmd.exists() and os.access(venv_cmd, os.X_OK):
            return str(venv_cmd)
    return None


def _require_cmd(name: str, aliases: Optional[List[str]] = None) -> str:
    candidates = [name]
    if aliases:
        candidates.extend(aliases)
    path = _find_cmd(candidates)
    if path:
        return path
    raise RuntimeError(f"Required command not found in PATH: {name}")


def _payload_error(error: str) -> Dict[str, Any]:
    return {"ok": False, "error": error}


def _read_json_file(path: Path) -> Any:
    if not path.exists():
        return None
    content = path.read_text(encoding="utf-8", errors="ignore").strip()
    if not content:
        return None
    return json.loads(content)


def _http_get_json(url: str, headers: Dict[str, str] | None = None, timeout_seconds: int = 30) -> Any:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
        body = response.read().decode("utf-8", errors="ignore")
    return json.loads(body)


def _resolve_input_path(raw_path: str) -> Path:
    candidate = Path(raw_path).expanduser()
    if candidate.exists():
        return candidate.resolve()

    if not candidate.is_absolute():
        repo_candidate = (REPO_ROOT / candidate).resolve()
        if repo_candidate.exists():
            return repo_candidate

    # When caller passes host workspace paths, remap them to /app paths in container.
    raw = str(candidate)
    marker = "/workspaces/llm-osint/"
    if marker in raw:
        suffix = raw.split(marker, 1)[1]
        mapped = (REPO_ROOT / suffix).resolve()
        if mapped.exists():
            return mapped

    raise RuntimeError(
        f"File not found: {candidate}. "
        "If calling from outside the container, pass a path under the repo (e.g. apps/...)."
    )


def _query_crtsh_subdomains(domain: str, timeout_seconds: int = 30) -> List[str]:
    url = f"https://crt.sh/?q=%25.{urllib.parse.quote(domain)}&output=json"
    try:
        payload = _http_get_json(url, timeout_seconds=timeout_seconds)
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    names: set[str] = set()
    for item in payload:
        if not isinstance(item, dict):
            continue
        raw = item.get("name_value")
        if not isinstance(raw, str):
            continue
        for part in raw.splitlines():
            host = part.strip().lower()
            if not host:
                continue
            host = host.removeprefix("*.").rstrip(".")
            if host == domain or host.endswith(f".{domain}"):
                names.add(host)
    return sorted(names)[:5000]


def _extract_emails(text: str, domain: str) -> List[str]:
    pattern = re.compile(r"[A-Za-z0-9._%+-]+@" + re.escape(domain) + r"\b")
    found = {m.group(0).lower() for m in pattern.finditer(text)}
    return sorted(found)[:2000]


def _tool_sherlock(input_data: Dict[str, Any]) -> Dict[str, Any]:
    username = str(input_data.get("username", "")).strip().lstrip("@")
    if not username:
        raise RuntimeError("Missing required input: username")

    sherlock_cmd = _require_cmd("sherlock")
    with tempfile.TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir) / "sherlock"
        out_dir.mkdir(parents=True, exist_ok=True)

        cmd = [
            sherlock_cmd,
            username,
            "--print-found",
            "--folderoutput",
            str(out_dir),
        ]
        result = _run_cmd(cmd, timeout_seconds=180)

        found_lines: List[str] = []
        for line in result["stdout"].splitlines():
            if "http://" in line or "https://" in line:
                found_lines.append(line.strip())

        output_files = [str(p) for p in out_dir.glob("*") if p.is_file()]
        return {
            "command": cmd,
            "returncode": result["returncode"],
            "foundCount": len(found_lines),
            "found": found_lines[:500],
            "outputFiles": output_files,
            "stdout": result["stdout"][:20000],
            "stderr": result["stderr"][:10000],
        }


def _tool_maigret(input_data: Dict[str, Any]) -> Dict[str, Any]:
    username = str(input_data.get("username", "")).strip().lstrip("@")
    if not username:
        raise RuntimeError("Missing required input: username")

    maigret_cmd = _require_cmd("maigret")
    with tempfile.TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir) / "maigret"
        out_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            maigret_cmd,
            username,
            "--folderoutput",
            str(out_dir),
            "--json",
            "simple",
            "--txt",
            "--no-progressbar",
        ]
        result = _run_cmd(cmd, timeout_seconds=240)
        parsed = None
        json_files = sorted(out_dir.glob("*.json"))
        if json_files:
            try:
                parsed = _read_json_file(json_files[0])
            except Exception:
                parsed = None
        return {
            "command": cmd,
            "returncode": result["returncode"],
            "parsed": parsed,
            "outputFiles": [str(p) for p in out_dir.glob("*") if p.is_file()],
            "stdout": result["stdout"][:20000],
            "stderr": result["stderr"][:10000],
        }


def _tool_holehe(input_data: Dict[str, Any]) -> Dict[str, Any]:
    email = str(input_data.get("email", "")).strip()
    if not email:
        raise RuntimeError("Missing required input: email")

    holehe_cmd = _require_cmd("holehe")
    cmd = [holehe_cmd, email, "--only-used"]
    result = _run_cmd(cmd, timeout_seconds=120)
    used = []
    for line in result["stdout"].splitlines():
        clean = line.strip()
        if clean.startswith("[+]"):
            used.append(clean)
    return {
        "command": cmd,
        "returncode": result["returncode"],
        "usedServiceCount": len(used),
        "usedServices": used[:500],
        "stdout": result["stdout"][:20000],
        "stderr": result["stderr"][:10000],
    }


def _tool_theharvester(input_data: Dict[str, Any]) -> Dict[str, Any]:
    domain = str(input_data.get("domain", "")).strip()
    source = str(input_data.get("source", "all")).strip() or "all"
    limit = int(input_data.get("limit", 100))
    if not domain:
        raise RuntimeError("Missing required input: domain")

    tool_cmd = _find_cmd(["theHarvester", "theHarvester.py", "theharvester"])
    venv_harvester = Path("/opt/osint-venv/bin/theHarvester")
    if (
        tool_cmd
        and "/usr/local/bin/" in tool_cmd
        and venv_harvester.exists()
        and os.access(venv_harvester, os.X_OK)
    ):
        tool_cmd = str(venv_harvester)
    if tool_cmd:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_base = str(Path(tmpdir) / "theharvester")
            cmd = [
                tool_cmd,
                "-d",
                domain,
                "-b",
                source,
                "-l",
                str(limit),
                "-f",
                out_base,
            ]
            result = _run_cmd(cmd, timeout_seconds=180)
            generated = [str(p) for p in Path(tmpdir).glob("*") if p.is_file()]
            response = {
                "command": cmd,
                "returncode": result["returncode"],
                "domain": domain,
                "source": source,
                "limit": limit,
                "generatedFiles": generated,
                "stdout": result["stdout"][:30000],
                "stderr": result["stderr"][:10000],
            }
            # Broken wrapper path from older images: retry with venv script.
            if (
                result["returncode"] != 0
                and "/opt/theHarvester/theHarvester.py" in result["stderr"]
                and venv_harvester.exists()
                and os.access(venv_harvester, os.X_OK)
                and str(venv_harvester) != tool_cmd
            ):
                retry_cmd = [
                    str(venv_harvester),
                    "-d",
                    domain,
                    "-b",
                    source,
                    "-l",
                    str(limit),
                    "-f",
                    out_base,
                ]
                retry_result = _run_cmd(retry_cmd, timeout_seconds=180)
                response = {
                    "command": retry_cmd,
                    "returncode": retry_result["returncode"],
                    "domain": domain,
                    "source": source,
                    "limit": limit,
                    "generatedFiles": generated,
                    "stdout": retry_result["stdout"][:30000],
                    "stderr": retry_result["stderr"][:10000],
                }
            return response

    # Fallback: passive CT search when CLI is missing.
    subdomains = _query_crtsh_subdomains(domain)
    return {
        "supported": True,
        "fallback": "crtsh",
        "domain": domain,
        "source": source,
        "limit": limit,
        "subdomainCount": len(subdomains),
        "subdomains": subdomains[:5000],
        "emails": [],
        "warning": "theHarvester command is not installed; returned passive crt.sh results.",
    }


def _tool_amass(input_data: Dict[str, Any]) -> Dict[str, Any]:
    domain = str(input_data.get("domain", "")).strip()
    passive = bool(input_data.get("passive", True))
    if not domain:
        raise RuntimeError("Missing required input: domain")

    amass_cmd = _find_cmd(["amass"])
    if amass_cmd:
        with tempfile.TemporaryDirectory() as tmpdir:
            txt_out = Path(tmpdir) / "amass.txt"
            cmd_base = [amass_cmd, "enum", "-d", domain]
            if passive:
                cmd_base.insert(2, "-passive")
            cmd = cmd_base + ["-o", str(txt_out)]
            result = _run_cmd(cmd, timeout_seconds=240)
            names = []
            if txt_out.exists():
                for line in txt_out.read_text(encoding="utf-8", errors="ignore").splitlines():
                    host = line.strip().lower().rstrip(".")
                    if host and (host == domain or host.endswith(f".{domain}")):
                        names.append(host)
            # Some versions print results to stdout even when -o fails silently.
            for line in result["stdout"].splitlines():
                host = line.strip().lower().rstrip(".")
                if host and (host == domain or host.endswith(f".{domain}")):
                    names.append(host)
            return {
                "command": cmd,
                "returncode": result["returncode"],
                "domain": domain,
                "passive": passive,
                "subdomainCount": len(names),
                "subdomains": sorted(set(names))[:5000],
                "warning": "",
                "stdout": result["stdout"][:20000],
                "stderr": result["stderr"][:10000],
            }

    subdomains = _query_crtsh_subdomains(domain)
    return {
        "supported": True,
        "fallback": "crtsh",
        "domain": domain,
        "passive": passive,
        "subdomainCount": len(subdomains),
        "subdomains": subdomains[:5000],
        "warning": "amass command is not installed; returned passive crt.sh results.",
    }


def _tool_sublist3r(input_data: Dict[str, Any]) -> Dict[str, Any]:
    domain = str(input_data.get("domain", "")).strip()
    if not domain:
        raise RuntimeError("Missing required input: domain")

    sublist3r_cmd = _require_cmd("sublist3r")
    with tempfile.TemporaryDirectory() as tmpdir:
        out_file = Path(tmpdir) / "subdomains.txt"
        cmd = [
            sublist3r_cmd,
            "-d",
            domain,
            "-o",
            str(out_file),
        ]
        result = _run_cmd(cmd, timeout_seconds=180)
        subdomains = []
        if out_file.exists():
            subdomains = [
                line.strip()
                for line in out_file.read_text(encoding="utf-8", errors="ignore").splitlines()
                if line.strip()
            ]
        return {
            "command": cmd,
            "returncode": result["returncode"],
            "domain": domain,
            "subdomainCount": len(subdomains),
            "subdomains": subdomains[:5000],
            "stdout": result["stdout"][:20000],
            "stderr": result["stderr"][:10000],
        }


def _tool_whatweb(input_data: Dict[str, Any]) -> Dict[str, Any]:
    target = str(input_data.get("target", "")).strip()
    if not target:
        raise RuntimeError("Missing required input: target")

    whatweb_cmd = _require_cmd("whatweb")
    with tempfile.TemporaryDirectory() as tmpdir:
        json_file = Path(tmpdir) / "whatweb.json"
        cmd = [whatweb_cmd, target, "--log-json", str(json_file)]
        result = _run_cmd(cmd, timeout_seconds=120)
        parsed = None
        if json_file.exists():
            try:
                parsed = _read_json_file(json_file)
            except Exception:
                parsed = None
        return {
            "command": cmd,
            "returncode": result["returncode"],
            "target": target,
            "parsed": parsed,
            "stdout": result["stdout"][:20000],
            "stderr": result["stderr"][:10000],
        }


def _tool_exiftool(input_data: Dict[str, Any]) -> Dict[str, Any]:
    path = str(input_data.get("path", "")).strip()
    content_b64 = str(input_data.get("contentBase64", "")).strip()
    if not path and not content_b64:
        raise RuntimeError("Missing required input: path or contentBase64")

    temp_dir: Optional[tempfile.TemporaryDirectory[str]] = None
    if path:
        resolved = _resolve_input_path(path)
    else:
        try:
            payload = base64.b64decode(content_b64, validate=True)
        except Exception:
            raise RuntimeError("contentBase64 is not valid base64 data")
        temp_dir = tempfile.TemporaryDirectory()
        filename = str(input_data.get("filename", "input.bin")).strip() or "input.bin"
        resolved = Path(temp_dir.name) / filename
        resolved.write_bytes(payload)

    exiftool_cmd = _require_cmd("exiftool")
    cmd = [exiftool_cmd, "-json", str(resolved)]
    result = _run_cmd(cmd, timeout_seconds=60)
    parsed = None
    try:
        parsed = json.loads(result["stdout"])
    except Exception:
        parsed = None
    response = {
        "command": cmd,
        "returncode": result["returncode"],
        "path": str(resolved),
        "parsed": parsed,
        "stdout": result["stdout"][:20000],
        "stderr": result["stderr"][:10000],
    }
    if temp_dir is not None:
        temp_dir.cleanup()
    return response


def _tool_phoneinfoga(input_data: Dict[str, Any]) -> Dict[str, Any]:
    number = str(input_data.get("number", "")).strip()
    if not number:
        raise RuntimeError("Missing required input: number")

    phoneinfoga_cmd = _find_cmd(["phoneinfoga"])
    if phoneinfoga_cmd:
        cmd = [phoneinfoga_cmd, "scan", "-n", number]
        result = _run_cmd(cmd, timeout_seconds=180)
        return {
            "command": cmd,
            "returncode": result["returncode"],
            "number": number,
            "stdout": result["stdout"][:30000],
            "stderr": result["stderr"][:10000],
        }

    digits = "".join(ch for ch in number if ch.isdigit())
    e164_guess = f"+{digits}" if digits else number
    return {
        "supported": False,
        "number": number,
        "normalized": e164_guess,
        "warning": "phoneinfoga command is not installed in this container.",
        "nextSteps": [
            "Install phoneinfoga binary in the kali image.",
            "Re-run this tool for carrier/OSINT enrichment.",
        ],
    }


def _tool_reconng(input_data: Dict[str, Any]) -> Dict[str, Any]:
    domain = str(input_data.get("domain", "")).strip()
    if not domain:
        raise RuntimeError("Missing required input: domain")

    recon_cmd = _find_cmd(["recon-ng"])
    module = str(input_data.get("module", "recon/domains-hosts/hackertarget")).strip() or "recon/domains-hosts/hackertarget"
    source = str(input_data.get("source", domain)).strip() or domain
    if recon_cmd:
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = "ws_osint"
            resource_path = Path(tmpdir) / "recon-ng.rc"
            script = "\n".join(
                [
                    f"workspaces create {workspace}",
                    f"workspaces select {workspace}",
                    f"modules load {module}",
                    f"options set SOURCE {source}",
                    "run",
                    "show hosts",
                    "show contacts",
                    "show credentials",
                    "exit",
                ]
            )
            resource_path.write_text(script, encoding="utf-8")
            cmd = [recon_cmd, "-r", str(resource_path)]
            result = _run_cmd(cmd, timeout_seconds=240)
            stdout = result["stdout"]
            if (
                "Invalid module name" in stdout
                or "No modules enabled/installed" in stdout
                or "Invalid option name" in stdout
            ):
                # Module marketplace isn't initialized; fall back to passive data.
                result = {"returncode": result["returncode"], "stdout": stdout, "stderr": result["stderr"]}
            else:
                return {
                    "command": cmd,
                    "returncode": result["returncode"],
                    "domain": domain,
                    "module": module,
                    "stdout": stdout[:40000],
                    "stderr": result["stderr"][:10000],
                }
            # fall through to whois+dig fallback with warning below

    whois_cmd = _find_cmd(["whois"])
    dig_cmd = _find_cmd(["dig"])
    whois_text = ""
    if whois_cmd:
        whois_res = _run_cmd([whois_cmd, domain], timeout_seconds=30)
        whois_text = whois_res["stdout"][:40000]
    ns_records: List[str] = []
    mx_records: List[str] = []
    if dig_cmd:
        ns_res = _run_cmd([dig_cmd, "+short", "NS", domain], timeout_seconds=20)
        mx_res = _run_cmd([dig_cmd, "+short", "MX", domain], timeout_seconds=20)
        ns_records = [line.strip() for line in ns_res["stdout"].splitlines() if line.strip()]
        mx_records = [line.strip() for line in mx_res["stdout"].splitlines() if line.strip()]
    return {
        "supported": True,
        "fallback": "whois+dig",
        "domain": domain,
        "module": module,
        "whois_excerpt": whois_text,
        "ns_records": ns_records[:100],
        "mx_records": mx_records[:100],
        "subdomains": _query_crtsh_subdomains(domain)[:1000],
        "warning": "recon-ng module unavailable; returned passive fallback data.",
    }


def _find_spiderfoot_cmd() -> str:
    found = _find_cmd(["spiderfoot", "sf.py"])
    if found:
        return found
    sf_path = Path("/opt/spiderfoot/sf.py")
    if sf_path.exists() and os.access(sf_path, os.X_OK):
        return str(sf_path)
    raise RuntimeError("SpiderFoot command not found (expected spiderfoot or sf.py)")


def _tool_spiderfoot(input_data: Dict[str, Any]) -> Dict[str, Any]:
    target = str(input_data.get("target", "")).strip()
    if not target:
        raise RuntimeError("Missing required input: target")
    modules = input_data.get("modules", "sfp_dnsresolve,sfp_email,sfp_accounts")
    if isinstance(modules, list):
        module_str = ",".join(str(item).strip() for item in modules if str(item).strip())
    else:
        module_str = str(modules).strip() or "sfp_dnsresolve,sfp_email,sfp_accounts"

    # SpiderFoot CLI behavior is unstable in this headless container; keep output deterministic.
    return {
        "supported": True,
        "fallback": "passive_dns",
        "target": target,
        "modules": module_str,
        "subdomains": _query_crtsh_subdomains(target)[:1000],
        "warning": "SpiderFoot execution disabled in this container; returned passive DNS fallback.",
    }


def _tool_whatsmyname(input_data: Dict[str, Any]) -> Dict[str, Any]:
    username = str(input_data.get("username", "")).strip().lstrip("@")
    if not username:
        raise RuntimeError("Missing required input: username")

    # Keep default latency bounded; this tool performs one HTTP request per site.
    max_sites = int(input_data.get("maxSites", 40))
    timeout_seconds = int(input_data.get("timeoutSeconds", 2))
    data_url = str(
        input_data.get(
            "dataUrl",
            "https://raw.githubusercontent.com/WebBreacher/WhatsMyName/main/wmn-data.json",
        )
    ).strip()

    payload = _http_get_json(data_url, timeout_seconds=20)
    sites = payload.get("sites", []) if isinstance(payload, dict) else []
    findings: List[Dict[str, Any]] = []
    checked = 0

    for site in sites:
        if checked >= max_sites:
            break
        if not isinstance(site, dict):
            continue
        uri_check = site.get("uri_check")
        if not isinstance(uri_check, str) or "{account}" not in uri_check:
            continue

        checked += 1
        url = uri_check.replace("{account}", username)
        req = urllib.request.Request(url, method="GET", headers={"User-Agent": "llm-osint/1.0"})
        exists = False
        status = None
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
                status = int(response.status)
                exists = status == 200
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            exists = False
        except Exception:
            status = None
            exists = False

        if exists:
            findings.append(
                {
                    "site": site.get("name"),
                    "url": url,
                    "status": status,
                }
            )

    return {
        "username": username,
        "checkedSites": checked,
        "foundCount": len(findings),
        "found": findings[:500],
    }


def _tool_hibp(input_data: Dict[str, Any]) -> Dict[str, Any]:
    email = str(input_data.get("email", "")).strip()
    if not email:
        raise RuntimeError("Missing required input: email")

    api_key = str(input_data.get("apiKey") or os.getenv("HIBP_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("HIBP_API_KEY missing. Set env HIBP_API_KEY or pass input apiKey.")

    url = f"https://haveibeenpwned.com/api/v3/breachedaccount/{urllib.parse.quote(email)}?truncateResponse=false"
    headers = {
        "hibp-api-key": api_key,
        "user-agent": "llm-osint-pipeline",
        "accept": "application/json",
    }
    try:
        breaches = _http_get_json(url, headers=headers, timeout_seconds=30)
        if not isinstance(breaches, list):
            breaches = []
        return {
            "email": email,
            "breachCount": len(breaches),
            "breaches": breaches[:200],
        }
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return {"email": email, "breachCount": 0, "breaches": []}
        body = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else ""
        raise RuntimeError(f"HIBP request failed ({exc.code}): {body[:400]}")


def _tool_shodan_host(input_data: Dict[str, Any]) -> Dict[str, Any]:
    host = str(input_data.get("host", "")).strip()
    if not host:
        raise RuntimeError("Missing required input: host (IP)")

    api_key = str(input_data.get("apiKey") or os.getenv("SHODAN_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("SHODAN_API_KEY missing. Set env SHODAN_API_KEY or pass input apiKey.")

    url = f"https://api.shodan.io/shodan/host/{urllib.parse.quote(host)}?key={urllib.parse.quote(api_key)}"
    data = _http_get_json(url, headers={"accept": "application/json"}, timeout_seconds=30)
    if not isinstance(data, dict):
        raise RuntimeError("Invalid Shodan response")
    services = data.get("data", []) if isinstance(data.get("data"), list) else []
    return {
        "host": host,
        "organization": data.get("org"),
        "country": data.get("country_name"),
        "openPortCount": len(services),
        "ports": data.get("ports", []),
        "services": services[:200],
    }


def _tool_dnsdumpster(input_data: Dict[str, Any]) -> Dict[str, Any]:
    domain = str(input_data.get("domain", "")).strip()
    if not domain:
        raise RuntimeError("Missing required input: domain")

    # Keep deterministic passive fallback to avoid no-output failures from CLI/library variants.
    dig_cmd = _find_cmd(["dig"])
    ns_records: List[str] = []
    mx_records: List[str] = []
    if dig_cmd:
        ns_res = _run_cmd([dig_cmd, "+short", "NS", domain], timeout_seconds=20)
        mx_res = _run_cmd([dig_cmd, "+short", "MX", domain], timeout_seconds=20)
        ns_records = [line.strip() for line in ns_res["stdout"].splitlines() if line.strip()]
        mx_records = [line.strip() for line in mx_res["stdout"].splitlines() if line.strip()]
    return {
        "supported": True,
        "fallback": "dig+crtsh",
        "domain": domain,
        "ns_records": ns_records[:100],
        "mx_records": mx_records[:100],
        "subdomains": _query_crtsh_subdomains(domain)[:1000],
        "warning": "dnsdumpster CLI/library disabled; returned passive DNS fallback.",
    }


def _tool_maltego_manual(_: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "supported": False,
        "reason": "Maltego is a GUI-first tool and is not reliably automatable in this headless MCP container.",
        "nextSteps": [
            "Run Maltego Community/Desktop on a workstation.",
            "Export entities/links as CSV/GraphML and ingest results back through MCP graph tools.",
        ],
    }


def _tool_foca_manual(_: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "supported": False,
        "reason": "FOCA is Windows GUI-focused and not available as a stable headless Linux CLI.",
        "nextSteps": [
            "Run FOCA in Windows for document metadata extraction.",
            "Use exiftool and theHarvester in this pipeline for automated metadata/email pivots.",
        ],
    }


def main() -> None:
    try:
        payload = json.load(sys.stdin)
        tool = str(payload.get("tool", "")).strip()
        input_data = payload.get("input", {}) or {}
        if not isinstance(input_data, dict):
            raise RuntimeError("Invalid input payload")

        handlers = {
            "osint_sherlock_username": _tool_sherlock,
            "osint_maigret_username": _tool_maigret,
            "osint_whatsmyname_username": _tool_whatsmyname,
            "osint_holehe_email": _tool_holehe,
            "osint_hibp_email": _tool_hibp,
            "osint_theharvester_email_domain": _tool_theharvester,
            "osint_reconng_domain": _tool_reconng,
            "osint_spiderfoot_scan": _tool_spiderfoot,
            "osint_amass_domain": _tool_amass,
            "osint_sublist3r_domain": _tool_sublist3r,
            "osint_dnsdumpster_domain": _tool_dnsdumpster,
            "osint_maltego_manual": _tool_maltego_manual,
            "osint_foca_manual": _tool_foca_manual,
            "osint_shodan_host": _tool_shodan_host,
            "osint_whatweb_target": _tool_whatweb,
            "osint_exiftool_extract": _tool_exiftool,
            "osint_phoneinfoga_number": _tool_phoneinfoga,
        }

        handler = handlers.get(tool)
        if handler is None:
            raise RuntimeError(f"Unsupported OSINT tool: {tool}")

        result = handler(input_data)
        print(json.dumps({"ok": True, "result": result}))
    except subprocess.TimeoutExpired as exc:
        print(json.dumps(_payload_error(f"Tool timed out: {exc}")))
    except Exception as exc:
        print(json.dumps(_payload_error(str(exc))))


if __name__ == "__main__":
    main()
