#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List


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


def _require_cmd(name: str) -> str:
    path = shutil.which(name)
    if not path:
        raise RuntimeError(f"Required command not found in PATH: {name}")
    return path


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


def _tool_sherlock(input_data: Dict[str, Any]) -> Dict[str, Any]:
    username = str(input_data.get("username", "")).strip().lstrip("@")
    if not username:
        raise RuntimeError("Missing required input: username")

    _require_cmd("sherlock")
    with tempfile.TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir) / "sherlock"
        out_dir.mkdir(parents=True, exist_ok=True)

        cmd = [
            "sherlock",
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

    _require_cmd("maigret")
    with tempfile.TemporaryDirectory() as tmpdir:
        json_file = Path(tmpdir) / "maigret.json"
        txt_file = Path(tmpdir) / "maigret.txt"
        cmd = [
            "maigret",
            username,
            "--json",
            str(json_file),
            "--txt",
            str(txt_file),
        ]
        result = _run_cmd(cmd, timeout_seconds=240)
        parsed = None
        if json_file.exists():
            try:
                parsed = _read_json_file(json_file)
            except Exception:
                parsed = None
        return {
            "command": cmd,
            "returncode": result["returncode"],
            "parsed": parsed,
            "stdout": result["stdout"][:20000],
            "stderr": result["stderr"][:10000],
        }


def _tool_holehe(input_data: Dict[str, Any]) -> Dict[str, Any]:
    email = str(input_data.get("email", "")).strip()
    if not email:
        raise RuntimeError("Missing required input: email")

    _require_cmd("holehe")
    cmd = ["holehe", email, "--only-used"]
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

    _require_cmd("theHarvester")
    with tempfile.TemporaryDirectory() as tmpdir:
        out_base = str(Path(tmpdir) / "theharvester")
        cmd = [
            "theHarvester",
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
        return {
            "command": cmd,
            "returncode": result["returncode"],
            "domain": domain,
            "source": source,
            "limit": limit,
            "generatedFiles": generated,
            "stdout": result["stdout"][:30000],
            "stderr": result["stderr"][:10000],
        }


def _tool_amass(input_data: Dict[str, Any]) -> Dict[str, Any]:
    domain = str(input_data.get("domain", "")).strip()
    passive = bool(input_data.get("passive", True))
    if not domain:
        raise RuntimeError("Missing required input: domain")

    _require_cmd("amass")
    with tempfile.TemporaryDirectory() as tmpdir:
        json_out = Path(tmpdir) / "amass.json"
        cmd = ["amass", "enum", "-d", domain, "-json", str(json_out)]
        if passive:
            cmd.insert(2, "-passive")
        result = _run_cmd(cmd, timeout_seconds=240)
        entries: List[Dict[str, Any]] = []
        if json_out.exists():
            for line in json_out.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        names = []
        for item in entries:
            name = item.get("name")
            if isinstance(name, str):
                names.append(name)
        return {
            "command": cmd,
            "returncode": result["returncode"],
            "domain": domain,
            "passive": passive,
            "subdomainCount": len(names),
            "subdomains": sorted(set(names))[:5000],
            "stdout": result["stdout"][:20000],
            "stderr": result["stderr"][:10000],
        }


def _tool_sublist3r(input_data: Dict[str, Any]) -> Dict[str, Any]:
    domain = str(input_data.get("domain", "")).strip()
    if not domain:
        raise RuntimeError("Missing required input: domain")

    _require_cmd("sublist3r")
    with tempfile.TemporaryDirectory() as tmpdir:
        out_file = Path(tmpdir) / "subdomains.txt"
        cmd = [
            "sublist3r",
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

    _require_cmd("whatweb")
    with tempfile.TemporaryDirectory() as tmpdir:
        json_file = Path(tmpdir) / "whatweb.json"
        cmd = ["whatweb", target, "--log-json", str(json_file)]
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
    if not path:
        raise RuntimeError("Missing required input: path")

    resolved = Path(path).resolve()
    if not resolved.exists():
        raise RuntimeError(f"File not found: {resolved}")

    _require_cmd("exiftool")
    cmd = ["exiftool", "-json", str(resolved)]
    result = _run_cmd(cmd, timeout_seconds=60)
    parsed = None
    try:
        parsed = json.loads(result["stdout"])
    except Exception:
        parsed = None
    return {
        "command": cmd,
        "returncode": result["returncode"],
        "path": str(resolved),
        "parsed": parsed,
        "stdout": result["stdout"][:20000],
        "stderr": result["stderr"][:10000],
    }


def _tool_phoneinfoga(input_data: Dict[str, Any]) -> Dict[str, Any]:
    number = str(input_data.get("number", "")).strip()
    if not number:
        raise RuntimeError("Missing required input: number")

    _require_cmd("phoneinfoga")
    cmd = ["phoneinfoga", "scan", "-n", number]
    result = _run_cmd(cmd, timeout_seconds=180)
    return {
        "command": cmd,
        "returncode": result["returncode"],
        "number": number,
        "stdout": result["stdout"][:30000],
        "stderr": result["stderr"][:10000],
    }


def _tool_reconng(input_data: Dict[str, Any]) -> Dict[str, Any]:
    domain = str(input_data.get("domain", "")).strip()
    if not domain:
        raise RuntimeError("Missing required input: domain")

    _require_cmd("recon-ng")
    module = str(input_data.get("module", "recon/domains-hosts/hackertarget")).strip() or "recon/domains-hosts/hackertarget"
    source = str(input_data.get("source", domain)).strip() or domain

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
        cmd = ["recon-ng", "-r", str(resource_path)]
        result = _run_cmd(cmd, timeout_seconds=240)
        return {
            "command": cmd,
            "returncode": result["returncode"],
            "domain": domain,
            "module": module,
            "stdout": result["stdout"][:40000],
            "stderr": result["stderr"][:10000],
        }


def _find_spiderfoot_cmd() -> str:
    for candidate in ("spiderfoot", "sf.py"):
        path = shutil.which(candidate)
        if path:
            return path
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

    cmd_name = _find_spiderfoot_cmd()
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = Path(tmpdir) / "spiderfoot.json"
        if cmd_name.endswith("sf.py"):
            cmd = [
                cmd_name,
                "-s",
                target,
                "-m",
                module_str,
                "-o",
                "json",
                "-q",
            ]
        else:
            cmd = [
                cmd_name,
                "-s",
                target,
                "-m",
                module_str,
                "-o",
                "json",
                "-q",
            ]
        result = _run_cmd(cmd, timeout_seconds=300)
        parsed = None
        try:
            parsed = json.loads(result["stdout"])
        except Exception:
            parsed = None
        return {
            "command": cmd,
            "returncode": result["returncode"],
            "target": target,
            "modules": module_str,
            "parsed": parsed,
            "rawOutputPath": str(output_file),
            "stdout": result["stdout"][:40000],
            "stderr": result["stderr"][:12000],
        }


def _tool_whatsmyname(input_data: Dict[str, Any]) -> Dict[str, Any]:
    username = str(input_data.get("username", "")).strip().lstrip("@")
    if not username:
        raise RuntimeError("Missing required input: username")

    max_sites = int(input_data.get("maxSites", 300))
    timeout_seconds = int(input_data.get("timeoutSeconds", 8))
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

    cmd = shutil.which("dnsdumpster")
    if cmd:
        result = _run_cmd([cmd, domain], timeout_seconds=180)
        return {
            "command": [cmd, domain],
            "returncode": result["returncode"],
            "domain": domain,
            "stdout": result["stdout"][:40000],
            "stderr": result["stderr"][:12000],
        }

    try:
        from dnsdumpster.DNSDumpsterAPI import DNSDumpsterAPI  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"dnsdumpster library unavailable: {exc}")

    api = DNSDumpsterAPI()
    response = api.search(domain)
    if not isinstance(response, dict):
        raise RuntimeError("Invalid DNSDumpster response")
    dns_records = response.get("dns_records", {}) if isinstance(response.get("dns_records"), dict) else {}
    host_count = 0
    for key in ("host", "dns", "mx", "txt"):
        value = dns_records.get(key)
        if isinstance(value, list):
            host_count += len(value)
    return {
        "domain": domain,
        "recordGroups": list(dns_records.keys()),
        "recordCountApprox": host_count,
        "dns_records": dns_records,
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
