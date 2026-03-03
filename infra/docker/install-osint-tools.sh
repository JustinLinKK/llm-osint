#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
OSINT_VENV_DIR="${OSINT_VENV_DIR:-/opt/osint-venv}"
OSINT_STRICT_INSTALL="${OSINT_STRICT_INSTALL:-1}"

log() {
  echo "[osint-install] $*"
}

warn() {
  echo "[osint-install] warning: $*" >&2
}

log "apt-get update"
apt-get update

APT_PACKAGES=(
  ca-certificates
  curl
  git
  jq
  python3
  python3-pip
  python3-venv
  dnsutils
  whois
  nmap
  whatweb
  amass
  libimage-exiftool-perl
  python3-requests
  unzip
)

for pkg in "${APT_PACKAGES[@]}"; do
  log "installing apt package: ${pkg}"
  if ! apt-get install -y --no-install-recommends "${pkg}"; then
    warn "failed to install apt package ${pkg}"
  fi
done

log "creating virtualenv at ${OSINT_VENV_DIR}"
python3 -m venv "${OSINT_VENV_DIR}"

PIP_BIN="${OSINT_VENV_DIR}/bin/pip"
PYTHON_BIN="${OSINT_VENV_DIR}/bin/python"

log "upgrading pip in virtualenv"
"${PYTHON_BIN}" -m pip install --no-cache-dir --upgrade pip setuptools wheel

PIP_PACKAGES=(
  sherlock-project
  maigret
  holehe
  git+https://github.com/laramies/theHarvester.git
  sublist3r
  dnsdumpster
  shodan
)

for pkg in "${PIP_PACKAGES[@]}"; do
  log "installing pip package: ${pkg}"
  if ! "${PIP_BIN}" install --no-cache-dir "${pkg}"; then
    warn "failed to install pip package ${pkg}"
  fi
done

install_tar_or_zip_asset() {
  local asset_url="$1"
  local extract_dir="$2"
  local archive_path="/tmp/asset_download"
  if ! curl -fsSL "${asset_url}" -o "${archive_path}"; then
    return 1
  fi
  if printf "%s" "${asset_url}" | grep -qiE '\.zip($|\?)'; then
    unzip -q "${archive_path}" -d "${extract_dir}"
  else
    tar -xf "${archive_path}" -C "${extract_dir}"
  fi
  rm -f "${archive_path}"
}

AMASS_DIR="/tmp/amass"
if ! command -v amass >/dev/null 2>&1; then
  log "apt did not provide amass; attempting GitHub release fallback"
  mkdir -p "${AMASS_DIR}"
  AMASS_RELEASE_JSON="$(curl -fsSL https://api.github.com/repos/owasp-amass/amass/releases/latest || true)"
  AMASS_ASSET_URL="$(printf '%s' "${AMASS_RELEASE_JSON}" | jq -r '.assets[]?.browser_download_url' | grep -Ei 'linux.*amd64.*\.(zip|tar\.gz)$' | head -n1 || true)"
  if [ -n "${AMASS_ASSET_URL}" ] && install_tar_or_zip_asset "${AMASS_ASSET_URL}" "${AMASS_DIR}"; then
    AMASS_BIN="$(find "${AMASS_DIR}" -type f -name amass | head -n1 || true)"
    if [ -n "${AMASS_BIN}" ] && [ -f "${AMASS_BIN}" ]; then
      install -m 0755 "${AMASS_BIN}" /usr/local/bin/amass
    else
      warn "amass binary not found in downloaded archive"
    fi
  else
    warn "failed to download/extract amass release archive"
  fi
fi

THEHARVESTER_DIR="/opt/theHarvester"
THEHARVESTER_FALLBACK_REF="${THEHARVESTER_FALLBACK_REF:-4.8.2}"
log "ensuring theHarvester is executable"
if ! "${PIP_BIN}" show theHarvester >/dev/null 2>&1; then
  "${PIP_BIN}" install --no-cache-dir git+https://github.com/laramies/theHarvester.git || warn "failed to install theHarvester via pip"
fi
if [ -x "${OSINT_VENV_DIR}/bin/theHarvester" ]; then
  cat >/usr/local/bin/theHarvester <<EOF
#!/usr/bin/env bash
exec "${OSINT_VENV_DIR}/bin/theHarvester" "\$@"
EOF
  cat >/usr/local/bin/theHarvester.py <<EOF
#!/usr/bin/env bash
exec "${OSINT_VENV_DIR}/bin/theHarvester" "\$@"
EOF
  chmod +x /usr/local/bin/theHarvester /usr/local/bin/theHarvester.py
else
  rm -rf "${THEHARVESTER_DIR}" || true
  if git clone --depth 1 --branch "${THEHARVESTER_FALLBACK_REF}" https://github.com/laramies/theHarvester.git "${THEHARVESTER_DIR}"; then
    if [ -f "${THEHARVESTER_DIR}/pyproject.toml" ]; then
      log "installing theHarvester runtime dependencies from source"
      if ! "${PYTHON_BIN}" - <<PY >/tmp/theharvester-deps.txt; then
import tomllib
from pathlib import Path
data = tomllib.loads(Path("${THEHARVESTER_DIR}/pyproject.toml").read_text(encoding="utf-8"))
for dep in data.get("project", {}).get("dependencies", []):
    print(dep)
PY
        warn "failed to parse theHarvester dependencies from pyproject.toml"
      else
        while IFS= read -r dep; do
          [ -n "${dep}" ] || continue
          "${PIP_BIN}" install --no-cache-dir "${dep}" || warn "failed to install theHarvester dependency: ${dep}"
        done </tmp/theharvester-deps.txt
      fi
    fi
    THEHARVESTER_ENTRY=""
    if [ -f "${THEHARVESTER_DIR}/bin/theHarvester" ]; then
      THEHARVESTER_ENTRY="${THEHARVESTER_DIR}/bin/theHarvester"
    elif [ -f "${THEHARVESTER_DIR}/theHarvester/theHarvester.py" ]; then
      THEHARVESTER_ENTRY="${THEHARVESTER_DIR}/theHarvester/theHarvester.py"
    elif [ -f "${THEHARVESTER_DIR}/theHarvester.py" ]; then
      THEHARVESTER_ENTRY="${THEHARVESTER_DIR}/theHarvester.py"
    fi
    if [ -n "${THEHARVESTER_ENTRY}" ]; then
    cat >/usr/local/bin/theHarvester <<EOF
#!/usr/bin/env bash
export PYTHONPATH="${THEHARVESTER_DIR}:\${PYTHONPATH:-}"
exec "${PYTHON_BIN}" "${THEHARVESTER_ENTRY}" "\$@"
EOF
    cat >/usr/local/bin/theHarvester.py <<EOF
#!/usr/bin/env bash
export PYTHONPATH="${THEHARVESTER_DIR}:\${PYTHONPATH:-}"
exec "${PYTHON_BIN}" "${THEHARVESTER_ENTRY}" "\$@"
EOF
    chmod +x /usr/local/bin/theHarvester /usr/local/bin/theHarvester.py
    else
      warn "failed to identify theHarvester entry point in source tree"
    fi
  else
    warn "failed to provision theHarvester executable"
  fi
fi

RECONNG_DIR="/opt/recon-ng"
if ! command -v recon-ng >/dev/null 2>&1; then
  log "installing recon-ng from source"
  if git clone --depth 1 https://github.com/lanmaster53/recon-ng.git "${RECONNG_DIR}"; then
    if [ -f "${RECONNG_DIR}/REQUIREMENTS" ]; then
      "${PIP_BIN}" install --no-cache-dir -r "${RECONNG_DIR}/REQUIREMENTS" || warn "failed to install recon-ng requirements"
    fi
    if [ -f "${RECONNG_DIR}/recon-ng" ]; then
      cat >/usr/local/bin/recon-ng <<EOF
#!/usr/bin/env bash
exec "${PYTHON_BIN}" "${RECONNG_DIR}/recon-ng" "\$@"
EOF
      chmod +x /usr/local/bin/recon-ng
    else
      warn "recon-ng executable not found after clone"
    fi
  else
    warn "failed to clone recon-ng repository"
  fi
fi

PHONEINFOGA_VERSION="${PHONEINFOGA_VERSION:-latest}"
log "installing phoneinfoga (${PHONEINFOGA_VERSION})"
PHONEINFOGA_DIR="/tmp/phoneinfoga"
mkdir -p "${PHONEINFOGA_DIR}"
PHONEINFOGA_ASSET_URL=""
if [ "${PHONEINFOGA_VERSION}" = "latest" ]; then
  PHONEINFOGA_RELEASE_JSON="$(curl -fsSL https://api.github.com/repos/sundowndev/phoneinfoga/releases/latest || true)"
  PHONEINFOGA_ASSET_URL="$(printf '%s' "${PHONEINFOGA_RELEASE_JSON}" | jq -r '.assets[]?.browser_download_url' | grep -Ei 'linux.*(amd64|x86_64).*\.(tar\.gz|zip)$' | head -n1 || true)"
else
  PHONEINFOGA_ASSET_URL="https://github.com/sundowndev/phoneinfoga/releases/download/${PHONEINFOGA_VERSION}/phoneinfoga_Linux_x86_64.tar.gz"
fi
if [ -n "${PHONEINFOGA_ASSET_URL}" ] && install_tar_or_zip_asset "${PHONEINFOGA_ASSET_URL}" "${PHONEINFOGA_DIR}"; then
  PHONEINFOGA_BIN="$(find "${PHONEINFOGA_DIR}" -maxdepth 6 -type f \( -name 'phoneinfoga' -o -name 'phoneinfoga*' \) | head -n1 || true)"
  if [ -n "${PHONEINFOGA_BIN}" ] && [ -f "${PHONEINFOGA_BIN}" ]; then
    chmod +x "${PHONEINFOGA_BIN}" || true
    install -m 0755 "${PHONEINFOGA_BIN}" /usr/local/bin/phoneinfoga
  else
    warn "phoneinfoga binary missing in archive"
  fi
else
  warn "failed to download/extract phoneinfoga release"
fi

SPIDERFOOT_DIR="/opt/spiderfoot"
log "installing spiderfoot from source"
if git clone --depth 1 https://github.com/smicallef/spiderfoot.git "${SPIDERFOOT_DIR}"; then
  if "${PIP_BIN}" install --no-cache-dir -r "${SPIDERFOOT_DIR}/requirements.txt"; then
    if [ -f "${SPIDERFOOT_DIR}/sf.py" ]; then
      cat >/usr/local/bin/sf.py <<EOF
#!/usr/bin/env bash
exec "${PYTHON_BIN}" "${SPIDERFOOT_DIR}/sf.py" "\$@"
EOF
      cat >/usr/local/bin/spiderfoot <<EOF
#!/usr/bin/env bash
exec "${PYTHON_BIN}" "${SPIDERFOOT_DIR}/sf.py" "\$@"
EOF
      chmod +x /usr/local/bin/spiderfoot /usr/local/bin/sf.py
    else
      warn "spiderfoot sf.py not found after clone"
    fi
  else
    warn "failed to install spiderfoot python dependencies"
  fi
else
  warn "failed to clone spiderfoot repository"
fi

CORE_TOOLS=(theHarvester amass recon-ng phoneinfoga spiderfoot sf.py)
MISSING=()
for tool in "${CORE_TOOLS[@]}"; do
  if command -v "${tool}" >/dev/null 2>&1; then
    log "verified: ${tool} -> $(command -v "${tool}")"
  else
    MISSING+=("${tool}")
    warn "missing after install: ${tool}"
  fi
done

rm -rf /var/lib/apt/lists/* /tmp/phoneinfoga /tmp/phoneinfoga.tar.gz /tmp/phoneinfoga_* /tmp/amass /tmp/amass.zip /tmp/asset_download /tmp/theharvester-deps.txt

if [ "${OSINT_STRICT_INSTALL}" = "1" ] && [ "${#MISSING[@]}" -gt 0 ]; then
  echo "[osint-install] error: required tools missing: ${MISSING[*]}" >&2
  exit 1
fi

log "done"
