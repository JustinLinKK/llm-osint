#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
OSINT_VENV_DIR="${OSINT_VENV_DIR:-/opt/osint-venv}"

echo "[osint-install] apt-get update"
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
)

for pkg in "${APT_PACKAGES[@]}"; do
  echo "[osint-install] installing apt package: ${pkg}"
  if ! apt-get install -y --no-install-recommends "${pkg}"; then
    echo "[osint-install] warning: failed to install apt package ${pkg}" >&2
  fi
done

echo "[osint-install] creating virtualenv at ${OSINT_VENV_DIR}"
python3 -m venv "${OSINT_VENV_DIR}"

PIP_BIN="${OSINT_VENV_DIR}/bin/pip"
PYTHON_BIN="${OSINT_VENV_DIR}/bin/python"

echo "[osint-install] upgrading pip in virtualenv"
"${PYTHON_BIN}" -m pip install --no-cache-dir --upgrade pip setuptools wheel

PIP_PACKAGES=(
  sherlock-project
  maigret
  holehe
  theHarvester
  sublist3r
  spiderfoot
  recon-ng
  dnsdumpster
  shodan
)

for pkg in "${PIP_PACKAGES[@]}"; do
  echo "[osint-install] installing pip package: ${pkg}"
  if ! "${PIP_BIN}" install --no-cache-dir "${pkg}"; then
    echo "[osint-install] warning: failed to install pip package ${pkg}" >&2
  fi
done

PHONEINFOGA_VERSION="${PHONEINFOGA_VERSION:-v2.12.3}"
PHONEINFOGA_URL="https://github.com/sundowndev/phoneinfoga/releases/download/${PHONEINFOGA_VERSION}/phoneinfoga_Linux_x86_64.tar.gz"
echo "[osint-install] installing phoneinfoga from ${PHONEINFOGA_URL}"
if curl -fsSL "${PHONEINFOGA_URL}" -o /tmp/phoneinfoga.tar.gz; then
  tar -xzf /tmp/phoneinfoga.tar.gz -C /tmp
  if [ -f /tmp/phoneinfoga ]; then
    install -m 0755 /tmp/phoneinfoga /usr/local/bin/phoneinfoga
  else
    echo "[osint-install] warning: phoneinfoga binary missing in archive" >&2
  fi
else
  echo "[osint-install] warning: failed to download phoneinfoga release" >&2
fi

rm -rf /var/lib/apt/lists/* /tmp/phoneinfoga /tmp/phoneinfoga.tar.gz
echo "[osint-install] done"
