FROM mcr.microsoft.com/devcontainers/base:ubuntu-22.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl ca-certificates jq make unzip \
    && rm -rf /var/lib/apt/lists/*

# Optional: Python fast tooling (uv)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    ln -s /root/.local/bin/uv /usr/local/bin/uv
