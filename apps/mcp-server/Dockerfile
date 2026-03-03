FROM node:20-slim

WORKDIR /app
ENV PATH="/opt/mcp-venv/bin:${PATH}"

# Python runtime for tool bridges
RUN apt-get update \
	&& apt-get install -y --no-install-recommends python3 python3-pip python3-venv ca-certificates \
	&& update-ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

# Enable Corepack to use Yarn 4.x
RUN corepack enable && corepack prepare yarn@4.6.0 --activate

# Copy workspace files
COPY package.json yarn.lock .yarnrc.yml ./
COPY .yarn ./.yarn

# Copy apps/mcp-server
COPY apps/mcp-server ./apps/mcp-server

# Install Python dependencies for MCP Python tools in an isolated venv,
# then provision the Playwright browser runtime used by LinkedIn capture.
RUN python3 -m venv /opt/mcp-venv \
	&& /opt/mcp-venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel \
	&& /opt/mcp-venv/bin/pip install --no-cache-dir \
	-r /app/apps/mcp-server/src/tools/tools_python/person_search/requirements.txt \
	-r /app/apps/mcp-server/src/tools/tools_python/research_integration/requirements.txt \
	&& /opt/mcp-venv/bin/python -m playwright install --with-deps chromium

# Install dependencies (including dev dependencies for build)
RUN yarn workspaces focus @osint/mcp-server

# Build
WORKDIR /app/apps/mcp-server
RUN yarn build

# Clean up and keep only production dependencies
WORKDIR /app
RUN yarn workspaces focus @osint/mcp-server --production

# Run
CMD ["node", "apps/mcp-server/dist/index.js"]
