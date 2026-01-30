FROM node:20-slim

WORKDIR /app

# Copy workspace files
COPY package.json yarn.lock .yarnrc.yml ./
COPY .yarn ./.yarn

# Copy apps/mcp-server
COPY apps/mcp-server ./apps/mcp-server

# Install dependencies
RUN yarn workspaces focus @osint/mcp-server --production

# Build
WORKDIR /app/apps/mcp-server
RUN yarn build

# Run
CMD ["node", "dist/index.js"]
