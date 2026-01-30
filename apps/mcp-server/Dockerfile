FROM node:20-slim

WORKDIR /app

# Enable Corepack to use Yarn 4.x
RUN corepack enable && corepack prepare yarn@4.6.0 --activate

# Copy workspace files
COPY package.json yarn.lock .yarnrc.yml ./
COPY .yarn ./.yarn

# Copy apps/mcp-server
COPY apps/mcp-server ./apps/mcp-server

# Install dependencies (including dev dependencies for build)
RUN yarn workspaces focus @osint/mcp-server

# Build
WORKDIR /app/apps/mcp-server
RUN yarn build

# Clean up and keep only production dependencies
WORKDIR /app
RUN yarn workspaces focus @osint/mcp-server --production

# Run
CMD ["node", "dist/index.js"]
