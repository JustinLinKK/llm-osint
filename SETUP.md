# Setup Guide

This is the shortest setup path. The fuller repo reference now lives in [docs/WIKI.md](docs/WIKI.md).

## Prerequisites

- Docker + Docker Compose
- Node.js 20+
- Yarn 4
- Python 3.11+

## 1. Create Environment Files

```bash
cp .env.example .env
cp infra/docker/.env.example infra/docker/.env
```

Required for normal LangGraph planning:

```bash
OPENROUTER_API_KEY=your_key_here
```

Useful optional credentials:

```bash
TAVILY_API_KEY=
HIBP_API_KEY=
SHODAN_API_KEY=
LINKEDIN_EMAIL=
LINKEDIN_PASSWORD=
BROWSERBASE_API_KEY=
BROWSERBASE_PROJECT_ID=
X_BEARER_TOKEN=
```

If you are inside the dev container, keep Docker service names in `.env` and connect the container to the compose network once:

```bash
docker network connect docker_default $(hostname) || true
```

If you run locally on the host OS, replace service hostnames in `.env` with `localhost`.

## 2. Install Dependencies

```bash
yarn install
python3 -m venv .venv-agent
. .venv-agent/bin/activate
pip install -r services/agent-langgraph/requirements.txt
```

## 3. Start Infrastructure And Apply Migrations

```bash
yarn infra:up
yarn db:migrate
```

Useful variants:

```bash
yarn infra:ps
yarn infra:up:build
yarn infra:restart:all-lite
yarn infra:rebuild:all-lite
```

## 4. Start The UI

```bash
yarn dev:web
```

If you also want the API outside Docker in watch mode:

```bash
yarn dev:api
```

## 5. Verify The Stack

```bash
curl http://localhost:3000/health
curl -X POST http://localhost:3000/runs \
  -H "Content-Type: application/json" \
  -d '{"prompt":"Investigate example.com and related accounts"}'
```

Helpful follow-ups:

```bash
curl -N http://localhost:3000/runs/<RUN_ID>/events
curl http://localhost:3000/runs/<RUN_ID>/report
cd apps/mcp-server && yarn example
```

## References

- [docs/WIKI.md](docs/WIKI.md)
- [docs/ENVIRONMENT.md](docs/ENVIRONMENT.md)
- [apps/mcp-server/MCP_CLIENT_GUIDE.md](apps/mcp-server/MCP_CLIENT_GUIDE.md)
