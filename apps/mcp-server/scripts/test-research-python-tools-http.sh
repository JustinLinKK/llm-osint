#!/usr/bin/env bash
set -euo pipefail

MCP_URL="${MCP_SERVER_URL:-http://mcp-server:3001/mcp}"
PROTOCOL_VERSION="${MCP_PROTOCOL_VERSION:-2025-11-25}"
OUT_BASE="${RESEARCH_TOOL_OUTPUT_DIR:-apps/mcp-server/tmp/research-tool-responses}"
STAMP="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="${OUT_BASE}/${STAMP}"

mkdir -p "${OUT_DIR}"

gen_run_id() {
  python3 - <<'PY'
import uuid
print(uuid.uuid4())
PY
}

create_run() {
  local run_id="$1"
  local prompt="$2"
  docker exec -i docker-postgres-1 psql -U osint -d osint -v ON_ERROR_STOP=1 \
    -c "INSERT INTO runs(run_id, prompt, seeds, status) VALUES ('${run_id}', '${prompt}', '[]'::jsonb, 'created') ON CONFLICT (run_id) DO NOTHING;" \
    >/dev/null
}

initialize_session() {
  local init_headers
  local init_body
  init_headers="$(mktemp)"
  init_body="$(mktemp)"

  curl -sS -D "${init_headers}" -o "${init_body}" \
    -H 'content-type: application/json' \
    -H 'accept: application/json, text/event-stream' \
    -X POST "${MCP_URL}" \
    --data "{\"jsonrpc\":\"2.0\",\"id\":\"init-1\",\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"${PROTOCOL_VERSION}\",\"capabilities\":{},\"clientInfo\":{\"name\":\"research-tool-http-test\",\"version\":\"1.0.0\"}}}"

  SESSION_ID="$(grep -i '^mcp-session-id:' "${init_headers}" | head -n1 | cut -d' ' -f2 | tr -d '\r')"
  if [ -z "${SESSION_ID:-}" ]; then
    echo "Failed to initialize MCP session"
    echo "Headers:"
    cat "${init_headers}"
    echo "Body:"
    cat "${init_body}"
    exit 1
  fi

  curl -sS \
    -H 'content-type: application/json' \
    -H 'accept: application/json, text/event-stream' \
    -H "mcp-session-id: ${SESSION_ID}" \
    -H "mcp-protocol-version: ${PROTOCOL_VERSION}" \
    -X POST "${MCP_URL}" \
    --data '{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}' \
    >/dev/null
}

close_session() {
  if [ -n "${SESSION_ID:-}" ]; then
    curl -sS -X DELETE \
      -H 'content-type: application/json' \
      -H 'accept: application/json, text/event-stream' \
      -H "mcp-session-id: ${SESSION_ID}" \
      -H "mcp-protocol-version: ${PROTOCOL_VERSION}" \
      "${MCP_URL}" \
      >/dev/null || true
  fi
}

extract_text_to_file() {
  local in_json="$1"
  local out_text="$2"
  python3 - "$in_json" "$out_text" <<'PY'
import json
import sys

in_path = sys.argv[1]
out_path = sys.argv[2]

text = ""
with open(in_path, "r", encoding="utf-8") as f:
    payload = json.load(f)
    result = payload.get("result", {})
    content = result.get("content", [])
    if content and isinstance(content, list) and isinstance(content[0], dict):
        if content[0].get("type") == "text":
            text = content[0].get("text", "")
    if not text:
        text = json.dumps(payload, indent=2)

with open(out_path, "w", encoding="utf-8") as f:
    f.write(text)
    if not text.endswith("\n"):
        f.write("\n")
PY
}

call_tool() {
  local tool_name="$1"
  local args_json="$2"
  local run_id="$3"
  local out_json="${OUT_DIR}/${tool_name}.json"
  local out_text="${OUT_DIR}/${tool_name}.txt"

  curl -sS -o "${out_json}" \
    -H 'content-type: application/json' \
    -H 'accept: application/json, text/event-stream' \
    -H "mcp-session-id: ${SESSION_ID}" \
    -H "mcp-protocol-version: ${PROTOCOL_VERSION}" \
    -X POST "${MCP_URL}" \
    --data "{\"jsonrpc\":\"2.0\",\"id\":\"call-${tool_name}\",\"method\":\"tools/call\",\"params\":{\"name\":\"${tool_name}\",\"arguments\":${args_json}}}"

  extract_text_to_file "${out_json}" "${out_text}"
  echo "${tool_name}: runId=${run_id} -> ${out_text}"
}

SESSION_ID=""
trap close_session EXIT

echo "Initializing MCP session against ${MCP_URL}"
initialize_session

echo "Writing outputs to ${OUT_DIR}"

# 1) person_search
RUN_ID="$(gen_run_id)"
create_run "${RUN_ID}" "person_search http test"
call_tool "person_search" "{\"runId\":\"${RUN_ID}\",\"name\":\"Barbara Liskov\",\"max_results\":1,\"delay\":0.2,\"request_timeout\":8.0}" "${RUN_ID}"

# 2) x_get_user_posts_api
RUN_ID="$(gen_run_id)"
create_run "${RUN_ID}" "x_get_user_posts_api http test"
call_tool "x_get_user_posts_api" "{\"runId\":\"${RUN_ID}\",\"username\":\"openai\",\"max_results\":3}" "${RUN_ID}"

# 3) linkedin_download_html_ocr (cheap smoke-test path)
RUN_ID="$(gen_run_id)"
create_run "${RUN_ID}" "linkedin_download_html_ocr http test"
call_tool "linkedin_download_html_ocr" "{\"runId\":\"${RUN_ID}\",\"reset_session\":true}" "${RUN_ID}"

# 4) google_serp_person_search
RUN_ID="$(gen_run_id)"
create_run "${RUN_ID}" "google_serp_person_search http test"
call_tool "google_serp_person_search" "{\"runId\":\"${RUN_ID}\",\"target_name\":\"Sam Altman\",\"max_results\":3}" "${RUN_ID}"

# 5) arxiv_search_and_download (no pdf download for speed)
RUN_ID="$(gen_run_id)"
create_run "${RUN_ID}" "arxiv_search_and_download http test"
call_tool "arxiv_search_and_download" "{\"runId\":\"${RUN_ID}\",\"topic\":\"transformer\",\"max_results\":1,\"no_download\":true}" "${RUN_ID}"

echo
echo "Done. Output files:"
ls -1 "${OUT_DIR}"
echo
echo "Directory: ${OUT_DIR}"
