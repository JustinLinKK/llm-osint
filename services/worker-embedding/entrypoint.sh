#!/usr/bin/env bash
set -euo pipefail

MODEL="${EMBEDDING_MODEL:-Qwen/Qwen3-Embedding-0.6B}"
SERVED_MODEL_NAME="${EMBEDDING_SERVED_MODEL_NAME:-${MODEL}}"
HOST="${EMBEDDING_HOST:-0.0.0.0}"
PORT="${EMBEDDING_PORT:-8000}"
API_KEY="${EMBEDDING_API_KEY:-}"
TRUST_REMOTE_CODE="${EMBEDDING_TRUST_REMOTE_CODE:-true}"
DTYPE="${EMBEDDING_DTYPE:-auto}"
MAX_MODEL_LEN="${EMBEDDING_MAX_MODEL_LEN:-}"
TENSOR_PARALLEL_SIZE="${EMBEDDING_TENSOR_PARALLEL_SIZE:-1}"
PIPELINE_PARALLEL_SIZE="${EMBEDDING_PIPELINE_PARALLEL_SIZE:-1}"
GPU_MEMORY_UTILIZATION="${EMBEDDING_GPU_MEMORY_UTILIZATION:-0.9}"
ENFORCE_SINGLE_MODEL="${EMBEDDING_ENFORCE_SINGLE_MODEL:-true}"
EXTRA_ARGS="${EMBEDDING_VLLM_ARGS:-}"

if [[ "${ENFORCE_SINGLE_MODEL}" == "true" ]]; then
  if [[ "${MODEL}" == *,* ]]; then
    echo "EMBEDDING_MODEL must be a single model identifier" >&2
    exit 1
  fi
  if [[ "${SERVED_MODEL_NAME}" != "${MODEL}" ]]; then
    echo "EMBEDDING_SERVED_MODEL_NAME must match EMBEDDING_MODEL when single-model mode is enabled" >&2
    exit 1
  fi
fi

ARGS=(
  serve
  "${MODEL}"
  --served-model-name "${SERVED_MODEL_NAME}"
  --host "${HOST}"
  --port "${PORT}"
  --dtype "${DTYPE}"
  --tensor-parallel-size "${TENSOR_PARALLEL_SIZE}"
  --pipeline-parallel-size "${PIPELINE_PARALLEL_SIZE}"
  --gpu-memory-utilization "${GPU_MEMORY_UTILIZATION}"
)

if [[ -n "${API_KEY}" ]]; then
  ARGS+=(--api-key "${API_KEY}")
fi

if [[ "${TRUST_REMOTE_CODE}" == "true" ]]; then
  ARGS+=(--trust-remote-code)
fi

if [[ -n "${MAX_MODEL_LEN}" ]]; then
  ARGS+=(--max-model-len "${MAX_MODEL_LEN}")
fi

if [[ -n "${EXTRA_ARGS}" ]]; then
  read -r -a EXTRA <<< "${EXTRA_ARGS}"
  ARGS+=("${EXTRA[@]}")
fi

exec vllm "${ARGS[@]}"
