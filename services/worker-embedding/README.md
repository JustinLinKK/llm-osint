# worker-embedding

Local embedding service using vLLM's OpenAI-compatible `/v1/embeddings` API.

## Run

```bash
docker compose -f infra/docker/docker-compose.yml up -d worker-embedding
```

## Environment

```bash
EMBEDDING_PROVIDER=vllm
EMBEDDING_MODEL=Qwen/Qwen3-Embedding-0.6B
EMBEDDING_SERVED_MODEL_NAME=Qwen/Qwen3-Embedding-0.6B
EMBEDDING_PORT=8000
EMBEDDING_API_KEY=
EMBEDDING_TRUST_REMOTE_CODE=true
EMBEDDING_DTYPE=auto
EMBEDDING_TASK=embed
EMBEDDING_TENSOR_PARALLEL_SIZE=1
EMBEDDING_PIPELINE_PARALLEL_SIZE=1
EMBEDDING_GPU_MEMORY_UTILIZATION=0.9
EMBEDDING_ENFORCE_SINGLE_MODEL=true
EMBEDDING_VLLM_ARGS=
```

## API

- `POST /v1/embeddings`

## Notes

- This container uses `vllm/vllm-openai:latest`.
- The compose service now requests `gpus: all`, so it can use your GPU if Docker Engine and the NVIDIA container runtime are configured correctly on the host.
- The local worker is pinned to one served model. With `EMBEDDING_ENFORCE_SINGLE_MODEL=true`, startup fails if the served model name does not match `EMBEDDING_MODEL`.
- The rest of the pipeline can target this worker, OpenRouter, or any remote OpenAI-compatible endpoint via env.
