#!/usr/bin/env python3
"""
LLM-based knowledge graph construction with cleanup-focused merging.

Pipeline:
1) One-stage extraction (entities + relations) from all input articles.
2) Exact deduplication over normalized names.
3) Sequential LLM-guided entity merge with hybrid candidate retrieval:
   - top-k1 by embedding cosine similarity
   - top-k2 by fast string-overlap index
4) Relation merge using the same strategy.
5) Persist final graph JSON (nodes + edges + metadata).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import random
import re
import uuid
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import aiohttp
import numpy as np

try:
    from transformers import AutoTokenizer  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    AutoTokenizer = None


DEFAULT_BASE_URL = "https://localllm.frederickpi.com"
DEFAULT_LLM_MODEL = "Qwen/Qwen3-32B"
DEFAULT_EMBED_MODEL = "qwen3-embed-0.6b"
DEFAULT_API_KEY = "not-needed"
DEFAULT_CONTEXT_LIMIT = 15536
DEFAULT_INPUT_TOKENS = 13500
DEFAULT_OUTPUT_TOKENS = 1200
DEFAULT_MERGE_OUTPUT_TOKENS = 800
DEFAULT_MAX_RETRIES = 5
DEFAULT_LLM_CONCURRENCY = 16
DEFAULT_EMBED_CONCURRENCY = 16
DEFAULT_EMBED_BATCH_SIZE = 64
DEFAULT_K1 = 30
DEFAULT_K2 = 30
DEFAULT_STRING_NGRAM = 3
DEFAULT_MERGE_PROMPT_CHAR_LIMIT = 42000

NO_THINK_SUFFIX = "\n \\no_think \n"
THINK_PATTERN = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)
NON_WORD_PATTERN = re.compile(r"[\W_]+", re.UNICODE)

EXTRACTION_SYSTEM_PROMPT = (
    "You extract entities and relations from text for open-domain knowledge graph construction. "
    "Do not assume a closed ontology. Entity types and relation types should be descriptive text. "
    "Return only JSON with this shape: "
    "{"
    '"entities": [{"canonical_name": str, "type": str, "alt_names": [str], "attributes": [str]}], '
    '"relations": [{"src": str, "dst": str, "canonical_name": str, "rel_type": str, "alt_names": [str]}]'
    "}. "
    "Do not include IDs, timestamps, or metadata fields not requested."
)

ENTITY_MERGE_SYSTEM_PROMPT = (
    "You decide entity coreference only between a query entity and provided candidates. "
    "Do not decide candidate-candidate merges. "
    "Return only JSON: "
    '{"merge_with_ids":[str], "canonical_name":str, "type":str, "attributes":[str]}. '
    "merge_with_ids must be a subset of candidate IDs that refer to the same entity as query."
)

RELATION_MERGE_SYSTEM_PROMPT = (
    "You decide relation coreference only between a query relation and provided candidates. "
    "Do not decide candidate-candidate merges. "
    "Return only JSON: "
    '{"merge_with_ids":[str], "canonical_name":str, "rel_type":str, "src_id":str, "dst_id":str}. '
    "merge_with_ids must be a subset of candidate IDs that refer to the same relation as query."
)


@dataclass
class Article:
    article_id: str
    text: str


@dataclass
class ExtractedEntity:
    canonical_name: str
    entity_type: str = ""
    alt_names: List[str] = field(default_factory=list)
    attributes: List[str] = field(default_factory=list)


@dataclass
class ExtractedRelation:
    src_name: str
    dst_name: str
    canonical_name: str
    rel_type: str = ""
    alt_names: List[str] = field(default_factory=list)


@dataclass
class GraphNode:
    node_id: str
    embedding: List[float]
    type: str
    alt_names: List[str]
    created_at: str
    updated_at: str
    attributes: List[str]
    canonical_name: str

    def all_names(self) -> List[str]:
        return unique_strings([self.canonical_name, *self.alt_names])

    def as_dict(self, include_embedding: bool = True) -> Dict[str, Any]:
        payload = {
            "node_id": self.node_id,
            "type": self.type,
            "alt_names": self.alt_names,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "attributes": self.attributes,
            "canonical_name": self.canonical_name,
        }
        if include_embedding:
            payload["embedding"] = self.embedding
        return payload

    def as_embedding_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "embedding": self.embedding,
        }


@dataclass
class GraphEdge:
    edge_id: str
    src_id: str
    dst_id: str
    rel_type: str
    created_at: str
    updated_at: str
    canonical_name: str
    alt_names: List[str]
    runtime_embedding: List[float] = field(default_factory=list, repr=False)

    def all_names(self) -> List[str]:
        return unique_strings([self.canonical_name, *self.alt_names])

    def as_dict(self) -> Dict[str, Any]:
        return {
            "edge_id": self.edge_id,
            "src_id": self.src_id,
            "dst_id": self.dst_id,
            "rel_type": self.rel_type,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "canonical_name": self.canonical_name,
            "alt_names": self.alt_names,
        }

    def as_embedding_dict(self) -> Dict[str, Any]:
        return {
            "edge_id": self.edge_id,
            "embedding": self.runtime_embedding,
        }


@dataclass
class EntityMergeDecision:
    merge_with_ids: List[str]
    canonical_name: str
    merged_type: str
    attributes: List[str]


@dataclass
class RelationMergeDecision:
    merge_with_ids: List[str]
    canonical_name: str
    rel_type: str
    src_id: str
    dst_id: str


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def backoff_seconds(attempt: int) -> float:
    return min(12.0, (2 ** attempt) + random.random() * 0.25)


def strip_think(text: str) -> str:
    return THINK_PATTERN.sub("", text).strip()


def normalize_name(text: str) -> str:
    lowered = text.lower().strip()
    lowered = NON_WORD_PATTERN.sub(" ", lowered)
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip()


def unique_strings(items: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    output: List[str] = []
    for item in items:
        value = str(item or "").strip()
        if not value:
            continue
        key = normalize_name(value)
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(value)
    return output


def choose_canonical_name(names: Iterable[str]) -> str:
    candidates = unique_strings(names)
    if not candidates:
        return "unknown"
    candidates.sort(key=lambda x: (len(x), x), reverse=True)
    return candidates[0]


def clip_text(text: str, limit: int = 180) -> str:
    value = str(text or "").strip()
    if len(value) <= limit:
        return value
    return value[: limit - 3].rstrip() + "..."


def clip_string_list(values: Iterable[str], item_limit: int = 12, char_limit: int = 120) -> List[str]:
    output: List[str] = []
    for value in unique_strings(values):
        output.append(clip_text(value, limit=char_limit))
        if len(output) >= item_limit:
            break
    return output


def fit_payload_char_limit(payload: Dict[str, Any], char_limit: int) -> Dict[str, Any]:
    candidates = list(payload.get("candidates") or [])
    if not isinstance(candidates, list) or len(json.dumps(payload, ensure_ascii=False)) <= char_limit:
        return payload

    # Trim candidate list progressively if prompt is too large.
    while candidates and len(json.dumps(payload, ensure_ascii=False)) > char_limit:
        next_size = max(1, len(candidates) // 2)
        candidates = candidates[:next_size]
        payload["candidates"] = candidates
        if next_size == 1:
            break
    return payload


def safe_json_load(text: str) -> Any:
    stripped = text.strip()
    stripped = re.sub(r"^\s*```(?:json)?\s*", "", stripped, flags=re.IGNORECASE)
    stripped = re.sub(r"\s*```\s*$", "", stripped)

    candidates = [stripped]
    for open_ch, close_ch in (("{", "}"), ("[", "]")):
        start = stripped.find(open_ch)
        end = stripped.rfind(close_ch)
        if start != -1 and end != -1 and end > start:
            candidates.append(stripped[start : end + 1])

    last_error: Optional[Exception] = None
    for candidate in candidates:
        try:
            return json.loads(candidate)
        except Exception as exc:  # noqa: PERF203
            last_error = exc
    raise ValueError(f"Unable to parse JSON payload: {last_error}")


class QwenTokenizer:
    def __init__(self, model_name: str = DEFAULT_LLM_MODEL, enabled: bool = True) -> None:
        self.model_name = model_name
        self.enabled = enabled
        self._tokenizer = None
        self._load_failed = False

    def _get(self):
        if not self.enabled:
            return None
        if self._tokenizer is not None:
            return self._tokenizer
        if self._load_failed:
            return None
        if AutoTokenizer is None:
            self._load_failed = True
            return None
        try:
            self._tokenizer = AutoTokenizer.from_pretrained(self.model_name, trust_remote_code=True)
        except Exception as exc:  # pragma: no cover - depends on local env
            logging.warning("Qwen tokenizer unavailable; using fallback truncation: %s", exc)
            self._load_failed = True
            return None
        return self._tokenizer

    def truncate(self, text: str, max_tokens: int) -> str:
        if max_tokens <= 0:
            raise ValueError("max_tokens must be positive")
        tok = self._get()
        if tok is None:
            # Conservative fallback when tokenizer is unavailable.
            return text[: max_tokens * 4]
        token_ids = tok.encode(text, add_special_tokens=False)
        if len(token_ids) <= max_tokens:
            return text
        return tok.decode(token_ids[:max_tokens], skip_special_tokens=True)


class QwenClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        llm_model: str,
        embed_model: str,
        llm_semaphore: asyncio.Semaphore,
        embed_semaphore: asyncio.Semaphore,
        max_retries: int = DEFAULT_MAX_RETRIES,
        timeout_seconds: int = 90,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.llm_model = llm_model
        self.embed_model = embed_model
        self.llm_semaphore = llm_semaphore
        self.embed_semaphore = embed_semaphore
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "QwenClient":
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        self._session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _post_json(
        self,
        path: str,
        payload: Dict[str, Any],
        semaphore: asyncio.Semaphore,
    ) -> Dict[str, Any]:
        if self._session is None:
            raise RuntimeError("Client session not initialized")
        url = f"{self.base_url}{path}"
        headers = {"Authorization": f"Bearer {self.api_key}"}

        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                async with semaphore:
                    async with self._session.post(url, headers=headers, json=payload) as resp:
                        text = await resp.text()
                        if resp.status != 200:
                            raise RuntimeError(f"HTTP {resp.status}: {text[:800]}")
                        return json.loads(text)
            except Exception as exc:  # noqa: PERF203
                last_error = exc
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(backoff_seconds(attempt))
        raise RuntimeError(f"Request failed after retries: {last_error}")

    async def chat(self, system_prompt: str, user_prompt: str, max_tokens: int) -> str:
        payload = {
            "model": self.llm_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"{user_prompt}{NO_THINK_SUFFIX}"},
            ],
            "temperature": 0.0,
            "max_tokens": max_tokens,
        }
        data = await self._post_json("/v1/chat/completions", payload, semaphore=self.llm_semaphore)
        raw = data["choices"][0]["message"]["content"]
        return strip_think(str(raw))

    async def chat_json(self, system_prompt: str, user_prompt: str, max_tokens: int) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(DEFAULT_MAX_RETRIES):
            try:
                text = await self.chat(system_prompt=system_prompt, user_prompt=user_prompt, max_tokens=max_tokens)
                return safe_json_load(strip_think(text))
            except Exception as exc:  # noqa: PERF203
                last_error = exc
                if attempt < DEFAULT_MAX_RETRIES - 1:
                    await asyncio.sleep(backoff_seconds(attempt))
        raise RuntimeError(f"Failed to parse JSON response after retries: {last_error}")

    async def embed_texts(self, texts: Sequence[str], batch_size: int = DEFAULT_EMBED_BATCH_SIZE) -> List[List[float]]:
        if not texts:
            return []
        indexed_batches: List[Tuple[List[int], List[str]]] = []
        for start in range(0, len(texts), batch_size):
            batch_texts = list(texts[start : start + batch_size])
            idxs = list(range(start, start + len(batch_texts)))
            indexed_batches.append((idxs, batch_texts))

        async def embed_batch(indices: List[int], batch_texts: List[str]) -> Tuple[List[int], List[List[float]]]:
            payload = {"model": self.embed_model, "input": batch_texts}
            data = await self._post_json("/v1/embeddings", payload, semaphore=self.embed_semaphore)
            vectors = [entry["embedding"] for entry in data.get("data", [])]
            if len(vectors) != len(batch_texts):
                raise RuntimeError(
                    f"Embedding size mismatch: expected {len(batch_texts)}, got {len(vectors)}"
                )
            return indices, vectors

        tasks = [embed_batch(indices, batch_texts) for indices, batch_texts in indexed_batches]
        results = await asyncio.gather(*tasks)
        out: List[Optional[List[float]]] = [None] * len(texts)
        for indices, vectors in results:
            for idx, vec in zip(indices, vectors):
                out[idx] = [float(x) for x in vec]
        if any(vec is None for vec in out):
            raise RuntimeError("Missing embeddings after batch collection")
        return [vec for vec in out if vec is not None]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Construct and clean a KG from article text.")
    parser.add_argument("--articles", required=True, help="Input path (.jsonl or .json).")
    parser.add_argument("--output", required=True, help="Output graph JSON path.")
    parser.add_argument(
        "--embeddings-output",
        default=None,
        help="Optional output path for embedding vectors JSON (default: <output>.embeddings.json).",
    )
    parser.add_argument(
        "--text-key",
        default="text",
        help="Preferred article text key in JSON objects (fallback keys are also tried).",
    )
    parser.add_argument(
        "--existing-graph",
        default=None,
        help="Optional existing graph JSON to merge with new extraction batch.",
    )
    parser.add_argument(
        "--existing-embeddings",
        default=None,
        help="Optional embedding vectors JSON for --existing-graph.",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="OpenAI-compatible endpoint base URL.")
    parser.add_argument("--api-key", default=DEFAULT_API_KEY, help="API key.")
    parser.add_argument("--llm-model", default=DEFAULT_LLM_MODEL, help="LLM model ID.")
    parser.add_argument("--embed-model", default=DEFAULT_EMBED_MODEL, help="Embedding model ID.")
    parser.add_argument("--k1", type=int, default=DEFAULT_K1, help="Top-k for embedding similarity candidates.")
    parser.add_argument("--k2", type=int, default=DEFAULT_K2, help="Top-k for string similarity candidates.")
    parser.add_argument(
        "--llm-concurrency",
        type=int,
        default=DEFAULT_LLM_CONCURRENCY,
        help="Concurrency semaphore for LLM calls.",
    )
    parser.add_argument(
        "--embed-concurrency",
        type=int,
        default=DEFAULT_EMBED_CONCURRENCY,
        help="Concurrency semaphore for embedding calls.",
    )
    parser.add_argument(
        "--embed-batch-size",
        type=int,
        default=DEFAULT_EMBED_BATCH_SIZE,
        help="Embedding batch size.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help="Max retries for LLM/embedding request failures.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=90,
        help="HTTP timeout per request in seconds.",
    )
    parser.add_argument(
        "--context-limit",
        type=int,
        default=DEFAULT_CONTEXT_LIMIT,
        help="Model context limit for truncation logic.",
    )
    parser.add_argument(
        "--extract-input-tokens",
        type=int,
        default=DEFAULT_INPUT_TOKENS,
        help="Max input tokens (approx) for extraction prompts.",
    )
    parser.add_argument(
        "--extract-output-tokens",
        type=int,
        default=DEFAULT_OUTPUT_TOKENS,
        help="Max output tokens for extraction responses.",
    )
    parser.add_argument(
        "--merge-output-tokens",
        type=int,
        default=DEFAULT_MERGE_OUTPUT_TOKENS,
        help="Max output tokens for merge-decision responses.",
    )
    parser.add_argument(
        "--disable-qwen-tokenizer",
        action="store_true",
        help="Disable Qwen tokenizer loading and use fallback truncation.",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level.")
    return parser.parse_args()


def load_articles(path: Path, text_key: str) -> List[Article]:
    if not path.exists():
        raise FileNotFoundError(f"Input not found: {path}")

    def pick_text(obj: Dict[str, Any]) -> str:
        candidates = [text_key, "text", "content", "article", "body", "summary"]
        for key in candidates:
            value = obj.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    rows: List[Dict[str, Any]] = []
    if path.suffix.lower() == ".jsonl":
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
    elif path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            rows = [row for row in payload if isinstance(row, dict)]
        elif isinstance(payload, dict):
            if isinstance(payload.get("articles"), list):
                rows = [row for row in payload["articles"] if isinstance(row, dict)]
            else:
                rows = [payload]
        else:
            raise ValueError("JSON payload must be an object or array of objects.")
    else:
        raise ValueError("Unsupported input extension. Use .jsonl or .json")

    articles: List[Article] = []
    for idx, row in enumerate(rows, start=1):
        text = pick_text(row)
        if not text:
            continue
        article_id = str(row.get("id") or row.get("article_id") or f"article-{idx}")
        articles.append(Article(article_id=article_id, text=text))
    return articles


def default_embeddings_path(graph_path: Path) -> Path:
    if graph_path.suffix:
        return graph_path.with_suffix(".embeddings.json")
    return Path(f"{graph_path}.embeddings.json")


def _to_float_vector(raw: Any) -> List[float]:
    if not isinstance(raw, list):
        return []
    out: List[float] = []
    for item in raw:
        try:
            out.append(float(item))
        except Exception:
            return []
    return out


def load_embeddings_file(path: Optional[Path], *, required: bool = False) -> Tuple[Dict[str, List[float]], Dict[str, List[float]]]:
    if path is None:
        return {}, {}
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Embeddings path not found: {path}")
        return {}, {}

    payload = json.loads(path.read_text(encoding="utf-8"))
    node_map: Dict[str, List[float]] = {}
    edge_map: Dict[str, List[float]] = {}

    if isinstance(payload, dict):
        node_rows = payload.get("node_embeddings")
        edge_rows = payload.get("edge_embeddings")

        if isinstance(node_rows, dict):
            for node_id, vector in node_rows.items():
                vec = _to_float_vector(vector)
                if vec:
                    node_map[str(node_id)] = vec
        elif isinstance(node_rows, list):
            for row in node_rows:
                if not isinstance(row, dict):
                    continue
                node_id = str(row.get("node_id") or "").strip()
                vector = _to_float_vector(row.get("embedding"))
                if node_id and vector:
                    node_map[node_id] = vector

        if isinstance(edge_rows, dict):
            for edge_id, vector in edge_rows.items():
                vec = _to_float_vector(vector)
                if vec:
                    edge_map[str(edge_id)] = vec
        elif isinstance(edge_rows, list):
            for row in edge_rows:
                if not isinstance(row, dict):
                    continue
                edge_id = str(row.get("edge_id") or "").strip()
                vector = _to_float_vector(row.get("embedding"))
                if edge_id and vector:
                    edge_map[edge_id] = vector

    return node_map, edge_map


def load_existing_graph(
    path: Optional[Path],
    embeddings_path: Optional[Path] = None,
    embeddings_required: bool = False,
) -> Tuple[List[GraphNode], List[GraphEdge]]:
    if path is None:
        return [], []
    if not path.exists():
        raise FileNotFoundError(f"Existing graph path not found: {path}")

    embedding_nodes, embedding_edges = load_embeddings_file(embeddings_path, required=embeddings_required)
    payload = json.loads(path.read_text(encoding="utf-8"))
    nodes_data = payload.get("nodes", []) if isinstance(payload, dict) else []
    edges_data = payload.get("edges", []) if isinstance(payload, dict) else []

    nodes: List[GraphNode] = []
    for row in nodes_data:
        if not isinstance(row, dict):
            continue
        node_id = str(row.get("node_id") or f"ent_{uuid.uuid4().hex}")
        inline_embedding = _to_float_vector(row.get("embedding"))
        chosen_embedding = embedding_nodes.get(node_id) or inline_embedding
        node = GraphNode(
            node_id=node_id,
            embedding=chosen_embedding,
            type=str(row.get("type") or ""),
            alt_names=unique_strings(row.get("alt_names") or []),
            created_at=str(row.get("created_at") or utc_now()),
            updated_at=str(row.get("updated_at") or utc_now()),
            attributes=unique_strings(row.get("attributes") or []),
            canonical_name=str(row.get("canonical_name") or "unknown"),
        )
        nodes.append(node)

    edges: List[GraphEdge] = []
    for row in edges_data:
        if not isinstance(row, dict):
            continue
        edge_id = str(row.get("edge_id") or f"rel_{uuid.uuid4().hex}")
        edge = GraphEdge(
            edge_id=edge_id,
            src_id=str(row.get("src_id") or ""),
            dst_id=str(row.get("dst_id") or ""),
            rel_type=str(row.get("rel_type") or ""),
            created_at=str(row.get("created_at") or utc_now()),
            updated_at=str(row.get("updated_at") or utc_now()),
            canonical_name=str(row.get("canonical_name") or "related_to"),
            alt_names=unique_strings(row.get("alt_names") or []),
            runtime_embedding=embedding_edges.get(edge_id, []),
        )
        if edge.src_id and edge.dst_id:
            edges.append(edge)
    return nodes, edges


def to_extracted_entities(raw_entities: Any) -> List[ExtractedEntity]:
    if not isinstance(raw_entities, list):
        return []
    entities: List[ExtractedEntity] = []
    for row in raw_entities:
        if not isinstance(row, dict):
            continue
        canonical = str(row.get("canonical_name") or row.get("name") or "").strip()
        alt_names = unique_strings(row.get("alt_names") or row.get("aliases") or [])
        if not canonical:
            if alt_names:
                canonical = alt_names[0]
            else:
                continue
        entity_type = str(row.get("type") or row.get("entity_type") or "").strip()
        attributes = unique_strings(row.get("attributes") or row.get("props") or [])
        entities.append(
            ExtractedEntity(
                canonical_name=canonical,
                entity_type=entity_type,
                alt_names=[name for name in alt_names if normalize_name(name) != normalize_name(canonical)],
                attributes=attributes,
            )
        )
    return entities


def _to_relation_name(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("canonical_name", "name", "entity", "text"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
    return ""


def to_extracted_relations(raw_relations: Any) -> List[ExtractedRelation]:
    if not isinstance(raw_relations, list):
        return []
    relations: List[ExtractedRelation] = []
    for row in raw_relations:
        if not isinstance(row, dict):
            continue
        src = _to_relation_name(row.get("src") or row.get("source"))
        dst = _to_relation_name(row.get("dst") or row.get("target"))
        canonical = str(row.get("canonical_name") or row.get("name") or "").strip()
        rel_type = str(row.get("rel_type") or row.get("type") or "").strip()
        alt_names = unique_strings(row.get("alt_names") or row.get("aliases") or [])
        if not src or not dst:
            continue
        if not canonical:
            canonical = rel_type or f"{src} -> {dst}"
        relations.append(
            ExtractedRelation(
                src_name=src,
                dst_name=dst,
                canonical_name=canonical,
                rel_type=rel_type,
                alt_names=[name for name in alt_names if normalize_name(name) != normalize_name(canonical)],
            )
        )
    return relations


def parse_extraction_payload(payload: Any) -> Tuple[List[ExtractedEntity], List[ExtractedRelation]]:
    if not isinstance(payload, dict):
        return [], []
    entities = to_extracted_entities(payload.get("entities"))
    relations = to_extracted_relations(payload.get("relations"))
    return entities, relations


def make_article_prompt(article: Article, tokenizer: QwenTokenizer, max_input_tokens: int) -> str:
    body = tokenizer.truncate(article.text, max_input_tokens)
    return (
        "Extract entities and relations from the following article.\n"
        "Rules:\n"
        "- Keep names faithful to source text.\n"
        "- alt_names should contain aliases or variants if available.\n"
        "- attributes should be short descriptive strings.\n"
        "- rel_type can be empty when unclear.\n"
        "- Return valid JSON only.\n\n"
        f"Article ID: {article.article_id}\n"
        f"Article Text:\n{body}"
    )


async def extract_all(
    articles: Sequence[Article],
    client: QwenClient,
    tokenizer: QwenTokenizer,
    max_input_tokens: int,
    max_output_tokens: int,
) -> Tuple[List[ExtractedEntity], List[ExtractedRelation], int]:
    entities: List[ExtractedEntity] = []
    relations: List[ExtractedRelation] = []
    failures = 0

    async def run_one(article: Article) -> Tuple[List[ExtractedEntity], List[ExtractedRelation]]:
        prompt = make_article_prompt(article, tokenizer=tokenizer, max_input_tokens=max_input_tokens)
        payload = await client.chat_json(
            system_prompt=EXTRACTION_SYSTEM_PROMPT,
            user_prompt=prompt,
            max_tokens=max_output_tokens,
        )
        return parse_extraction_payload(payload)

    tasks = [run_one(article) for article in articles]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for article, result in zip(articles, results):
        if isinstance(result, Exception):
            logging.warning("Extraction failed for %s: %s", article.article_id, result)
            failures += 1
            continue
        ent, rel = result
        entities.extend(ent)
        relations.extend(rel)
    return entities, relations, failures


def dedupe_entities_exact(drafts: Sequence[ExtractedEntity]) -> List[GraphNode]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for draft in drafts:
        all_names = unique_strings([draft.canonical_name, *draft.alt_names])
        if not all_names:
            continue
        key = normalize_name(draft.canonical_name or all_names[0])
        if not key:
            continue
        bucket = grouped.setdefault(
            key,
            {
                "names": [],
                "type_counter": Counter(),
                "attributes": [],
            },
        )
        bucket["names"].extend(all_names)
        if draft.entity_type.strip():
            bucket["type_counter"][draft.entity_type.strip()] += 1
        bucket["attributes"].extend(unique_strings(draft.attributes))

    now = utc_now()
    nodes: List[GraphNode] = []
    for bucket in grouped.values():
        names = unique_strings(bucket["names"])
        canonical = choose_canonical_name(names)
        alt_names = [name for name in names if normalize_name(name) != normalize_name(canonical)]
        entity_type = ""
        if bucket["type_counter"]:
            entity_type = bucket["type_counter"].most_common(1)[0][0]
        node = GraphNode(
            node_id=f"ent_{uuid.uuid4().hex}",
            embedding=[],
            type=entity_type,
            alt_names=alt_names,
            created_at=now,
            updated_at=now,
            attributes=unique_strings(bucket["attributes"]),
            canonical_name=canonical,
        )
        nodes.append(node)
    return nodes


def build_entity_embedding_text(node: GraphNode) -> str:
    names = unique_strings([node.canonical_name, *node.alt_names])
    return " | ".join(names)


def build_relation_embedding_text(edge: GraphEdge, node_lookup: Dict[str, GraphNode]) -> str:
    src = node_lookup.get(edge.src_id)
    dst = node_lookup.get(edge.dst_id)
    src_name = src.canonical_name if src else edge.src_id
    dst_name = dst.canonical_name if dst else edge.dst_id
    names = unique_strings([edge.canonical_name, *edge.alt_names])
    return f"{src_name} | {edge.rel_type} | {' | '.join(names)} | {dst_name}".strip()


async def ensure_entity_embeddings(nodes: Sequence[GraphNode], client: QwenClient, batch_size: int) -> None:
    pending_indices = [idx for idx, node in enumerate(nodes) if not node.embedding]
    if not pending_indices:
        return
    texts = [build_entity_embedding_text(nodes[idx]) for idx in pending_indices]
    vectors = await client.embed_texts(texts, batch_size=batch_size)
    for idx, vector in zip(pending_indices, vectors):
        nodes[idx].embedding = vector


async def ensure_relation_embeddings(
    edges: Sequence[GraphEdge],
    node_lookup: Dict[str, GraphNode],
    client: QwenClient,
    batch_size: int,
) -> None:
    pending_indices = [idx for idx, edge in enumerate(edges) if not edge.runtime_embedding]
    if not pending_indices:
        return
    texts = [build_relation_embedding_text(edges[idx], node_lookup=node_lookup) for idx in pending_indices]
    vectors = await client.embed_texts(texts, batch_size=batch_size)
    for idx, vector in zip(pending_indices, vectors):
        edges[idx].runtime_embedding = vector


def char_ngrams(text: str, n: int = DEFAULT_STRING_NGRAM) -> Set[str]:
    clean = normalize_name(text).replace(" ", "")
    if not clean:
        return set()
    if len(clean) < n:
        return {clean}
    return {clean[i : i + n] for i in range(0, len(clean) - n + 1)}


class StringSimilarityIndex:
    def __init__(self, id_to_text: Dict[str, str], ngram: int = DEFAULT_STRING_NGRAM) -> None:
        self.ngram = ngram
        self.id_to_ngrams: Dict[str, Set[str]] = {}
        self.postings: Dict[str, Set[str]] = defaultdict(set)
        for item_id, text in id_to_text.items():
            grams = char_ngrams(text, n=self.ngram)
            self.id_to_ngrams[item_id] = grams
            for gram in grams:
                self.postings[gram].add(item_id)

    def top_k(self, query_id: str, k: int) -> List[str]:
        query_grams = self.id_to_ngrams.get(query_id, set())
        if not query_grams:
            return []
        candidate_overlap: Counter[str] = Counter()
        for gram in query_grams:
            for candidate in self.postings.get(gram, set()):
                if candidate == query_id:
                    continue
                candidate_overlap[candidate] += 1
        scored: List[Tuple[float, str]] = []
        for candidate, overlap in candidate_overlap.items():
            candidate_grams = self.id_to_ngrams.get(candidate, set())
            union_size = len(query_grams | candidate_grams)
            if union_size == 0:
                continue
            score = overlap / union_size
            scored.append((score, candidate))
        scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
        return [candidate for _, candidate in scored[:k]]


def cosine_similarity(vec_a: Sequence[float], vec_b: Sequence[float]) -> float:
    if not vec_a or not vec_b:
        return float("-inf")
    if len(vec_a) != len(vec_b):
        return float("-inf")
    arr_a = np.asarray(vec_a, dtype=np.float32)
    arr_b = np.asarray(vec_b, dtype=np.float32)
    denom = float(np.linalg.norm(arr_a) * np.linalg.norm(arr_b))
    if denom <= 0:
        return float("-inf")
    return float(np.dot(arr_a, arr_b) / denom)


def top_k_embedding_candidates_entities(query_id: str, nodes: Dict[str, GraphNode], k: int) -> List[str]:
    query = nodes.get(query_id)
    if query is None or not query.embedding:
        return []
    scores: List[Tuple[float, str]] = []
    for candidate_id, candidate in nodes.items():
        if candidate_id == query_id:
            continue
        score = cosine_similarity(query.embedding, candidate.embedding)
        if math.isfinite(score):
            scores.append((score, candidate_id))
    scores.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return [candidate_id for _, candidate_id in scores[:k]]


def top_k_embedding_candidates_relations(query_id: str, edges: Dict[str, GraphEdge], k: int) -> List[str]:
    query = edges.get(query_id)
    if query is None or not query.runtime_embedding:
        return []
    scores: List[Tuple[float, str]] = []
    for candidate_id, candidate in edges.items():
        if candidate_id == query_id:
            continue
        score = cosine_similarity(query.runtime_embedding, candidate.runtime_embedding)
        if math.isfinite(score):
            scores.append((score, candidate_id))
    scores.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return [candidate_id for _, candidate_id in scores[:k]]


def majority_vote(values: Sequence[str], fallback: str) -> str:
    normalized_values = [value.strip() for value in values if str(value).strip()]
    if not normalized_values:
        return fallback
    return Counter(normalized_values).most_common(1)[0][0]


def average_embeddings(vectors: Sequence[Sequence[float]]) -> List[float]:
    valid = [np.asarray(vec, dtype=np.float32) for vec in vectors if vec]
    if not valid:
        return []
    dim = len(valid[0])
    filtered = [vec for vec in valid if len(vec) == dim]
    if not filtered:
        return []
    stacked = np.stack(filtered, axis=0)
    mean_vec = np.mean(stacked, axis=0)
    return [float(x) for x in mean_vec.tolist()]


def build_entity_lookup_by_name(nodes: Sequence[GraphNode]) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    for node in nodes:
        for name in node.all_names():
            key = normalize_name(name)
            if key and key not in lookup:
                lookup[key] = node.node_id
    return lookup


def dedupe_edges_exact(edges: Sequence[GraphEdge]) -> List[GraphEdge]:
    grouped: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for edge in edges:
        key = (
            edge.src_id,
            edge.dst_id,
            normalize_name(edge.rel_type),
            normalize_name(edge.canonical_name),
        )
        bucket = grouped.setdefault(
            key,
            {
                "src_id": edge.src_id,
                "dst_id": edge.dst_id,
                "rel_type_counter": Counter(),
                "names": [],
                "created_at": edge.created_at,
            },
        )
        if edge.rel_type.strip():
            bucket["rel_type_counter"][edge.rel_type.strip()] += 1
        bucket["names"].extend(edge.all_names())

    now = utc_now()
    output: List[GraphEdge] = []
    for bucket in grouped.values():
        names = unique_strings(bucket["names"])
        canonical = choose_canonical_name(names)
        alt_names = [name for name in names if normalize_name(name) != normalize_name(canonical)]
        rel_type = ""
        if bucket["rel_type_counter"]:
            rel_type = bucket["rel_type_counter"].most_common(1)[0][0]
        output.append(
            GraphEdge(
                edge_id=f"rel_{uuid.uuid4().hex}",
                src_id=bucket["src_id"],
                dst_id=bucket["dst_id"],
                rel_type=rel_type,
                created_at=bucket["created_at"],
                updated_at=now,
                canonical_name=canonical,
                alt_names=alt_names,
            )
        )
    return output


def resolve_replacement(entity_id: str, replacement_map: Dict[str, str]) -> str:
    seen: Set[str] = set()
    current = entity_id
    while current in replacement_map and current not in seen:
        seen.add(current)
        current = replacement_map[current]
    return current


def remap_edges_to_latest_nodes(edges: Sequence[GraphEdge], replacement_map: Dict[str, str]) -> List[GraphEdge]:
    remapped: List[GraphEdge] = []
    for edge in edges:
        src = resolve_replacement(edge.src_id, replacement_map)
        dst = resolve_replacement(edge.dst_id, replacement_map)
        if not src or not dst:
            continue
        remapped.append(
            GraphEdge(
                edge_id=edge.edge_id,
                src_id=src,
                dst_id=dst,
                rel_type=edge.rel_type,
                created_at=edge.created_at,
                updated_at=edge.updated_at,
                canonical_name=edge.canonical_name,
                alt_names=edge.alt_names,
                runtime_embedding=edge.runtime_embedding,
            )
        )
    return remapped


def relation_drafts_to_edges(
    drafts: Sequence[ExtractedRelation],
    node_lookup_by_name: Dict[str, str],
) -> Tuple[List[GraphEdge], int]:
    edges: List[GraphEdge] = []
    unresolved = 0
    now = utc_now()
    for draft in drafts:
        src_id = node_lookup_by_name.get(normalize_name(draft.src_name))
        dst_id = node_lookup_by_name.get(normalize_name(draft.dst_name))
        if not src_id or not dst_id:
            unresolved += 1
            continue
        names = unique_strings([draft.canonical_name, *draft.alt_names])
        canonical = choose_canonical_name(names)
        alt_names = [name for name in names if normalize_name(name) != normalize_name(canonical)]
        edges.append(
            GraphEdge(
                edge_id=f"rel_{uuid.uuid4().hex}",
                src_id=src_id,
                dst_id=dst_id,
                rel_type=draft.rel_type.strip(),
                created_at=now,
                updated_at=now,
                canonical_name=canonical,
                alt_names=alt_names,
            )
        )
    return edges, unresolved


async def llm_entity_merge_decision(
    query_node: GraphNode,
    candidate_nodes: Sequence[GraphNode],
    client: QwenClient,
    max_tokens: int,
) -> EntityMergeDecision:
    def compact_entity(node: GraphNode) -> Dict[str, Any]:
        return {
            "id": node.node_id,
            "canonical_name": clip_text(node.canonical_name),
            "type": clip_text(node.type, limit=90),
            "alt_names": clip_string_list(node.alt_names, item_limit=10, char_limit=100),
            "attributes": clip_string_list(node.attributes, item_limit=12, char_limit=100),
        }

    prompt_payload = {
        "query": compact_entity(query_node),
        "candidates": [compact_entity(node) for node in candidate_nodes],
        "instruction": "Return JSON only.",
    }
    prompt_payload = fit_payload_char_limit(prompt_payload, char_limit=DEFAULT_MERGE_PROMPT_CHAR_LIMIT)
    response = await client.chat_json(
        system_prompt=ENTITY_MERGE_SYSTEM_PROMPT,
        user_prompt=json.dumps(prompt_payload, ensure_ascii=False),
        max_tokens=max_tokens,
    )
    if not isinstance(response, dict):
        return EntityMergeDecision(merge_with_ids=[], canonical_name="", merged_type="", attributes=[])
    merge_with_ids = [str(x) for x in response.get("merge_with_ids", []) if str(x).strip()]
    canonical_name = str(response.get("canonical_name") or "").strip()
    merged_type = str(response.get("type") or "").strip()
    attributes = unique_strings(response.get("attributes") or [])
    return EntityMergeDecision(
        merge_with_ids=merge_with_ids,
        canonical_name=canonical_name,
        merged_type=merged_type,
        attributes=attributes,
    )


async def llm_relation_merge_decision(
    query_edge: GraphEdge,
    candidate_edges: Sequence[GraphEdge],
    node_lookup: Dict[str, GraphNode],
    client: QwenClient,
    max_tokens: int,
) -> RelationMergeDecision:
    def edge_to_obj(edge: GraphEdge) -> Dict[str, Any]:
        src_name = node_lookup[edge.src_id].canonical_name if edge.src_id in node_lookup else edge.src_id
        dst_name = node_lookup[edge.dst_id].canonical_name if edge.dst_id in node_lookup else edge.dst_id
        return {
            "id": edge.edge_id,
            "src_id": edge.src_id,
            "src_name": clip_text(src_name),
            "dst_id": edge.dst_id,
            "dst_name": clip_text(dst_name),
            "rel_type": clip_text(edge.rel_type, limit=90),
            "canonical_name": clip_text(edge.canonical_name),
            "alt_names": clip_string_list(edge.alt_names, item_limit=10, char_limit=100),
        }

    prompt_payload = {
        "query": edge_to_obj(query_edge),
        "candidates": [edge_to_obj(edge) for edge in candidate_edges],
        "instruction": "Return JSON only.",
    }
    prompt_payload = fit_payload_char_limit(prompt_payload, char_limit=DEFAULT_MERGE_PROMPT_CHAR_LIMIT)
    response = await client.chat_json(
        system_prompt=RELATION_MERGE_SYSTEM_PROMPT,
        user_prompt=json.dumps(prompt_payload, ensure_ascii=False),
        max_tokens=max_tokens,
    )
    if not isinstance(response, dict):
        return RelationMergeDecision(merge_with_ids=[], canonical_name="", rel_type="", src_id="", dst_id="")
    return RelationMergeDecision(
        merge_with_ids=[str(x) for x in response.get("merge_with_ids", []) if str(x).strip()],
        canonical_name=str(response.get("canonical_name") or "").strip(),
        rel_type=str(response.get("rel_type") or "").strip(),
        src_id=str(response.get("src_id") or "").strip(),
        dst_id=str(response.get("dst_id") or "").strip(),
    )


def merge_entity_cluster(cluster: Sequence[GraphNode], decision: EntityMergeDecision) -> GraphNode:
    names: List[str] = []
    types: List[str] = []
    attrs: List[str] = []
    vectors: List[List[float]] = []
    for node in cluster:
        names.extend(node.all_names())
        if node.type.strip():
            types.append(node.type.strip())
        attrs.extend(node.attributes)
        if node.embedding:
            vectors.append(node.embedding)

    canonical = decision.canonical_name.strip() or choose_canonical_name(names)
    all_names = unique_strings([canonical, *names])
    alt_names = [name for name in all_names if normalize_name(name) != normalize_name(canonical)]
    merged_type = decision.merged_type.strip() or majority_vote(types, fallback="")
    merged_attrs = unique_strings([*attrs, *decision.attributes])
    now = utc_now()
    return GraphNode(
        node_id=f"ent_{uuid.uuid4().hex}",
        embedding=average_embeddings(vectors),
        type=merged_type,
        alt_names=alt_names,
        created_at=now,
        updated_at=now,
        attributes=merged_attrs,
        canonical_name=canonical,
    )


def merge_relation_cluster(cluster: Sequence[GraphEdge], decision: RelationMergeDecision) -> GraphEdge:
    names: List[str] = []
    rel_types: List[str] = []
    src_ids: List[str] = []
    dst_ids: List[str] = []
    vectors: List[List[float]] = []
    for edge in cluster:
        names.extend(edge.all_names())
        if edge.rel_type.strip():
            rel_types.append(edge.rel_type.strip())
        src_ids.append(edge.src_id)
        dst_ids.append(edge.dst_id)
        if edge.runtime_embedding:
            vectors.append(edge.runtime_embedding)

    canonical = decision.canonical_name.strip() or choose_canonical_name(names)
    rel_type = decision.rel_type.strip() or majority_vote(rel_types, fallback="")
    src_id = decision.src_id.strip() or majority_vote(src_ids, fallback=cluster[0].src_id)
    dst_id = decision.dst_id.strip() or majority_vote(dst_ids, fallback=cluster[0].dst_id)
    all_names = unique_strings([canonical, *names])
    alt_names = [name for name in all_names if normalize_name(name) != normalize_name(canonical)]
    now = utc_now()
    return GraphEdge(
        edge_id=f"rel_{uuid.uuid4().hex}",
        src_id=src_id,
        dst_id=dst_id,
        rel_type=rel_type,
        created_at=now,
        updated_at=now,
        canonical_name=canonical,
        alt_names=alt_names,
        runtime_embedding=average_embeddings(vectors),
    )


async def merge_entities(
    nodes: Sequence[GraphNode],
    client: QwenClient,
    k1: int,
    k2: int,
    max_tokens: int,
) -> Tuple[List[GraphNode], Dict[str, str]]:
    active: Dict[str, GraphNode] = {node.node_id: node for node in nodes}
    replacement_map: Dict[str, str] = {}
    queue: deque[str] = deque(node.node_id for node in nodes)

    while queue:
        query_id = queue.popleft()
        query_node = active.get(query_id)
        if query_node is None:
            continue

        string_index = StringSimilarityIndex(
            {
                node_id: " | ".join(unique_strings([node.canonical_name, *node.alt_names]))
                for node_id, node in active.items()
            }
        )
        top_embedding = top_k_embedding_candidates_entities(query_id=query_id, nodes=active, k=k1)
        top_string = string_index.top_k(query_id=query_id, k=k2)
        candidate_ids = list(dict.fromkeys([*top_embedding, *top_string]))
        if not candidate_ids:
            continue

        candidates = [active[cid] for cid in candidate_ids if cid in active and cid != query_id]
        if not candidates:
            continue

        decision = await llm_entity_merge_decision(
            query_node=query_node,
            candidate_nodes=candidates,
            client=client,
            max_tokens=max_tokens,
        )
        allowed = {node.node_id for node in candidates}
        selected = [cid for cid in decision.merge_with_ids if cid in allowed and cid in active]
        if not selected:
            continue

        cluster_ids = [query_id, *selected]
        cluster_nodes = [active[cid] for cid in cluster_ids if cid in active]
        merged = merge_entity_cluster(cluster_nodes, decision=decision)

        for old_id in cluster_ids:
            if old_id in active:
                replacement_map[old_id] = merged.node_id
                active.pop(old_id, None)
        active[merged.node_id] = merged
        queue.append(merged.node_id)

    return list(active.values()), replacement_map


async def merge_relations(
    edges: Sequence[GraphEdge],
    node_lookup: Dict[str, GraphNode],
    client: QwenClient,
    k1: int,
    k2: int,
    max_tokens: int,
) -> List[GraphEdge]:
    active: Dict[str, GraphEdge] = {edge.edge_id: edge for edge in edges}
    queue: deque[str] = deque(edge.edge_id for edge in edges)

    while queue:
        query_id = queue.popleft()
        query_edge = active.get(query_id)
        if query_edge is None:
            continue

        string_index = StringSimilarityIndex(
            {
                edge_id: build_relation_embedding_text(edge, node_lookup=node_lookup)
                for edge_id, edge in active.items()
            }
        )
        top_embedding = top_k_embedding_candidates_relations(query_id=query_id, edges=active, k=k1)
        top_string = string_index.top_k(query_id=query_id, k=k2)
        candidate_ids = list(dict.fromkeys([*top_embedding, *top_string]))
        if not candidate_ids:
            continue

        candidates = [active[cid] for cid in candidate_ids if cid in active and cid != query_id]
        if not candidates:
            continue

        decision = await llm_relation_merge_decision(
            query_edge=query_edge,
            candidate_edges=candidates,
            node_lookup=node_lookup,
            client=client,
            max_tokens=max_tokens,
        )
        allowed = {edge.edge_id for edge in candidates}
        selected = [cid for cid in decision.merge_with_ids if cid in allowed and cid in active]
        if not selected:
            continue

        cluster_ids = [query_id, *selected]
        cluster_edges = [active[cid] for cid in cluster_ids if cid in active]
        merged = merge_relation_cluster(cluster_edges, decision=decision)

        for old_id in cluster_ids:
            active.pop(old_id, None)
        active[merged.edge_id] = merged
        queue.append(merged.edge_id)

    return list(active.values())


async def run_pipeline(args: argparse.Namespace) -> Dict[str, Any]:
    articles_path = Path(args.articles)
    output_path = Path(args.output)
    embeddings_output_path = Path(args.embeddings_output) if args.embeddings_output else default_embeddings_path(output_path)
    existing_path = Path(args.existing_graph) if args.existing_graph else None
    existing_embeddings_path = (
        Path(args.existing_embeddings)
        if args.existing_embeddings
        else default_embeddings_path(existing_path)
        if existing_path is not None
        else None
    )

    articles = load_articles(path=articles_path, text_key=args.text_key)
    if not articles:
        raise ValueError("No articles with valid text found in input.")

    existing_nodes, existing_edges = load_existing_graph(
        existing_path,
        embeddings_path=existing_embeddings_path,
        embeddings_required=bool(args.existing_embeddings),
    )

    tokenizer = QwenTokenizer(model_name=args.llm_model, enabled=not args.disable_qwen_tokenizer)
    llm_semaphore = asyncio.Semaphore(args.llm_concurrency)
    embed_semaphore = asyncio.Semaphore(args.embed_concurrency)

    async with QwenClient(
        base_url=args.base_url,
        api_key=args.api_key,
        llm_model=args.llm_model,
        embed_model=args.embed_model,
        llm_semaphore=llm_semaphore,
        embed_semaphore=embed_semaphore,
        max_retries=args.max_retries,
        timeout_seconds=args.timeout_seconds,
    ) as client:
        extracted_entities, extracted_relations, extraction_failures = await extract_all(
            articles=articles,
            client=client,
            tokenizer=tokenizer,
            max_input_tokens=min(args.extract_input_tokens, args.context_limit - args.extract_output_tokens),
            max_output_tokens=args.extract_output_tokens,
        )
        logging.info(
            "Extraction complete: %s entities, %s relations, %s failed articles",
            len(extracted_entities),
            len(extracted_relations),
            extraction_failures,
        )

        new_nodes = dedupe_entities_exact(extracted_entities)
        all_nodes = [*existing_nodes, *new_nodes]
        await ensure_entity_embeddings(all_nodes, client=client, batch_size=args.embed_batch_size)
        merged_nodes, replacement_map = await merge_entities(
            nodes=all_nodes,
            client=client,
            k1=args.k1,
            k2=args.k2,
            max_tokens=args.merge_output_tokens,
        )
        node_lookup = {node.node_id: node for node in merged_nodes}
        name_lookup = build_entity_lookup_by_name(merged_nodes)

        remapped_existing_edges = remap_edges_to_latest_nodes(existing_edges, replacement_map=replacement_map)
        new_edges_from_drafts, unresolved_relations = relation_drafts_to_edges(
            extracted_relations,
            node_lookup_by_name=name_lookup,
        )
        all_edges = dedupe_edges_exact([*remapped_existing_edges, *new_edges_from_drafts])
        await ensure_relation_embeddings(all_edges, node_lookup=node_lookup, client=client, batch_size=args.embed_batch_size)
        merged_edges = await merge_relations(
            edges=all_edges,
            node_lookup=node_lookup,
            client=client,
            k1=args.k1,
            k2=args.k2,
            max_tokens=args.merge_output_tokens,
        )

    # Ensure edges reference final known nodes only.
    final_edges = [edge for edge in merged_edges if edge.src_id in node_lookup and edge.dst_id in node_lookup]
    node_embedding_rows = [node.as_embedding_dict() for node in merged_nodes if node.embedding]
    edge_embedding_rows = [edge.as_embedding_dict() for edge in final_edges if edge.runtime_embedding]
    final_payload = {
        "nodes": [node.as_dict(include_embedding=False) for node in merged_nodes],
        "edges": [edge.as_dict() for edge in final_edges],
        "metadata": {
            "generated_at": utc_now(),
            "embeddings_file": str(embeddings_output_path),
            "input_articles": len(articles),
            "existing_nodes": len(existing_nodes),
            "existing_edges": len(existing_edges),
            "extracted_entities": len(extracted_entities),
            "extracted_relations": len(extracted_relations),
            "unresolved_relations_after_entity_merge": unresolved_relations,
            "entity_nodes_final": len(merged_nodes),
            "relation_edges_final": len(final_edges),
            "extraction_failures": extraction_failures,
            "config": {
                "k1": args.k1,
                "k2": args.k2,
                "llm_model": args.llm_model,
                "embed_model": args.embed_model,
                "llm_concurrency": args.llm_concurrency,
                "embed_concurrency": args.embed_concurrency,
                "embed_batch_size": args.embed_batch_size,
                "max_retries": args.max_retries,
                "timeout_seconds": args.timeout_seconds,
                "context_limit": args.context_limit,
            },
        },
    }
    embeddings_payload = {
        "node_embeddings": node_embedding_rows,
        "edge_embeddings": edge_embedding_rows,
        "metadata": {
            "generated_at": utc_now(),
            "graph_file": str(output_path),
            "node_embeddings_count": len(node_embedding_rows),
            "edge_embeddings_count": len(edge_embedding_rows),
            "embed_model": args.embed_model,
        },
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    embeddings_output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(final_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    embeddings_output_path.write_text(json.dumps(embeddings_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return final_payload


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    payload = asyncio.run(run_pipeline(args))
    logging.info("Wrote graph: %s nodes, %s edges", len(payload["nodes"]), len(payload["edges"]))


if __name__ == "__main__":
    main()
