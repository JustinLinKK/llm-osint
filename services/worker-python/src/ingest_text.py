from __future__ import annotations

import argparse
import hashlib
import os
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import psycopg
import requests
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, PointStruct, VectorParams

DEFAULT_MAX_CHARS = 2000
DEFAULT_OVERLAP = 200


@dataclass
class Chunk:
    chunk_id: str
    chunk_index: int
    char_start: int
    char_end: int
    text: str
    section_title: str | None


def load_env() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    env_path = repo_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)


def chunk_text(text: str, max_chars: int = DEFAULT_MAX_CHARS, overlap: int = DEFAULT_OVERLAP) -> List[Chunk]:
    cleaned = text.replace("\r\n", "\n").strip()
    if not cleaned:
        return []

    sections = split_sections(cleaned)
    if not sections:
        return []

    chunks: List[Chunk] = []
    index = 0

    for section in sections:
        start = section["content_start"]
        end_limit = section["content_end"]
        while start < end_limit:
            end = min(start + max_chars, end_limit)
            if end < end_limit:
                window = cleaned[start:end]
                last_break = max(window.rfind("\n"), window.rfind(" "))
                if last_break > 0:
                    end = start + last_break
            chunk_text_value = cleaned[start:end].strip()
            if chunk_text_value:
                chunks.append(
                    Chunk(
                        chunk_id=str(uuid.uuid4()),
                        chunk_index=index,
                        char_start=start,
                        char_end=end,
                        text=chunk_text_value,
                        section_title=section["title"],
                    )
                )
                index += 1
            if end >= end_limit:
                break
            start = max(section["content_start"], end - overlap)

    return chunks


def split_sections(text: str) -> List[dict]:
    lines = text.split("\n")
    sections: List[dict] = []
    offset = 0
    current_title: str | None = None
    current_start = 0

    def push_section(end_offset: int) -> None:
        trimmed_start = min(current_start, end_offset)
        trimmed_end = max(trimmed_start, end_offset)
        if trimmed_end > trimmed_start:
            sections.append({"title": current_title, "content_start": trimmed_start, "content_end": trimmed_end})

    for line in lines:
        line_start = offset
        line_end = offset + len(line)
        normalized = line.strip()
        if is_heading_line(normalized):
            push_section(line_start)
            current_title = normalized.lstrip("#").strip() or normalized
            current_start = line_end + 1
        offset = line_end + 1

    push_section(len(text))

    if not sections:
        return [{"title": None, "content_start": 0, "content_end": len(text)}]

    return sections


def is_heading_line(line: str) -> bool:
    if not line:
        return False
    if line.startswith("#") and line.lstrip("#").strip():
        return True
    if len(line) > 80:
        return False
    letters = "".join([ch for ch in line if ch.isalpha()])
    return len(letters) >= 4 and letters.isupper()


def insert_document(
    conn: psycopg.Connection,
    run_id: str,
    text: str,
    source_url: Optional[str],
    title: Optional[str],
) -> str:
    sha256 = hashlib.sha256(text.encode("utf-8")).hexdigest()
    source_type = "text"
    content_type = "text/plain"
    source_domain = urlparse(source_url).hostname if source_url else None

    with conn.cursor() as cur:
        cur.execute(
            "SELECT document_id FROM documents WHERE run_id = %s AND sha256 = %s",
            (run_id, sha256),
        )
        row = cur.fetchone()
        if row:
            return str(row[0])

        document_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO documents(
              document_id, run_id, source_url, source_domain, source_type,
              content_type, sha256, trust_tier, extraction_state, title
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 3, 'parsed', %s)
            """,
            (
                document_id,
                run_id,
                source_url,
                source_domain,
                source_type,
                content_type,
                sha256,
                title,
            ),
        )
        return document_id


def insert_chunks(conn: psycopg.Connection, document_id: str, chunks: List[Chunk]) -> None:
    if not chunks:
        return

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO chunks(
              chunk_id, document_id, kind, chunk_index, char_start, char_end, text
            ) VALUES (%s, %s, 'body', %s, %s, %s, %s)
            """,
            [
                (
                    chunk.chunk_id,
                    document_id,
                    chunk.chunk_index,
                    chunk.char_start,
                    chunk.char_end,
                    chunk.text,
                )
                for chunk in chunks
            ],
        )


def embed_texts(texts: List[str]) -> List[List[float]]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY not set")

    model = os.getenv("OPENROUTER_EMBED_MODEL", "openai/text-embedding-3-small")
    payload = {
        "model": model,
        "input": texts,
    }
    response = requests.post(
        "https://openrouter.ai/api/v1/embeddings",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=60,
    )
    response.raise_for_status()
    data = response.json()
    embeddings = [item.get("embedding") for item in data.get("data", [])]
    if any(vec is None for vec in embeddings):
        raise RuntimeError("Embedding response missing vectors")
    return embeddings  # type: ignore[return-value]


def ensure_qdrant_collection(client: QdrantClient, collection: str, vector_size: int) -> None:
    try:
        client.get_collection(collection)
        return
    except Exception:
        pass

    client.create_collection(
        collection_name=collection,
        vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
    )


def upsert_vectors(
    client: QdrantClient,
    collection: str,
    run_id: str,
    document_id: str,
    source_url: Optional[str],
    chunks: List[Chunk],
    embeddings: List[List[float]],
    title: Optional[str],
) -> None:
    points: List[PointStruct] = []
    for chunk, vector in zip(chunks, embeddings):
        payload = {
            "run_id": run_id,
            "document_id": document_id,
            "chunk_id": chunk.chunk_id,
            "chunk_index": chunk.chunk_index,
            "char_start": chunk.char_start,
            "char_end": chunk.char_end,
            "source_url": source_url,
            "source_type": "text",
            "content_type": "text/plain",
            "title": title,
            "section_title": chunk.section_title,
        }
        points.append(PointStruct(id=chunk.chunk_id, vector=vector, payload=payload))

    client.upsert(collection_name=collection, points=points)


def update_chunk_vectors(conn: psycopg.Connection, chunks: List[Chunk], model: str) -> None:
    if not chunks:
        return

    with conn.cursor() as cur:
        cur.executemany(
            "UPDATE chunks SET vector_id = %s, embedding_model = %s WHERE chunk_id = %s",
            [(chunk.chunk_id, model, chunk.chunk_id) for chunk in chunks],
        )


def batch(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def ingest_text(
    run_id: str,
    text: str,
    source_url: Optional[str],
    title: Optional[str],
    max_chars: int,
    overlap: int,
) -> Tuple[str, int]:
    load_env()

    dsn = os.getenv("DATABASE_URL", "postgresql://osint:osint@postgres:5432/osint")
    qdrant_url = os.getenv("QDRANT_URL", "http://qdrant:6333")
    collection = os.getenv("QDRANT_COLLECTION", "osint_chunks")
    embed_model = os.getenv("OPENROUTER_EMBED_MODEL", "openai/text-embedding-3-small")

    chunks = chunk_text(text, max_chars=max_chars, overlap=overlap)

    with psycopg.connect(dsn) as conn:
        document_id = insert_document(conn, run_id, text, source_url, title)
        insert_chunks(conn, document_id, chunks)
        conn.commit()

        if chunks:
            texts = [chunk.text for chunk in chunks]
            embeddings: List[List[float]] = []
            for batch_texts in batch(texts, 32):
                embeddings.extend(embed_texts(batch_texts))

            client = QdrantClient(url=qdrant_url)
            ensure_qdrant_collection(client, collection, len(embeddings[0]))
            upsert_vectors(client, collection, run_id, document_id, source_url, chunks, embeddings, title)
            update_chunk_vectors(conn, chunks, embed_model)
            conn.commit()

    return document_id, len(chunks)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest raw text into Postgres + Qdrant")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--text", help="Raw text to ingest")
    parser.add_argument("--text-file", help="Path to a text file to ingest")
    parser.add_argument("--source-url", help="Optional source URL")
    parser.add_argument("--title", help="Optional title")
    parser.add_argument("--max-chars", type=int, default=DEFAULT_MAX_CHARS)
    parser.add_argument("--overlap", type=int, default=DEFAULT_OVERLAP)

    args = parser.parse_args()

    if args.text_file:
        text = Path(args.text_file).read_text(encoding="utf-8")
    elif args.text:
        text = args.text
    else:
        text = sys.stdin.read()

    if not text.strip():
        raise SystemExit("No text provided")

    document_id, chunk_count = ingest_text(
        run_id=args.run_id,
        text=text,
        source_url=args.source_url,
        title=args.title,
        max_chars=args.max_chars,
        overlap=args.overlap,
    )

    print(f"document_id={document_id} chunks={chunk_count}")


if __name__ == "__main__":
    main()
