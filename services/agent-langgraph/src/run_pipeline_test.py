from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, Optional, TYPE_CHECKING
from urllib.parse import urlparse

try:
    import psycopg
except ModuleNotFoundError:  # pragma: no cover - integration dependency
    psycopg = None

try:
    import requests
except ModuleNotFoundError:  # pragma: no cover - integration dependency
    requests = None

try:
    from minio import Minio
except ModuleNotFoundError:  # pragma: no cover - integration dependency
    Minio = None  # type: ignore[assignment]

if TYPE_CHECKING:
    from minio import Minio as MinioClient

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

def _get_dsn() -> str:
    return os.getenv("DATABASE_URL", "postgresql://osint:osint@postgres:5432/osint")


def _ensure_run(run_id: str, prompt: str) -> None:
    if psycopg is None:
        raise RuntimeError("Missing dependency: psycopg. Install services/agent-langgraph/requirements.txt.")
    dsn = _get_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO runs(run_id, prompt, seeds, constraints, status)
                VALUES (%s, %s, %s::jsonb, %s::jsonb, 'created')
                ON CONFLICT (run_id) DO NOTHING
                """,
                (run_id, prompt, "[]", "{}"),
            )


def _get_document_object(document_id: str) -> Dict[str, Optional[str]]:
    if psycopg is None:
        raise RuntimeError("Missing dependency: psycopg. Install services/agent-langgraph/requirements.txt.")
    dsn = _get_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT bucket, object_key, version_id, etag
                FROM document_objects
                WHERE document_id = %s AND kind = 'raw'
                LIMIT 1
                """,
                (document_id,),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError("No document_object found for raw content")
            return {
                "bucket": row[0],
                "objectKey": row[1],
                "versionId": row[2],
                "etag": row[3],
            }


def _fetch_text(url: str, max_chars: int) -> str:
    if requests is None:
        raise RuntimeError("Missing dependency: requests. Install services/agent-langgraph/requirements.txt.")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    text = response.text
    if max_chars > 0:
        return text[:max_chars]
    return text


def _get_minio_client() -> "MinioClient":
    if Minio is None:
        raise RuntimeError("Missing dependency: minio. Install services/agent-langgraph/requirements.txt.")
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minio")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minio12345")

    parsed = urlparse(endpoint)
    host = parsed.hostname or endpoint
    port = parsed.port
    use_ssl = (parsed.scheme or "http") == "https"

    if port:
        host = f"{host}:{port}"

    return Minio(host, access_key=access_key, secret_key=secret_key, secure=use_ssl)


def _fetch_text_from_minio(bucket: str, object_key: str, max_chars: int) -> str:
    client = _get_minio_client()
    response = client.get_object(bucket, object_key)
    try:
        data = response.read()
    finally:
        response.close()
        response.release_conn()

    text = data.decode("utf-8", errors="ignore")
    if max_chars > 0:
        return text[:max_chars]
    return text


def main() -> None:
    from env import load_env
    from logger import get_logger
    from mcp_client import RoutedMcpClient
    from planner_graph import run_planner

    logger = get_logger(__name__)
    load_env()
    parser = argparse.ArgumentParser(description="Run end-to-end pipeline test")
    parser.add_argument(
        "--url",
        default="http://example.com",
        help="HTTP URL to fetch for smoke testing",
    )
    parser.add_argument("--max-chars", type=int, default=40000)
    args = parser.parse_args()

    run_id = os.getenv("RUN_ID") or _new_run_id()
    prompt = f"Fetch {args.url}"

    logger.info("Pipeline test started", extra={"run_id": run_id, "url": args.url})
    _ensure_run(run_id, prompt)

    planner_result = run_planner(run_id=run_id, prompt=prompt, inputs=[], max_iterations=1)
    if not planner_result.documents_created:
        raise RuntimeError("Planner did not create any documents")

    document_id = planner_result.documents_created[0]
    doc_obj = _get_document_object(document_id)

    evidence = {
        "bucket": doc_obj.get("bucket"),
        "objectKey": doc_obj.get("objectKey"),
        "versionId": doc_obj.get("versionId"),
        "etag": doc_obj.get("etag"),
        "documentId": document_id,
    }

    bucket = doc_obj.get("bucket")
    object_key = doc_obj.get("objectKey")
    if bucket and object_key:
        text = _fetch_text_from_minio(bucket, object_key, args.max_chars)
        logger.info("Loaded text from MinIO", extra={"bucket": bucket, "object_key": object_key})
    else:
        text = _fetch_text(args.url, args.max_chars)

    client = RoutedMcpClient()
    client.start()
    try:
        ingest_text = client.call_tool(
            "ingest_text",
            {
                "runId": run_id,
                "text": text,
                "title": "Pipeline test",
                "maxChars": 8000,
                "overlap": 200,
                "evidenceJson": json.dumps(evidence),
            },
        )
        if not ingest_text.ok:
            raise RuntimeError(f"ingest_text failed: {ingest_text.content}")

        entities = [
            {
                "entityType": "Person",
                "properties": {"name": "Joe Biden"},
                "evidence": {"objectRef": evidence},
                "relations": [
                    {
                        "type": "MENTIONED_IN",
                        "targetType": "Article",
                        "targetProperties": {
                            "uri": args.url,
                            "name": "Joe Biden - Wikipedia",
                        },
                        "evidenceRef": evidence,
                    }
                ],
            },
            {
                "entityType": "Article",
                "properties": {"uri": args.url, "name": "Joe Biden - Wikipedia"},
                "evidence": {"objectRef": evidence},
            },
        ]

        ingest_graph = client.call_tool(
            "ingest_graph_entities",
            {
                "runId": run_id,
                "entitiesJson": json.dumps(entities),
            },
        )
        if not ingest_graph.ok:
            raise RuntimeError(f"ingest_graph_entities failed: {ingest_graph.content}")

    finally:
        client.close()

    logger.info("Pipeline test completed", extra={"run_id": run_id})
    print("GOOD")


def _new_run_id() -> str:
    import uuid

    return str(uuid.uuid4())


if __name__ == "__main__":
    main()
