"""Semantic product-similarity search backed by pgvector (sync/psycopg2)."""

from typing import Any

import psycopg2
from sentence_transformers import SentenceTransformer

from src.config import POSTGRES_CONFIG

MODEL = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
CONN = psycopg2.connect(**POSTGRES_CONFIG)
CONN.autocommit = True


def similar_by_text(query: str, limit: int = 5) -> list[dict[str, Any]]:
    vec = MODEL.encode(query, normalize_embeddings=True).tolist()
    sql = """
        SELECT p.id,
               p.name,
               p.price_cents,
               1 - (e.embedding <#> %s::vector) AS score
        FROM product_embeddings e
        JOIN products p ON p.id = e.product_id
        ORDER BY e.embedding <#> %s::vector
        LIMIT %s
    """
    with CONN.cursor() as cur:
        cur.execute(sql, (vec, vec, limit))
        rows = cur.fetchall()
    return [{"id": r[0], "name": r[1], "price_cents": r[2], "score": r[3]} for r in rows]


def similar_to_product(product_id: int, limit: int = 5) -> list[dict[str, Any]]:
    sql = """
        WITH src AS (
            SELECT embedding
            FROM product_embeddings
            WHERE product_id = %s
        )
        SELECT p.id,
               p.name,
               p.price_cents,
               1 - (e.embedding <#> src.embedding) AS score
        FROM product_embeddings e
        JOIN products p ON p.id = e.product_id
        CROSS JOIN src
        WHERE e.product_id <> %s
        ORDER BY e.embedding <#> src.embedding
        LIMIT %s
    """
    with CONN.cursor() as cur:
        cur.execute(sql, (product_id, product_id, limit))
        rows = cur.fetchall()
    return [{"id": r[0], "name": r[1], "price_cents": r[2], "score": r[3]} for r in rows]
