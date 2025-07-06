"""Full-text product search with Redis result-cache + hit-rate counters."""

from __future__ import annotations

import logging
from typing import Any

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from src.config import CACHE_TTL, POSTGRES_CONFIG
from src.db.redis_client import redis_client

_log = logging.getLogger(__name__)


class ProductSearchService:
    def __init__(self) -> None:
        self._conn = psycopg2.connect(**POSTGRES_CONFIG)
        self._conn.autocommit = True

    # ─────────────────────────── public api ────────────────────────────
    def search(
        self,
        query: str,
        category: int | None = None,
        price_range: tuple[int, int] | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """
        Full-text search across product name+description with optional
        category-id and price-cents range filters.

        Caches each distinct parameter set for 1 h.
        """
        cache_key = self._make_key(query, category, price_range, limit)
        cached = redis_client.get_json(cache_key)
        if cached is not None:
            redis_client.client.incr("stats:search:hits")
            return cached

        redis_client.client.incr("stats:search:miss")

        rows = self._run_pg_query(query, category, price_range, limit)
        redis_client.set_json(cache_key, rows, ttl=CACHE_TTL)
        return rows

    def cache_stats(self) -> dict[str, int]:
        """Return current hit / miss counters."""
        h = redis_client.client.get("stats:search:hits") or 0
        m = redis_client.client.get("stats:search:miss") or 0
        return {"hits": int(h), "miss": int(m)}

    # ──────────────────────── internal helpers ─────────────────────────
    def _make_key(
        self,
        q: str,
        cat: int | None,
        pr: tuple[int, int] | None,
        lim: int,
    ) -> str:
        pr_key = f"{pr[0]}-{pr[1]}" if pr else "all"
        return f"search:{q}:{cat or 'all'}:{pr_key}:{lim}"

    def _run_pg_query(
        self,
        q: str,
        cat: int | None,
        pr: tuple[int, int] | None,
        lim: int,
    ) -> list[dict[str, Any]]:
        with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
            parts = [
                sql.SQL(
                    "SELECT id, name, price_cents, "
                    "ts_rank_cd(search_vector, plainto_tsquery(%s)) AS rank "
                    "FROM products "
                    "WHERE search_vector @@ plainto_tsquery(%s)"
                )
            ]
            params = [q, q]

            if cat:
                parts.append(sql.SQL("AND category_id = %s"))
                params.append(cat)
            if pr:
                parts.append(sql.SQL("AND price_cents BETWEEN %s AND %s"))
                params.extend(pr)

            parts.append(sql.SQL("ORDER BY rank DESC LIMIT %s"))
            params.append(lim)

            cur.execute(sql.SQL(" ").join(parts), params)
            return [dict(r) for r in cur.fetchall()]


search_service = ProductSearchService()
