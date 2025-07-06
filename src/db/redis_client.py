"""Redis connection and utilities."""

from __future__ import annotations

import datetime
import json
import time
from typing import Any

import redis

from src.config import CART_TTL, RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW, REDIS_CONFIG


class RedisClient:
    def __init__(self) -> None:
        self._r = redis.Redis(**REDIS_CONFIG)
        self.client = self._r

    # ──────────────────────────── JSON cache ────────────────────────────
    def get_json(self, key: str) -> Any | None:
        val = self._r.get(key)
        return json.loads(val) if val else None

    def set_json(self, key: str, value: Any, ttl: int) -> bool:
        return bool(self._r.setex(key, ttl, json.dumps(value)))

    # ────────────────────────── shopping cart ───────────────────────────
    def _cart_key(self, user_id: str) -> str:
        return f"cart:{user_id}"

    def add_to_cart(self, user_id: str, product_id: str, qty: int = 1) -> None:
        key = self._cart_key(user_id)
        self._r.hincrby(key, product_id, qty)
        self._r.expire(key, CART_TTL)

    def update_cart(self, user_id: str, product_id: str, qty: int) -> None:
        key = self._cart_key(user_id)
        if qty <= 0:
            self._r.hdel(key, product_id)
        else:
            self._r.hset(key, product_id, qty)
        self._r.expire(key, CART_TTL)

    def get_cart(self, user_id: str) -> dict[str, int]:
        key = self._cart_key(user_id)
        raw = self._r.hgetall(key)
        return {(pid.decode() if isinstance(pid, bytes) else pid): int(qty) for pid, qty in raw.items()}

    def clear_cart(self, user_id: str) -> None:
        self._r.delete(self._cart_key(user_id))

    # ───────────────────────────── rate limit ───────────────────────────
    def _bucket_key(self, user_id: str, endpoint: str) -> str:
        window_id = int(time.time()) // RATE_LIMIT_WINDOW
        return f"rl:{user_id}:{endpoint}:{window_id}"

    def rate_limit_ok(self, user_id: str, endpoint: str) -> bool:
        key = self._bucket_key(user_id, endpoint)
        cnt = self._r.incr(key)
        if cnt == 1:
            self._r.expire(key, RATE_LIMIT_WINDOW)
        return cnt <= RATE_LIMIT_REQUESTS

    # ──────────────────────── hot products toplist ──────────────────────
    def _hot_key(self, date: datetime.date | None = None) -> str:
        date = date or datetime.date.today()
        return f"hot_products:{date.isoformat()}"

    def record_view(self, product_id: str, score: int = 1) -> None:
        self._r.zincrby(self._hot_key(), score, product_id)

    def get_hot_products(self, date: datetime.date | None = None, top: int = 10) -> list[tuple[str, float]]:
        key = self._hot_key(date)
        pairs = self._r.zrevrange(key, 0, top - 1, withscores=True)
        return [(pid.decode() if isinstance(pid, bytes) else pid, score) for pid, score in pairs]


redis_client = RedisClient()
