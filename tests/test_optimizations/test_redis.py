import time

from src.config import CART_TTL, RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW
from src.db.redis_client import redis_client


def test_cart_add():
    user = "U1"
    redis_client.clear_cart(user)

    redis_client.add_to_cart(user, "P1", 2)
    redis_client.add_to_cart(user, "P1", 1)  # total 3
    redis_client.add_to_cart(user, "P2", 4)

    cart = redis_client.get_cart(user)
    assert cart == {"P1": 3, "P2": 4}

    ttl = redis_client.client.ttl(f"cart:{user}")
    assert 0 < ttl <= CART_TTL


def test_rate_limit_buckets():
    user, endpoint = "U1", "search"
    passed = 0

    # use up all slots
    for _ in range(RATE_LIMIT_REQUESTS):
        assert redis_client.rate_limit_ok(user, endpoint)
        passed += 1

    # next call should fail
    assert not redis_client.rate_limit_ok(user, endpoint)
    assert passed == RATE_LIMIT_REQUESTS

    # wait for new window
    time.sleep(RATE_LIMIT_WINDOW + 1)
    assert redis_client.rate_limit_ok(user, endpoint)


def test_hot_products_sorted():
    redis_client.client.delete(redis_client._hot_key())  # reset today’s zset

    redis_client.record_view("P1")  # +1
    redis_client.record_view("P2", score=3)  # +3
    redis_client.record_view("P3", score=2)  # +2

    top = redis_client.get_hot_products(top=3)
    pids = [pid for pid, score in top]
    assert pids == ["P2", "P3", "P1"]
