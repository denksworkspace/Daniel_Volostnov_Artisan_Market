from src.db.redis_client import redis_client


def test_redis_cart():
    user, product = "U42", "P99"
    redis_client.clear_cart(user)

    redis_client.add_to_cart(user, product, 2)
    qty = int(redis_client.client.hget(f"cart:{user}", product))
    assert qty == 2
