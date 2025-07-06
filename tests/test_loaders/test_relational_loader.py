import json

from src.db.postgres_client import db


def test_order_items_fk_integrity():
    with db.get_cursor() as cur:
        cur.execute(
            """
            SELECT oi.product_id
            FROM order_items oi
            LEFT JOIN products p ON oi.product_id = p.id
            WHERE p.id IS NULL
            LIMIT 1
            """
        )
        row = cur.fetchone()
        assert row is None, "orphaned product_id in order_items"


def test_embeddings_shape():
    with db.get_cursor() as cur:
        cur.execute("SELECT embedding FROM product_embeddings LIMIT 1")
        row = cur.fetchone()
    vec = json.loads(row["embedding"])
    assert len(vec) == 384
