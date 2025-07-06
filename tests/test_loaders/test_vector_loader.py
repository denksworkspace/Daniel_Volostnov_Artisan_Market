"""Verify that vector_loader creates at least one embedding row."""

from src.db.postgres_client import db
from src.loaders.vector_loader import main as load_vectors


def test_vector_loader_insert():
    load_vectors()  # rebuild embeddings

    with db.get_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM product_embeddings")
        row_count = cur.fetchone()["c"]

    # we generated 60 mock products earlier, so expect ≥ 60 vectors
    assert row_count >= 60
