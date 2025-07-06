from src.loaders.vector_loader import main as load_vectors
from src.services.vector_search_service import (
    similar_by_text,
    similar_to_product,
)


def _ensure_vectors():
    """Regenerate embeddings once per test to satisfy postgres_clean fixture."""
    load_vectors()


def test_vector_similar_by_text():
    _ensure_vectors()
    res = similar_by_text("handmade wooden bowl", limit=3)
    assert isinstance(res, list)
    if res:
        assert {"id", "name", "score"} <= res[0].keys()


def test_vector_similar_to_product():
    _ensure_vectors()
    res = similar_to_product(1, limit=3)
    assert isinstance(res, list)
    if res:
        # first hit must not be the same product
        assert res[0]["id"] != 1
