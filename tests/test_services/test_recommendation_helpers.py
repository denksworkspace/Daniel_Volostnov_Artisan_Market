from src.loaders.graph_loader import main as load_graph
from src.services.recommendation_service import recommendation_service


def _seed_graph():
    """
    neo4j_clean autouse fixture empties DB for every test; reload data each time.
    `load_graph()` is idempotent and fast (~200 ms).
    """
    load_graph()


def test_also_bought_service():
    _seed_graph()
    recs = recommendation_service.also_bought(product_id=1, limit=3)
    assert isinstance(recs, list)
    if recs:  # may be empty for some synthetic data
        assert {"id", "name", "freq"} <= recs[0].keys()


def test_frequently_bought_together_service():
    _seed_graph()
    recs = recommendation_service.frequently_bought_together(product_id=1, limit=3)
    assert isinstance(recs, list)
    if recs:
        assert {"id", "name", "together"} <= recs[0].keys()


def test_personalized_service():
    _seed_graph()
    recs = recommendation_service.personalized(user_id=1, limit=3)
    assert isinstance(recs, list)
    if recs:
        assert {"id", "name", "score"} <= recs[0].keys()
