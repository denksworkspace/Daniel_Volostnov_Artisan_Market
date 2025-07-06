from src.db.redis_client import redis_client
from src.services.search_service import search_service


def test_search_cache_hits():
    redis_client.client.delete("stats:search:hits", "stats:search:miss")
    redis_client.client.flushdb()  # clear any old cache keys

    # first run -> miss
    res1 = search_service.search("wooden bowl", limit=5)
    stats1 = search_service.cache_stats()
    assert stats1["miss"] == 1 and stats1["hits"] == 0

    # second identical run -> hit
    res2 = search_service.search("wooden bowl", limit=5)
    stats2 = search_service.cache_stats()
    assert stats2["hits"] == 1 and stats2["miss"] == 1
    assert res1 == res2  # cached result identical
