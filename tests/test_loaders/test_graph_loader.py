from src.db.neo4j_client import neo4j_client
from src.loaders.graph_loader import main as load_graph


def test_graph_loaded():
    load_graph()  # rebuild data

    with neo4j_client.driver.session() as s:
        u_cnt = s.run("MATCH (u:User) RETURN count(u) AS c").single()["c"]
        p_cnt = s.run("MATCH (p:Product) RETURN count(p) AS c").single()["c"]
        c_cnt = s.run("MATCH (c:Category) RETURN count(c) AS c").single()["c"]
        r_cnt = s.run("MATCH ()-[r:PURCHASED]->() RETURN count(r) AS c").single()["c"]

    assert u_cnt > 0, "no users"
    assert p_cnt > 0, "no products"
    assert c_cnt > 0, "no categories"
    assert r_cnt > 0, "no PURCHASED relationships"
