"""High-level recommendation queries powered by Neo4j."""

from typing import Any

from src.db.neo4j_client import neo4j_client


class RecommendationService:
    def also_bought(self, product_id: int, limit: int = 5) -> list[dict[str, Any]]:
        query = """
        MATCH (p1:Product {id:$pid})<-[:PURCHASED]-(:User)-[:PURCHASED]->(p2:Product)
        WHERE p1 <> p2
        WITH p2, COUNT(*) AS freq
        RETURN p2.id AS id, p2.name AS name, freq
        ORDER BY freq DESC
        LIMIT $lim
        """
        with neo4j_client.driver.session() as s:
            return [dict(r) for r in s.run(query, pid=product_id, lim=limit)]

    def frequently_bought_together(self, product_id: int, limit: int = 5):
        query = """
        MATCH (:Product {id:$pid})<-[:PURCHASED]-(u:User)-[:PURCHASED]->(p:Product)
        WITH p, COUNT(*) AS together
        ORDER BY together DESC
        LIMIT $lim
        RETURN p.id AS id, p.name AS name, together
        """
        with neo4j_client.driver.session() as s:
            return [dict(r) for r in s.run(query, pid=product_id, lim=limit)]

    def personalized(self, user_id: int, limit: int = 5):
        query = """
        MATCH (u:User {id:$uid})-[:PURCHASED]->(p1:Product)<-[:PURCHASED]-(o:User)-[:PURCHASED]->(p2:Product)
        WHERE NOT (u)-[:PURCHASED]->(p2)
        WITH p2, COUNT(*) AS score
        ORDER BY score DESC
        LIMIT $lim
        RETURN p2.id AS id, p2.name AS name, score
        """
        with neo4j_client.driver.session() as s:
            return [dict(r) for r in s.run(query, uid=user_id, lim=limit)]


recommendation_service = RecommendationService()
