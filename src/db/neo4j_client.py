"""Neo4j connection and utilities."""

from neo4j import GraphDatabase

from src.config import NEO4J_CONFIG


class Neo4jClient:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            NEO4J_CONFIG["uri"],
            auth=(NEO4J_CONFIG["user"], NEO4J_CONFIG["password"]),
        )

    def close(self):
        self.driver.close()

    def create_constraints(self):
        with self.driver.session() as s:
            s.run("CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE")
            s.run("CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE")
            s.run("CREATE CONSTRAINT category_id IF NOT EXISTS FOR (c:Category) REQUIRE c.id IS UNIQUE")


neo4j_client = Neo4jClient()
