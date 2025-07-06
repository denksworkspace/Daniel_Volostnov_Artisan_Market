import psycopg2
import pytest
from neo4j import GraphDatabase
from pymongo import MongoClient
from redis import Redis

from src.config import MONGO_CONFIG, NEO4J_CONFIG, POSTGRES_CONFIG, REDIS_CONFIG


@pytest.fixture(autouse=True, scope="function")
def postgres_clean():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    cur.execute("BEGIN")
    yield
    conn.rollback()
    conn.close()


@pytest.fixture(autouse=True, scope="function")
def redis_clean():
    r = Redis(**REDIS_CONFIG)
    r.flushdb()
    yield
    r.flushdb()


@pytest.fixture(autouse=True, scope="function")
def mongo_clean():
    client = MongoClient(MONGO_CONFIG["uri"])
    db_name = MONGO_CONFIG["database"]
    client.drop_database(db_name)
    yield
    client.drop_database(db_name)
    client.close()


@pytest.fixture(autouse=True, scope="function")
def neo4j_clean():
    driver = GraphDatabase.driver(
        NEO4J_CONFIG["uri"],
        auth=(NEO4J_CONFIG["user"], NEO4J_CONFIG["password"]),
    )
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    yield
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    driver.close()
