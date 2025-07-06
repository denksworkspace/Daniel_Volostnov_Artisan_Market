"""Generate ~100 synthetic purchases and load them into Postgres + Neo4j."""

from __future__ import annotations

import datetime as dt
import logging
import random

import pandas as pd
import psycopg2
import psycopg2.extras
from neo4j import GraphDatabase
from pymongo import MongoClient

from src.config import MONGO_CONFIG, NEO4J_CONFIG, POSTGRES_CONFIG

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
rng = random.Random(42)


# ─────────────────────────── helpers ────────────────────────────
def _load_products(conn) -> pd.DataFrame:
    return pd.read_sql_query("SELECT id, category_id, price_cents FROM products", conn)


def _map_cat_name_to_id(conn) -> dict[str, int]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT id, name FROM categories")
        return {r["name"]: r["id"] for r in cur.fetchall()}


def _liked_categories_as_ids(conn) -> dict[int, list[int]]:
    name_to_id = _map_cat_name_to_id(conn)
    coll = MongoClient(MONGO_CONFIG["uri"])[MONGO_CONFIG["database"]].get_collection("user_preferences")
    liked_ids: dict[int, list[int]] = {}
    for doc in coll.find():
        uid = int(doc["user_id"][1:])
        liked_names = doc.get("liked_categories", [])
        liked_ids[uid] = [name_to_id[n] for n in liked_names if n in name_to_id]
    return liked_ids


def _rand_date(join: dt.date) -> str:
    today = dt.date.today()
    return (join + dt.timedelta(days=rng.randint(0, max((today - join).days, 1)))).isoformat()


def _insert_postgres(
    orders: list[tuple[int, int, str]],
    items: list[tuple[int, int, int, int]],
) -> None:
    sql_o = "INSERT INTO orders(id, user_id, created_at) VALUES (%s,%s,%s)"
    sql_i = "INSERT INTO order_items(order_id, product_id, quantity, price_cents) VALUES (%s,%s,%s,%s)"
    with psycopg2.connect(**POSTGRES_CONFIG) as conn:
        cur = conn.cursor()
        cur.executemany(sql_o, orders)
        cur.executemany(sql_i, items)
        conn.commit()


def _insert_neo(purchases: list[tuple[int, int, int, str]]) -> None:
    cypher = """
    MATCH (u:User {id:$uid}), (p:Product {id:$pid})
    MERGE (u)-[r:PURCHASED {date:$d}]->(p)
    ON CREATE SET r.quantity=$q
    ON MATCH  SET r.quantity = r.quantity + $q
    """
    driver = GraphDatabase.driver(
        NEO4J_CONFIG["uri"],
        auth=(NEO4J_CONFIG["user"], NEO4J_CONFIG["password"]),
    )
    with driver.session() as s:
        for uid, pid, qty, d in purchases:
            s.run(cypher, uid=uid, pid=pid, q=qty, d=d)
    driver.close()


# ───────────────────────────── main ─────────────────────────────
def main(total: int = 100) -> None:
    logging.info("loading reference data")
    with psycopg2.connect(**POSTGRES_CONFIG) as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT id, join_date FROM users")
        users = {int(r["id"]): r["join_date"] for r in cur.fetchall()}

        cur.execute("SELECT COALESCE(MAX(id),0) AS maxid FROM orders")
        next_oid = cur.fetchone()["maxid"] + 1

        prods = _load_products(conn)
        liked_by_uid = _liked_categories_as_ids(conn)

    orders, items, neo_tx = [], [], []
    logging.info("generating purchases")

    for _ in range(total):
        uid = rng.choice(list(users.keys()))
        liked_ids = liked_by_uid.get(uid, [])
        date = _rand_date(users[uid])

        picked_pids: set[int] = set()
        in_liked = False

        # pick 1-3 distinct products
        for _ in range(rng.randint(1, 3)):
            pool = prods[prods["category_id"].isin(liked_ids)] if liked_ids and rng.random() < 0.6 else prods
            pr = pool.sample(1).iloc[0]
            pid = int(pr["id"])
            if pid in picked_pids:
                continue
            picked_pids.add(pid)

            if pr["category_id"] in liked_ids:
                in_liked = True

            qty = rng.randint(1, 3)
            items.append((next_oid, pid, qty, int(pr["price_cents"])))
            neo_tx.append((uid, pid, qty, date))

        # ensure at least one liked-category item if user has likes
        if liked_ids and not in_liked:
            pr = prods[prods["category_id"].isin(liked_ids)].sample(1).iloc[0]
            pid = int(pr["id"])
            if pid not in picked_pids:  # keep PK uniqueness
                qty = rng.randint(1, 3)
                items.append((next_oid, pid, qty, int(pr["price_cents"])))
                neo_tx.append((uid, pid, qty, date))

        orders.append((next_oid, uid, date))
        next_oid += 1

    logging.info("inserting %s orders into Postgres", len(orders))
    _insert_postgres(orders, items)

    logging.info("upserting %s PURCHASED edges into Neo4j", len(neo_tx))
    _insert_neo(neo_tx)

    logging.info("purchase generation complete")


if __name__ == "__main__":
    main()
