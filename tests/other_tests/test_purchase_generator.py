"""Validate purchase_generator realism & DB inserts."""

# rebuild mapping locally (only uses Mongo + categories table)
from pymongo import MongoClient

from src.config import MONGO_CONFIG
from src.db.postgres_client import db
from src.utils.generators import purchase_generator


def _liked_categories() -> dict[int, list[int]]:
    coll = MongoClient(MONGO_CONFIG["uri"])[MONGO_CONFIG["database"]].get_collection("user_preferences")
    with db.get_cursor() as cur:
        cur.execute("SELECT id, name FROM categories")
        name_to_id = {r["name"]: r["id"] for r in cur.fetchall()}
    return {
        int(d["user_id"][1:]): [name_to_id[n] for n in d.get("liked_categories", []) if n in name_to_id]
        for d in coll.find()
    }


def test_purchase_generator_realism():
    TOTAL = 20  # keep test fast

    # baseline counts
    with db.get_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM orders")
        before = cur.fetchone()["c"]

    # run generator
    purchase_generator.main(total=TOTAL)

    # ══════════ assertions ══════════
    likes = _liked_categories()  # {user_id: [category,..]}

    with db.get_cursor() as cur:
        # 1. row-count increase
        cur.execute("SELECT COUNT(*) AS c FROM orders")
        after = cur.fetchone()["c"]
        assert after - before == TOTAL

        # 2. quantity distribution 1-3
        cur.execute("SELECT MIN(quantity) AS mi, MAX(quantity) AS ma FROM order_items")
        q = cur.fetchone()
        assert 1 <= q["mi"] <= q["ma"] <= 3

        # 3. join_date respected
        cur.execute(
            """
            SELECT 1
            FROM orders o
            JOIN users u ON u.id = o.user_id
            WHERE o.created_at < u.join_date
            LIMIT 1
            """
        )
        assert cur.fetchone() is None, "order before user join date detected"

        # 4. items aligned with user interests (only if user HAS interests)
        cur.execute(
            """
            SELECT o.user_id, p.category_id
            FROM order_items oi
            JOIN orders o ON o.id = oi.order_id
            JOIN products p ON p.id = oi.product_id
            """
        )
        interest_rows = [
            (uid, cid)
            for uid, cid in cur.fetchall()
            if likes.get(uid)  # user has preferences
        ]

        # count matches among those rows
        matches = sum(1 for uid, cid in interest_rows if cid in likes[uid])

        # only assert when there were any “interested” rows
        if interest_rows:
            assert matches > 0, "no items aligned with user interests"
