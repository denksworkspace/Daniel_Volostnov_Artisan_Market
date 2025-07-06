"""Load CSV data from raw_data/relational_data into PostgreSQL."""

from pathlib import Path

import pandas as pd

from src.db.postgres_client import db

DATA_DIR = Path(__file__).resolve().parents[2] / "raw_data" / "relational_data"


def _num(code: str) -> int:
    return int("".join(c for c in str(code) if c.isdigit()))


class RelationalLoader:
    def __init__(self) -> None:
        self.cur = db.get_cursor

    @staticmethod
    def _csv(stem: str) -> pd.DataFrame:
        return pd.read_csv(DATA_DIR / f"{stem}.csv")

    def _bulk(self, sql: str, rows: list[dict]) -> None:
        if not rows:
            return
        with self.cur() as cur:
            cur.executemany(sql, rows)

    # ───────────────────────── tables ─────────────────────────

    def load_categories(self) -> None:
        df = self._csv("categories")
        rows = df[["name", "description"]].to_dict("records")
        self._bulk(
            "INSERT INTO categories (name, description) "
            "VALUES (%(name)s, %(description)s) "
            "ON CONFLICT (name) DO NOTHING",
            rows,
        )

    def load_users(self) -> None:
        df = self._csv("users")
        rows = df.rename(columns={"name": "full_name"})[["email", "full_name", "join_date"]].to_dict("records")
        self._bulk(
            "INSERT INTO users (email, full_name, join_date) "
            "VALUES (%(email)s, %(full_name)s, %(join_date)s) "
            "ON CONFLICT (email) DO NOTHING",
            rows,
        )

    def load_sellers(self) -> None:
        df = self._csv("sellers")
        df["email"] = df["id"].str.lower() + "@seller.local"
        user_rows = df.rename(columns={"name": "full_name", "joined": "join_date"})[
            ["email", "full_name", "join_date"]
        ].to_dict("records")
        self._bulk(
            "INSERT INTO users (email, full_name, join_date) "
            "VALUES (%(email)s, %(full_name)s, %(join_date)s) "
            "ON CONFLICT (email) DO NOTHING",
            user_rows,
        )
        with self.cur() as cur:
            cur.execute("SELECT id, email FROM users WHERE email LIKE '%@seller.local'")
            email_uid = {r["email"]: r["id"] for r in cur.fetchall()}
        seller_rows = [{"user_id": email_uid[r["email"]], "rating": r["rating"]} for r in df.to_dict("records")]
        self._bulk(
            "INSERT INTO sellers (user_id, rating) VALUES (%(user_id)s, %(rating)s) ON CONFLICT (user_id) DO NOTHING",
            seller_rows,
        )

    def load_products(self) -> None:
        df = self._csv("products")
        self._bulk(
            "INSERT INTO categories (name) VALUES (%(name)s) ON CONFLICT (name) DO NOTHING",
            df[["category"]].drop_duplicates().rename(columns={"category": "name"}).to_dict("records"),
        )
        with self.cur() as cur:
            cur.execute("SELECT id, name FROM categories")
            cat_map = {r["name"]: r["id"] for r in cur.fetchall()}
            cur.execute("SELECT s.id, u.email FROM sellers s JOIN users u ON u.id = s.user_id")
            seller_map = {r["email"].split("@")[0].upper(): r["id"] for r in cur.fetchall()}
        rows = [
            {
                "category_id": cat_map[r["category"]],
                "seller_id": seller_map[r["seller_id"]],
                "name": r["name"],
                "description": r["description"],
                "price_cents": int(float(r["price"]) * 100),
            }
            for _, r in df.iterrows()
        ]
        self._bulk(
            "INSERT INTO products "
            "(category_id, seller_id, name, description, price_cents) "
            "VALUES "
            "(%(category_id)s, %(seller_id)s, %(name)s, %(description)s, %(price_cents)s) "
            "ON CONFLICT (name) DO NOTHING",
            rows,
        )

    def load_orders(self) -> None:
        df = self._csv("orders")
        df["id"] = df["id"].map(_num)
        df["user_id"] = df["user_id"].map(_num)
        rows = df.to_dict("records")
        self._bulk(
            "INSERT INTO orders (id, user_id, created_at) "
            "VALUES (%(id)s, %(user_id)s, %(created_at)s) "
            "ON CONFLICT (id) DO NOTHING",
            rows,
        )

    def load_order_items(self) -> None:
        df = self._csv("order_items")
        df["order_id"] = df["order_id"].map(_num)
        df["product_id"] = df["product_id"].map(_num)
        df.rename(columns={"price": "price_cents"}, inplace=True)
        df["price_cents"] = (df["price_cents"] * 100).astype(int)
        rows = df.to_dict("records")
        self._bulk(
            "INSERT INTO order_items "
            "(order_id, product_id, quantity, price_cents) "
            "VALUES "
            "(%(order_id)s, %(product_id)s, %(quantity)s, %(price_cents)s) "
            "ON CONFLICT DO NOTHING",
            rows,
        )

    def load_product_embeddings(self) -> None:
        df = self._csv("product_embeddings")
        df["product_id"] = df["product_id"].map(_num)
        rows = [{"product_id": r["product_id"], "embedding": r["embedding"]} for r in df.to_dict("records")]
        self._bulk(
            "INSERT INTO product_embeddings (product_id, embedding) "
            "VALUES (%(product_id)s, %(embedding)s::vector) "
            "ON CONFLICT (product_id) DO NOTHING",
            rows,
        )

    # ───────────────────────── entrypoint ─────────────────────────

    def run(self) -> None:
        self.load_categories()
        self.load_users()
        self.load_sellers()
        self.load_products()
        self.load_orders()
        self.load_order_items()
        self.load_product_embeddings()
        print("Relational load complete")


if __name__ == "__main__":
    RelationalLoader().run()
