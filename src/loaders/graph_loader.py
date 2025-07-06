from pathlib import Path

import pandas as pd

from src.db.neo4j_client import neo4j_client

ROOT = Path(__file__).resolve().parents[2]
REL = ROOT / "raw_data" / "relational_data"


def _num(code: str) -> int:
    return int("".join(ch for ch in str(code) if ch.isdigit()))


def main() -> None:
    neo4j_client.create_constraints()

    cat_df = pd.read_csv(REL / "categories.csv")
    prod_df = pd.read_csv(REL / "products.csv")
    user_df = pd.read_csv(REL / "users.csv")
    orders_df = pd.read_csv(REL / "orders.csv")
    items_df = pd.read_csv(REL / "order_items.csv")

    with neo4j_client.driver.session() as s:
        # Categories
        for _, row in cat_df.iterrows():
            s.run(
                "MERGE (c:Category {name:$name}) ON CREATE SET c.id=$cid",
                name=row["name"],
                cid=int(row["id"]),
            )

        # Products + BELONGS_TO
        for _, row in prod_df.iterrows():
            pid = _num(row["id"])
            s.run(
                """
                MERGE (p:Product {id:$pid})
                  SET p.name=$name, p.price=$price
                WITH p
                MERGE (c:Category {name:$cat})
                MERGE (p)-[:BELONGS_TO]->(c)
                """,
                pid=pid,
                name=row["name"],
                price=int(float(row["price"]) * 100),  # ← updated line
                cat=row["category"],
            )

        # Users
        for _, row in user_df.iterrows():
            s.run(
                "MERGE (u:User {id:$uid}) SET u.name=$name, u.join_date=$join",
                uid=_num(row["id"]),
                name=row["name"],
                join=row["join_date"],
            )

        # PURCHASED edges
        order_map = orders_df.set_index("id")
        for _, itm in items_df.iterrows():
            order = order_map.loc[itm["order_id"]]
            uid = _num(order["user_id"])
            pid = _num(itm["product_id"])
            s.run(
                """
                MATCH (u:User {id:$uid}), (p:Product {id:$pid})
                MERGE (u)-[r:PURCHASED {date:$d}]->(p)
                ON CREATE SET r.quantity=$q
                ON MATCH  SET r.quantity = r.quantity + $q
                """,
                uid=uid,
                pid=pid,
                d=order["created_at"],
                q=int(itm["quantity"]),
            )

    print("Graph load complete")


if __name__ == "__main__":
    main()
