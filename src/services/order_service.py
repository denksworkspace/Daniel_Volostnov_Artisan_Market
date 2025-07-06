from contextlib import contextmanager

from src.db.postgres_client import db


@contextmanager
def pg_session():
    Session = db.session_factory
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def create_order(user_id: int, items: list[dict]):
    """
    items = [{"product_id": 12, "qty": 2}, …]
    """
    with pg_session() as s:
        order_id = s.execute(
            "INSERT INTO orders(user_id) VALUES (:u) RETURNING id",
            {"u": user_id},
        ).scalar_one()

        rows = [
            {
                "order_id": order_id,
                "product_id": it["product_id"],
                "quantity": it["qty"],
                "price_cents": s.execute(
                    "SELECT price_cents FROM products WHERE id = :pid",
                    {"pid": it["product_id"]},
                ).scalar_one(),
            }
            for it in items
        ]
        s.execute(
            """
            INSERT INTO order_items(order_id, product_id, quantity, price_cents)
            VALUES (:order_id, :product_id, :quantity, :price_cents)
            """,
            rows,
        )
    return order_id
