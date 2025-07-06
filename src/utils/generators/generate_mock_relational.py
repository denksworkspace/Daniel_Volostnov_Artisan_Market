import csv
import datetime
import json
import random
from hashlib import sha1
from pathlib import Path

random.seed(42)

ROOT = Path(__file__).resolve().parents[2] / "raw_data" / "relational_data"
ROOT.mkdir(parents=True, exist_ok=True)


def dump(name: str, rows: list[dict]) -> None:
    if not rows:
        return
    with open(ROOT / f"{name}.csv", "w", newline="", encoding="utf8") as fh:
        writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


# categories --------------------------------------------------------
categories = [{"id": i + 1, "name": f"Category_{i + 1}", "description": f"Desc_{i + 1}"} for i in range(6)]
dump("categories", categories)

# users -------------------------------------------------------------
users = [
    {
        "id": f"U{i + 1:03}",
        "email": f"user{i + 1:03}@mail.local",
        "name": f"User {i + 1}",
        "join_date": datetime.date(2024, 1, 1) + datetime.timedelta(days=random.randint(0, 300)),
    }
    for i in range(30)
]
dump("users", users)

# sellers -----------------------------------------------------------
sellers = [
    {
        "id": f"S{i + 1:03}",
        "name": f"Seller {i + 1}",
        "specialty": random.choice(["wood", "textile", "ceramic", "glass"]),
        "rating": round(random.uniform(3.5, 5.0), 2),
        "joined": datetime.date(2023, 1, 1) + datetime.timedelta(days=random.randint(0, 700)),
    }
    for i in range(45)
]
dump("sellers", sellers)

# products ----------------------------------------------------------
products = []
for i in range(60):
    products.append(
        {
            "id": f"P{i + 1:03}",
            "category": random.choice(categories)["name"],
            "seller_id": random.choice(sellers)["id"],
            "name": f"Product {i + 1}",
            "description": f"Lovely handmade product {i + 1}",
            "price": round(random.uniform(10, 200), 2),
            "tags": "|".join(random.sample(["eco", "vintage", "gift", "modern", "rustic"], 2)),
            "stock": random.randint(1, 50),
        }
    )
dump("products", products)

# orders & order_items ---------------------------------------------
orders, order_items = [], []
oid = 1
for u in users:
    for _ in range(random.randint(0, 4)):
        orders.append(
            {
                "id": f"O{oid:04}",
                "user_id": u["id"],
                "created_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
            }
        )
        for p in random.sample(products, random.randint(1, 3)):
            order_items.append(
                {
                    "order_id": f"O{oid:04}",
                    "product_id": p["id"],
                    "quantity": random.randint(1, 3),
                    "price": p["price"],
                }
            )
        oid += 1
dump("orders", orders)
dump("order_items", order_items)

# product_embeddings -----------------------------------------------
embeddings = []
for p in products:
    vec = [random.uniform(-1, 1) for _ in range(384)]
    embeddings.append(
        {
            "product_id": p["id"],
            "embedding": json.dumps(vec),
            "hash": sha1(json.dumps(vec).encode()).hexdigest(),
        }
    )
dump("product_embeddings", embeddings)

print(f"CSV files generated in {ROOT}")
