import datetime
import json
import random
from pathlib import Path

import pandas as pd


def project_root(start: Path) -> Path:
    for p in [start, *start.parents]:
        if (p / "pyproject.toml").exists():
            return p
    raise RuntimeError("project root with pyproject.toml not found")


ROOT = project_root(Path(__file__).resolve())
REL = ROOT / "raw_data" / "relational_data"
DOC = ROOT / "raw_data" / "document_store"
DOC.mkdir(parents=True, exist_ok=True)

products_df = pd.read_csv(REL / "products.csv")
users_df = pd.read_csv(REL / "users.csv")
sellers_df = pd.read_csv(REL / "sellers.csv")
categories = products_df["category"].unique().tolist()
rng = random.Random(42)


def dump(stem: str, data):
    (DOC / f"{stem}.json").write_text(json.dumps(data, indent=2), encoding="utf8")


# ── reviews ─────────────────────────────────────────────────────────────
reviews = []
for _ in range(120):
    reviews.append(
        {
            "product_id": rng.choice(products_df["id"]),
            "user_id": rng.choice(users_df["id"]),
            "rating": rng.randint(3, 5),
            "title": rng.choice(["Great", "Nice", "Could be better"]),
            "content": "Auto-generated review text.",
            "images": [],
            "helpful_votes": rng.randint(0, 20),
            "verified_purchase": rng.choice([True, False]),
            "created_at": datetime.datetime.utcnow().isoformat() + "Z",
            "comments": [
                {
                    "user_id": rng.choice(users_df["id"]),
                    "content": "Agree!",
                    "created_at": datetime.datetime.utcnow().isoformat() + "Z",
                }
                for _ in range(rng.randint(0, 2))
            ],
        }
    )
dump("reviews", reviews)

# ── product_specs ───────────────────────────────────────────────────────
specs = []
for _, row in products_df.iterrows():
    specs.append(
        {
            "product_id": row["id"],
            "category": row["category"],
            "specs": {
                "material": rng.choice(["Wood", "Cotton", "Clay", "Glass"]),
                "dimensions": {
                    "length": rng.randint(5, 30),
                    "width": rng.randint(5, 30),
                    "height": rng.randint(1, 10),
                    "unit": "cm",
                },
                "care_instructions": ["Keep dry", "Hand wash only"],
                "capacity": f"{rng.randint(1, 5)} liters",
            },
        }
    )
dump("product_specs", specs)

# ── seller_profiles ─────────────────────────────────────────────────────
profiles = []
for _, row in sellers_df.iterrows():
    profiles.append(
        {
            "seller_id": row["id"],
            "bio": "Handmade artisan.",
            "portfolio": [f"https://picsum.photos/seed/{row['id']}/{rng.randint(200, 300)}"],
            "rating": row["rating"],
        }
    )
dump("seller_profiles", profiles)

# ── user_preferences ────────────────────────────────────────────────────
prefs = []
for _, row in users_df.iterrows():
    prefs.append(
        {
            "user_id": row["id"],
            "liked_categories": rng.sample(categories, rng.randint(1, 3)),
            "recent_views": rng.sample(products_df["id"].tolist(), 5),
        }
    )
dump("user_preferences", prefs)

print(f"JSON docs written to {DOC}")
