import json
from pathlib import Path

import pandas as pd

from src.db.mongodb_client import mongo_client


def project_root(start: Path) -> Path:
    for p in [start, *start.parents]:
        if (p / "pyproject.toml").exists():
            return p
    raise RuntimeError("project root not found")


ROOT = project_root(Path(__file__).resolve())
DOC = ROOT / "raw_data" / "document_store"

MAP = {
    "reviews": "reviews",
    "product_specs": "product_specs",
    "seller_profiles": "seller_profiles",
    "user_preferences": "user_preferences",
}


def read_records(stem: str):
    j = DOC / f"{stem}.json"
    if j.exists():
        return json.loads(j.read_text(encoding="utf8"))
    c = DOC / f"{stem}.csv"
    if c.exists():
        return pd.read_csv(c).to_dict("records")
    return []


def main():
    for coll, stem in MAP.items():
        docs = read_records(stem)
        if docs:
            mongo_client.get_collection(coll).insert_many(docs)
            print(f"{coll} → {len(docs)} docs")
    print("Mongo load complete")


if __name__ == "__main__":
    main()
