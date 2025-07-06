"""Create/refresh 384-dim pgvector embeddings for every product description."""

from pathlib import Path

import pandas as pd
import psycopg2
from sentence_transformers import SentenceTransformer

from src.config import POSTGRES_CONFIG

ROOT = Path(__file__).resolve().parents[2]
REL = ROOT / "raw_data" / "relational_data" / "products.csv"

MODEL = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


def product_id(code: str) -> int:
    """Convert 'P015' -> 15."""
    return int("".join(c for c in code if c.isdigit()))


def main() -> None:
    df = pd.read_csv(REL)[["id", "description"]]
    texts = df["description"].fillna("").tolist()
    embeds = MODEL.encode(texts, normalize_embeddings=True)

    sql = """
        INSERT INTO product_embeddings(product_id, embedding)
        VALUES (%s, %s::vector)
        ON CONFLICT (product_id)
        DO UPDATE SET embedding = EXCLUDED.embedding
    """

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()

    for row, vec in zip(df.itertuples(index=False), embeds, strict=False):
        pid = product_id(row.id)  # row.id is e.g. 'P015'
        cur.execute(sql, (pid, vec.tolist()))

    conn.commit()
    cur.close()
    conn.close()
    print("vector load complete")


if __name__ == "__main__":
    main()
