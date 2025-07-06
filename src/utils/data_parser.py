"""Utilities for parsing CSV data."""

from pathlib import Path

import pandas as pd

RAW_DIR = Path(__file__).resolve().parents[2] / "raw_data" / "relational_data"


class DataParser:
    @staticmethod
    def _read(name: str) -> pd.DataFrame:
        return pd.read_csv(RAW_DIR / f"{name}.csv")

    def parse_categories(self) -> pd.DataFrame:
        return self._read("categories")

    def parse_users(self) -> pd.DataFrame:
        return self._read("users")

    def parse_sellers(self) -> pd.DataFrame:
        return self._read("sellers")

    def parse_products(self) -> pd.DataFrame:
        return self._read("products")

    def parse_orders(self) -> pd.DataFrame:
        return self._read("orders")

    def parse_order_items(self) -> pd.DataFrame:
        return self._read("order_items")

    def parse_product_embeddings(self) -> pd.DataFrame:
        return self._read("product_embeddings")
