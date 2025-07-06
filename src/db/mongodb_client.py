"""MongoDB connection and utilities."""

import atexit
import logging

from pymongo import ASCENDING, DESCENDING, MongoClient, errors
from pymongo.database import Database

from src.config import MONGO_CONFIG

logger = logging.getLogger(__name__)


class MongoDBClient:
    def __init__(self):
        try:
            self.client = MongoClient(
                MONGO_CONFIG["uri"],
                serverSelectionTimeoutMS=5000,
            )
            self.client.admin.command("ping")
        except errors.ServerSelectionTimeoutError as exc:
            logger.critical("MongoDB unavailable: %s", exc)
            raise
        self.db: Database = self.client[MONGO_CONFIG["database"]]
        self._create_indexes()

    def get_collection(self, name: str):
        return self.db[name]

    def close(self):
        self.client.close()

    def _create_indexes(self):
        self.db.reviews.create_index([("product_id", ASCENDING)])
        self.db.reviews.create_index([("user_id", ASCENDING)])
        self.db.reviews.create_index([("rating", DESCENDING), ("created_at", DESCENDING)])
        self.db.product_specs.create_index([("product_id", ASCENDING)], unique=True)
        self.db.seller_profiles.create_index([("seller_id", ASCENDING)], unique=True)
        self.db.user_preferences.create_index([("user_id", ASCENDING)], unique=True)


mongo_client = MongoDBClient()
atexit.register(mongo_client.close)
