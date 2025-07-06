from src.db.mongodb_client import mongo_client
from src.loaders.document_loader import main as load_mongo


def test_collections_filled():
    # load sample JSON after the autouse cleanup fixture has emptied Mongo
    load_mongo()

    for name in (
        "reviews",
        "product_specs",
        "seller_profiles",
        "user_preferences",
    ):
        coll = mongo_client.get_collection(name)
        assert coll.count_documents({}) > 0, f"{name} is empty"
