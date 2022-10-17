from pymongo import UpdateOne, MongoClient

CLIENT = MongoClient(
    "mongodb+srv://sharded-search-service:KC2718oU0Jt9Qt7v@search-service.ynzkd.mongodb.net/test")
DB = CLIENT.product_search


def get_boosting_products(product_ids):
    payload = []
    for product in product_ids:
        payload.append(UpdateOne({'id': product.get('id')}, {
                       '$set': {'Winter_sell': '1'}}))
    if payload:
        DB['products'].bulk_write(payload)
