from pymongo import UpdateOne, MongoClient
from app import DB




def get_boosting_products(product_ids):
    payload = []
    for product in product_ids:
        payload.append(UpdateOne({'id': product.get('id')}, {
                       '$set': {'Winter_sell': '1'}}))
    if payload:
        DB['products'].bulk_write(payload)
