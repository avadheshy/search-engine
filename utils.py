from pymongo import MongoClient
import csv
from email.mime import image

data_map = {
    "Uniq Id": "product_id",
    "Title": "name",
    "Manufacturer": "manufacturer",
    "Sku": "sku_code",
}


CLIENT = MongoClient(
    "mongodb+srv://searchengine-appuser:qJSjAhUkcAlyuAwy@search-service.ynzkd.mongodb.net/?retryWrites=true&w=majority"
)
DB = CLIENT.search_engine


def insert_file_data_products(collection_name, file_path):

    payload = []
    with open(file_path, mode="r") as file:
        csvFile = csv.DictReader(file)
        for lines in csvFile:
            data = {
                "product_id": lines.get("Uniq Id"),
                "name": lines.get("Title"),
                "manufacturer": lines.get("Manufacturer"),
                "sku_code": lines.get("Sku"),
            }
            payload.append(data)
    DB[collection_name].insert_many(payload)
    return True
