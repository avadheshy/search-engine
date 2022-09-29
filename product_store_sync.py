from mysql import connector
import mysql

from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne

CLIENT = MongoClient(
    "mongodb+srv://searchengine-appuser:qJSjAhUkcAlyuAwy@search-service.ynzkd.mongodb.net/?retryWrites=true&w=majority"
)
DB = CLIENT.search

connection = mysql.connector.connect(
    host="127.0.0.1",
    database="pos",
    port=3310,
    user="nagendra.kumar",
    password="hwkUBlUpdM0G7ORQ",
)

current_time = datetime.now()
prev_time = current_time - timedelta(hours=180)

f = "%Y-%m-%d %H:%M:%S"
def sync_product_store(prev_time):
    prev_time = current_time - timedelta(hours=prev_time)
    cur = connection.cursor()
    Query1 = "SELECT * FROM  pos.inventories WHERE inventories.store_id > %s"
    cur.execute(Query1, (prev_time,))
    result = cur.fetchall()
    keys1 = [
        "id",
        "store_id",
        "product_id",
        "price",
        "wholesale_price",
        "wholesale_moq",
        "old_price",
        "fast_sale",
        "status",
        "sale_app",
        "sale_pos",
        "auto_pricing_at",
        "created_at",
        "updated_at",
    ]
    data = []
    for res in result:
        d = {}
        for i in range(len(keys1)):
            if keys1[i] == "price":
                d[keys1[i]] = float(res[i]) if res[i] else 0.0
            elif keys1[i] == "wholesale_price":
                d[keys1[i]] = float(res[i]) if res[i] else 0.0
            elif keys1[i]=='old_price':
                d[keys1[i]]=float(res[i]) if res[i] else 0.0
            elif keys1[i]=='sale_app':
                d[keys1[i]]=str(res[i]) if res[i] else "0"
            elif keys1[i]=='sale_pos':
                d[keys1[i]]=float(res[i]) if res[i] else "0"
            elif keys1[i] == "created_at":
                d[keys1[i]] = res[i].strftime(f) if res[i] else None
            elif keys1[i] == "updated_at":
                d[keys1[i]] = res[i].strftime(f) if res[i] else None
            elif keys1[i] == "auto_pricing_at":
                d[keys1[i]] = res[i].strftime(f) if res[i] else None
            else:
                d[keys1[i]] = res[i] if res[i] else None
        data.append(d)
    payload = []
    for res in data:
        query = {}
        query["product_id"] = str(res["product_id"])
        query["store_id"] = str(res["store_id"])
        payload.append(UpdateOne(query, {"$set": res}))
    DB["product_store"].bulk_write(payload)
    connection.close()
    return True, "Syncing was successfull."
