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
cur = connection.cursor()
current_time = datetime.now()
f = "%Y-%m-%d %H:%M:%S"

def product_ids_sync_for_inventories(strore_id,product_ids):
    cur = connection.cursor()
    Query1 = "SELECT * FROM  pos.inventories WHERE inventories.store_id={} AND inventories.product_id IN {}".format(str(strore_id),tuple(map(str,product_ids)))
    print(Query1)
    cur.execute(Query1)
    result = cur.fetchall()
    data = []
    f = "%Y-%m-%d %H:%M:%S"
    keys = [
        "id",
        "product_id",
        "store_id",
        "quantity",
        "batch_number",
        "unit_cost_price",
        "expiry_date",
        "status",
        "shipment_id",
        "user_id",
        "created_at",
        "updated_at",
    ]
    data = []
    for res in result:
        d = {}
        for i in range(len(keys)):
            if keys[i] == "quantity":
                d[keys[i]] = float(res[i]) if res[i] else 0.0
            elif keys[i] == "unit_cost_price":
                d[keys[i]] = float(res[i]) if res[i] else 0.0
            elif keys[i] == "created_at":
                d[keys[i]] = res[i].strftime(f) if res[i] else None
            elif keys[i] == "updated_at":
                d[keys[i]] = res[i].strftime(f) if res[i] else None
            else:
                d[keys[i]] = res[i] if res[i] else None
        data.append(d)
    
    payload = []
    for res in data:
        query = {}
        query["product_id"] = str(res["product_id"])
        query["store_id"] = str(res["store_id"])
        payload.append(UpdateOne(query, {"$set": res}))
    DB["inventories"].bulk_write(payload)
    print(payload)
    connection.close()
    return True, "Data Synced Successfully."
    
    
    