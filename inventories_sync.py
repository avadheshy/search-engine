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


def sync_inventories(prev_time):
    cur = connection.cursor()
    Query = "SELECT * FROM  pos.inventories WHERE inventories.updated_at > %s"
    cur.execute(Query, (prev_time,))
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
    p_ids = []
    store_ids = []
    
    for res in data:
        query = {}
        query["product_id"] = str(res["product_id"])
        query["store_id"] = str(res["store_id"])
        p_ids.append(str(res["product_id"]))
        store_ids.append(str(res["store_id"]))
        payload.append(UpdateOne(query, {"$set": res}))
    DB["inventories"].bulk_write(payload)
    DB["inventories"].aggregate([
    {"$match": {"product_id": {"$in": p_ids}, "store_id": {"$in": store_ids}}},
    {
        '$project': {
            '_id': 0, 
            'product_id': 1, 
            'store_id': 1, 
            'quantity': {
                '$toDouble': '$quantity'
            }
        }
    }, {
        '$match': {
            'quantity': {
                '$gte': 1
            }
        }
    }, {
        '$group': {
            '_id': {
                'store_id': '$store_id', 
                'product_id': '$product_id'
            }, 
            'data': {
                '$push': '$$ROOT'
            }
        }
    }, {
        '$project': {
            'store_id': '$_id.store_id', 
            'product_id': '$_id.product_id', 
            'inv_qty': {
                '$sum': '$data.quantity'
            }, 
            '_id': 0
        }
    }])
    DB['product_store'].bulk_write()
    connection.close()
    return True, "Data Synced Successfully."
