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
def sync_mall_data(prev_time):
    prev_time = current_time - timedelta(hours=prev_time)
    cur = connection.cursor()
    Query1 = "SELECT * FROM  pos.product_warehouse_stocks WHERE inventories.created_at > %s OR inventories.updated_at > %s"
    cur.execute(Query1, (prev_time,prev_time,))
    result = cur.fetchall()
    keys1 = [
        'id',
        'warehouse_id',
        'product_id',
        'stock',
        'created_at',
        'updated_at'
    ]
    data = []
    for res in result:
        d = {}
        for i in range(len(keys1)):
            if keys1[i] == "stock":
                d[keys1[i]] = float(res[i]) if res[i] else 0.0
            elif keys1[i] == "created_at":
                d[keys1[i]] = res[i].strftime(f) if res[i] else None
            elif keys1[i] == "updated_at":
                d[keys1[i]] = res[i].strftime(f) if res[i] else None
            else:
                d[keys1[i]] = res[i] if res[i] else None
        data.append(d)
    payload = []
    for res in data:
        query = {}
        query["product_id"] = str(res["product_id"])
        query["warehouse_id"] = str(res["warehouse_id"])
        payload.append(UpdateOne(query, {"$set": res}))
    if payload:
        DB["product_warehouse_stocks"].bulk_write(payload)
    connection.close()
    return True, "Syncing was successfull."