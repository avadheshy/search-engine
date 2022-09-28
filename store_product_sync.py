
#######sync_product_store#######

import json
from mysql import connector
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne

CLIENT = MongoClient(
    "mongodb+srv://searchengine-appuser:qJSjAhUkcAlyuAwy@search-service.ynzkd.mongodb.net/?retryWrites=true&w=majority"
)
DB = CLIENT.product_search

current_time = datetime.now()
prev_time = current_time - timedelta(hours=.25)
connection = connector.connect(
      host="pos-prod-aurora.cluster-ro-crvi1ow7nyif.ap-south-1.rds.amazonaws.com",
      user="nagendra.kumar",
      password="EB91c7lNtPRdG5uD"
    )
cur1 = connection.cursor()
Query1 = "SELECT * FROM  pos.product_store WHERE product_store.updated_at > %s OR product_store.created_at > %s"
cur1.execute(Query1, (prev_time, prev_time, ))
result1 = cur1.fetchall()
connection.close()

# connection = connector.connect(
#       host="pos-prod-aurora.cluster-ro-crvi1ow7nyif.ap-south-1.rds.amazonaws.com",
#       user="nagendra.kumar",
#       password="EB91c7lNtPRdG5uD"
#     )
# cur2 = connection.cursor()
# Query = "SELECT * FROM  pos.inventories WHERE inventories.updated_at > %s"
# cur2.execute(Query, (prev_time,))
# result2 = cur2.fetchall()
# connection.close()
# print("RESULT2", result2)


def sync_product_store(result):

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
    f = "%Y-%m-%d %H:%M:%S"
    data = []
    for res in result:
        d = {}
        for i in range(len(keys1)):
            if keys1[i] == "price":
                d[keys1[i]] = float(res[i]) if res[i] else None
            elif keys1[i] == "wholesale_price":
                d[keys1[i]] = float(res[i]) if res[i] else None
            elif keys1[i] == "old_price":
                d[keys1[i]] = float(res[i]) if res[i] else None    
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
        res["product_id"] = str(res["product_id"])
        res["store_id"] = str(res["store_id"])
        res["panel"] = 'cron'
        payload.append(UpdateOne(query, {"$set": res}, upsert=True))
    if payload:
        DB["product_store"].bulk_write(payload)
    return True, "Syncing was successfull."

def sync_inventories(result):

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
                d[keys[i]] = float(res[i]) if res[i] else None
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
    return True, "Data Synced Successfully."

def lambda_handler(event, context):

    print("function started!")
    if event["trigger"] == "success":
        print("trigger block!")
        result, message = sync_product_store(result1)
    print("function ends!")
    return {"status": True}

