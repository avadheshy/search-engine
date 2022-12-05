########sync_inventories########

import json
from mysql import connector
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne
from settings import POS_SQL_PASSWORD, POS_SQL_USER, POS_SQL_HOST, SHARDED_SEARCH_DB

current_time = datetime.now()
prev_time = current_time - timedelta(hours=2)
connection = connector.connect(
    host=POS_SQL_HOST,
    user=POS_SQL_USER,
    password=POS_SQL_PASSWORD
)
cur2 = connection.cursor()
Query = "SELECT * FROM  pos.inventories WHERE inventories.updated_at > %s OR inventories.created_at > %s"
cur2.execute(Query, (prev_time, prev_time,))
result2 = cur2.fetchall()
connection.close()
INVENTORY_COUNT = [
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
    }
]


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
            elif keys[i] == "unit_cost_price":
                d[keys[i]] = float(res[i]) if res[i] else None
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
        query["batch_number"] = str(res["batch_number"])
        query["product_id"] = str(res["product_id"])
        query["store_id"] = str(res["store_id"])
        res["batch_number"] = str(res["batch_number"])
        res["product_id"] = str(res["product_id"])
        res["store_id"] = str(res["store_id"])
        p_ids.append(str(res["product_id"]))
        store_ids.append(str(res["store_id"]))
        res["panel"] = "cron"
        payload.append(UpdateOne(query, {"$set": res}, upsert=True))
    if payload:
        SHARDED_SEARCH_DB["inventories"].bulk_write(payload)
    INVENTORY_PIPELINE = [{"$match": {"store_id": {"$in": store_ids}, "product_id": {"$in": p_ids}}}] + INVENTORY_COUNT
    inventory_count = SHARDED_SEARCH_DB["inventories"].aggregate(INVENTORY_PIPELINE)
    update_product_payload = []
    for inv_count in inventory_count:
        update_product_payload.append(
            UpdateOne({'product_id': inv_count.get('product_id'), 'store_id': inv_count.get('store_id')},
                      {"$set": {"inv_qty": inv_count.get("inv_qty"), "qty_source": "cron"}}))
    SHARDED_SEARCH_DB['product_store'].bulk_write(update_product_payload)
    return True, "Data Synced Successfully."


def lambda_handler(event, context):
    print("function started!")
    print("trigger block!")
    print(result2)
    sync_inventories(result2)
    print("function ends!")
    return {"status": True}
