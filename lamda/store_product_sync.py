import json
from mysql import connector
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne
from  settings import  USER,HOST,PASSWORD,SHARDED_SEARCH_DB





def sync_product_store():
    current_time = datetime.now()
    prev_time = current_time - timedelta(minutes=10)
    connection = connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD
    )
    cur1 = connection.cursor()
    Query1 = "SELECT * FROM  pos.product_store WHERE product_store.updated_at >= %s OR product_store.created_at >= %s"
    cur1.execute(Query1, (prev_time, prev_time, ))
    result = cur1.fetchall()
    connection.close()

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
            d[keys1[i]] = str(res[i]) if res[i] else None
        data.append(d)
    payload = []
    product_ids = []
    for res in data:
        query = {}
        query["product_id"] = str(res["product_id"])
        product_ids.append(str(res["product_id"]))
        query["store_id"] = str(res["store_id"])
        res["product_id"] = str(res["product_id"])
        res["store_id"] = str(res["store_id"])
        res["status"] = str(res["status"])
        res["sale_pos"] = str(res["sale_pos"])
        res["sale_app"] = str(res["sale_app"])
        res["panel"] = 'cron'
    p_data = SHARDED_SEARCH_DB["products"].find({'id': {'$in': product_ids}})
    p_data_map = {}
    for i in p_data:
        is_mall = "1" if i.get("is_mall") in [1, "1"] else "0"
        p_data_map[i.get('id')] = i.get('name'), i.get('barcode'), is_mall, i.get('group_id'), i.get('brand_id'), i.get('category_id')
    for resp in data:
        query = {"product_id": resp.get("product_id"), "store_id": resp.get("store_id")}
        resp['name'], resp['barcode'], resp['is_mall'], resp['group_id'], resp['brand_id'], resp['category_id'] = p_data_map.get(resp.get('product_id'))
        payload.append(UpdateOne(query, {"$set": resp}, upsert=True))
    if payload:
        print(payload)
        SHARDED_SEARCH_DB["product_store_sharded"].bulk_write(payload)
    return True, "Syncing was successfull."

def lambda_handler(event, context):

    print("function started!")
    result, message = sync_product_store()
    print("function ends!")
    return {"status": True}

