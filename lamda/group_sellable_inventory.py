import json
from datetime import datetime, timedelta
from mysql import connector
from pymongo import MongoClient, UpdateOne
from settings import SHARDED_SEARCH_DB as DB

# CLIENT = MongoClient("mongodb+srv://sharded-search-service:KC2718oU0Jt9Qt7v@search-service.ynzkd.mongodb.net/test")
# DB = CLIENT.product_search


def sync_group_sellable_inventory():
    current_time = datetime.now()
    prev_time = current_time - timedelta(days=2)
    connection = connector.connect(
        host="127.0.0.1",
        user="",
        password="",
    )
    cursor = connection.cursor()
    query = f"SELECT warehouse_id as wh_id, group_id as g_id, product_id as p_id, product_name as name, " \
            f"sum(quantity) as inv_qty FROM niyoweb.group_sellable_inventory where " \
            f"group_sellable_inventory.created_at >= '{prev_time}' and group_sellable_inventory.product_name is not Null" \
            f" group by wh_id, g_id, p_id"
    print(datetime.now(), " :::::: ", query)
    cursor.execute(str(query))
    result = [dict((cursor.description[i][0], value) for i, value in enumerate(row)) for row in cursor.fetchall()]
    print("sql_data_len : ", len(result))
    if result:
        payload_list = []
        for product in result:
            payload = {
                "wh_id": int(product.get("wh_id")),
                "g_id": product.get("g_id"),
                "p_id": product.get("p_id"),
                "name": product.get("name", "").strip(),
                "inv_qty": float(product.get("inv_qty")),
            }
            filter_kwargs = {
                "wh_id": product.get("wh_id"),
                "g_id": product.get("g_id"),
                "p_id": product.get("p_id"),
            }
            payload_list.append(UpdateOne(filter_kwargs, {"$set": payload}, upsert=True))

        if payload_list:
            DB["group_sellable_inventory"].delete_many({})
            DB["group_sellable_inventory"].bulk_write(payload_list)
    return True, "Syncing Done."


def lambda_handler(event, context):
    res, message = sync_group_sellable_inventory()
    return {
        'statusCode': 200,
        'body': json.dumps(message)
    }
