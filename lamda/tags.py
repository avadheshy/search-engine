from mysql import connector
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne, UpdateMany
from settings import POS_SQL_HOST, POS_SQL_PASSWORD, POS_SQL_USER, SHARDED_SEARCH_DB


def sync_product_tag():
    current_time = datetime.now()
    prev_time = current_time - timedelta(hours=2)
    connection = connector.connect(
        host=POS_SQL_HOST,
        user=POS_SQL_USER,
        password=POS_SQL_PASSWORD
    )
    cur = connection.cursor()
    Query1 = "SELECT * FROM  pos.product_tag WHERE product_tag.updated_at > %s OR product_tag.created_at > %s"
    cur.execute(Query1, (prev_time, prev_time))
    result1 = cur.fetchall()
    Query2 = "SELECT * FROM  pos.product_tag WHERE product_tag.deleted_at> %s"
    cur.execute(Query2, (prev_time,))
    result2 = cur.fetchall()
    connection.close()
    keys = [
        "id",
        "product_id",
        "tag_id",
        "created_at",
        "updated_at",
        "deleted_at"
    ]
    data = []
    for res in result1:
        d = {}
        for i in range(len(keys)):
            d[keys[i]] = str(res[i]) if res.get(i) else None
        data.append(d)

    payload = []
    payload1 = []
    for res in data:
        payload.append(
            UpdateOne({"id": res.get("product_id")}, {"$set": {'$push': {'tag_ids': res.get('tag_id')}}}))
        payload1.append(
            UpdateMany({"product_id": res.get("product_id")}, {"$set": {'$push': {'tag_ids': res.get('tag_id')}}}))
    if payload:
        SHARDED_SEARCH_DB["search_products"].bulk_write(payload)
    if payload1:
        SHARDED_SEARCH_DB["product_store_sharded"].bulk_write(payload1)

    data = []
    for res in result2:
        d = {}
        for i in range(len(keys)):
            d[keys[i]] = str(res[i]) if res[i] else None
        data.append(d)

    payload = []
    payload1 = []
    for res in data:
        payload.append(
            UpdateOne({"id": res.get("product_id")}, {"$set": {'$pull': {'tag_ids': res.get('tag_id')}}}))
        payload1.append(
            UpdateMany({"product_id": res.get("product_id")}, {"$set": {'$pull': {'tag_ids': res.get('tag_id')}}}))
    if payload:
        SHARDED_SEARCH_DB["search_products"].bulk_write(payload)
    if payload1:
        SHARDED_SEARCH_DB["product_store_sharded"].bulk_write(payload1)

    return True, "Syncing was successfull."


def lambda_handler(event, context):
    print("function started!")
    result, message = sync_product_tag()
    print("function ends!")
    return {"status": True}
