from mysql import connector
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne,UpdateMany

CLIENT = MongoClient(
    "mongodb+srv://searchengine-appuser:qJSjAhUkcAlyuAwy@search-service.ynzkd.mongodb.net/?retryWrites=true&w=majority"
)
DB = CLIENT.product_search

def sync_product_tag():
    current_time = datetime.now()
    prev_time = current_time - timedelta(hours=2)
    connection = connector.connect(
        host="pos-prod-aurora.cluster-ro-crvi1ow7nyif.ap-south-1.rds.amazonaws.com",
        user="nagendra.kumar",
        password="EB91c7lNtPRdG5uD"
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
            d[keys[i]] = str(res[i]) if res[i] else None
        data.append(d)

    payload = []
    payload1=[]
    for res in data:
        payload.append(
        UpdateOne({"id": res.get("product_id")}, {"$set": {'$push': {'tag_ids': res.get('tag_id')}}}))
        payload1.append(UpdateMany({"product_id": res.get("product_id")}, {"$set": {'$push': {'tag_ids': res.get('tag_id')}}}))
    if payload:
        DB["search_products"].bulk_write(payload)
    if payload1:
        DB["product_store_sharded"].bulk_write(payload1)

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
        DB["search_products"].bulk_write(payload)
    if payload1:
        DB["product_store_sharded"].bulk_write(payload1)


    return True, "Syncing was successfull."


def lambda_handler(event, context):
    print("function started!")
    result, message = sync_product_tag()
    print("function ends!")
    return {"status": True}
