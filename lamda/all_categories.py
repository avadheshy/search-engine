from mysql import connector
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne, UpdateMany

CLIENT = MongoClient(
    "mongodb+srv://searchengine-appuser:qJSjAhUkcAlyuAwy@search-service.ynzkd.mongodb.net/?retryWrites=true&w=majority"
)
DB = CLIENT.product_search


def sync_all_categories():
    current_time = datetime.now()
    prev_time = current_time - timedelta(days=15)
    connection = connector.connect(
        host="pos-prod-aurora.cluster-ro-crvi1ow7nyif.ap-south-1.rds.amazonaws.com",
        user="nagendra.kumar",
        password="EB91c7lNtPRdG5uD"
    )
    cur1 = connection.cursor()
    Query1 = "SELECT * FROM  pos.all_categories WHERE all_categories.updated_at > %s OR all_categories.created_at > %s"
    cur1.execute(Query1, (prev_time, prev_time))
    result = cur1.fetchall()
    connection.close()
    keys = [
        "id",
        "name",
        "icon",
        "cat_level",
        "cl4_id",
        "cl4_name",
        "cl3_id",
        "cl3_name",
        "cl2_id",
        "cl2_name",
        "cl1_id",
        "cl1_name",
        "created_at",
        "updated_at"
    ]
    data = []
    for res in result:
        d = {}
        for i in range(len(keys)):
            d[keys[i]] = str(res[i]) if res[i] else None
        data.append(d)
    payload = []
    for res in data:
        payload.append(
            UpdateMany({'category_id': int(res.get('id'))}, {"$set": {'cat_level': res.get('cat_level')}}))

    if payload:
        DB["search_products"].bulk_write(payload)
        DB["product_store_sharded"].bulk_write(payload)
    return True, "Syncing was successfull."


def lambda_handler(event, context):

    print("function started!")
    result, message = sync_all_categories()
    print("function ends!")
    return {"status": True}