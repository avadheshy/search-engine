
from mysql import connector

from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne
from  settings import  PROD_SQL_PASSWORD,PROD_SQL_USER,PROD_SQL_HOST,SHARDED_SEARCH_DB


current_time = datetime.now()
prev_time = current_time - timedelta(hours=180)

f = "%Y-%m-%d %H:%M:%S"
def sync_mall_data(prev_time):
    connection = connector.connect(
        host=PROD_SQL_HOST,
        user=PROD_SQL_USER,
        password=PROD_SQL_PASSWORD
    )
    cur = connection.cursor()
    Query1 = "SELECT * FROM  pos.product_warehouse_stocks WHERE product_warehouse_stocks.created_at > %s OR product_warehouse_stocks.updated_at > %s"
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
                d[keys1[i]] = float(res[i]) if res[i] else None
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
        res["product_id"]=str(res["product_id"])
        res["warehouse_id"]=str(res["warehouse_id"])
        payload.append(UpdateOne(query, {"$set": res}))
    if payload:
        SHARDED_SEARCH_DB["product_warehouse_stocks"].bulk_write(payload)
    connection.close()
    return True, "Syncing was successfull."

def lambda_handler(event, context):

    print("function started!")
    if event["trigger"] == "success":
        print("trigger block!")
        result, message = sync_mall_data(prev_time)
    print("function ends!")
    return {"status": True}