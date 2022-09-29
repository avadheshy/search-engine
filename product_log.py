
#######sync_product_store#######

from mysql import connector
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne

CLIENT = MongoClient(
    "mongodb+srv://searchengine-appuser:qJSjAhUkcAlyuAwy@search-service.ynzkd.mongodb.net/?retryWrites=true&w=majority"
)
DB = CLIENT.product_search

def product_log_check():
    connection = connector.connect(
        host="pos-prod-aurora.cluster-ro-crvi1ow7nyif.ap-south-1.rds.amazonaws.com",
        user="nagendra.kumar",
        password="EB91c7lNtPRdG5uD"
    )
    cur = connection.cursor()
    Query1 = "SELECT COUNT(*) FROM  pos.product_store;"
    cur.execute(Query1)
    sql_result1 = cur.fetchall()
    Query2 = "SELECT COUNT(*) FROM  pos.inventories;"
    cur.execute(Query1)
    sql_result2 = cur.fetchall()
    Query3 = "SELECT COUNT(*) FROM  pos.products;"
    cur.execute(Query1)
    sql_result3 = cur.fetchall()
    Query4 = "SELECT COUNT(*) FROM  pos.product_warehouse_stocks;"
    cur.execute(Query1)
    sql_result4 = cur.fetchall()
    # mongodb result
    mongo_result1=DB['product_store'].count()
    mongo_result2=DB['inventories'].count()
    mongo_result3=DB['products'].count()
    mongo_result4=DB['product_warehouse_stocks'].count()
    print(f"sql product_store count is {sql_result1[0][0]} and mongo product_store count is {mongo_result1}")
    print(f"sql inventories count is {sql_result2[0][0]} and mongo inventories count is {mongo_result2}")
    print(f"sql products count is {sql_result3[0][0]} and mongo products count is {mongo_result3}")
    print(f"sql product_warehouse_stocks count is {sql_result4[0][0]} and mongo product_warehouse_stocks count is {mongo_result4}")
    




    



