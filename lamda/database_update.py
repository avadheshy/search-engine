import json
from mysql import connector
from datetime import datetime, timedelta
from pymongo import MongoClient


def lambda_handler(event, context):
    # TODO implement
    #######sync_product_store#######

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

        Query2 = "SELECT id FROM pos.inventories ORDER BY id DESC LIMIT 1;"
        cur.execute(Query2)
        sql_result2 = cur.fetchall()

        Query3 = "SELECT COUNT(*) FROM  pos.products;"
        cur.execute(Query3)
        sql_result3 = cur.fetchall()

        Query4 = "SELECT COUNT(*) FROM  pos.product_warehouse_stocks;"
        cur.execute(Query4)
        sql_result4 = cur.fetchall()

        Query5 = "SELECT COUNT(*) FROM  pos.brands;"
        cur.execute(Query5)
        sql_result5 = cur.fetchall()
        Query6 = "SELECT COUNT(*) FROM  pos.all_categories;"
        cur.execute(Query6)
        sql_result6 = cur.fetchall()
        connection.close()

        # mongodb result
        mongo_result1 = DB['product_store_sharded'].aggregate([{"$count": "count"}]).next()
        mongo_result2 = DB['inventories'].aggregate([
            {
                '$project': {
                    '_id': 0,
                    'id': {
                        '$toInt': '$id'
                    }
                }
            }, {
                '$sort': {
                    'id': -1
                }
            }, {
                '$limit': 1
            }
        ]).next()
        mongo_result3 = DB['products'].aggregate([{"$count": "count"}]).next()
        mongo_result4 = DB['product_warehouse_stocks'].aggregate([{"$count": "count"}]).next()
        mongo_result5 = DB['brands'].aggregate([{"$count": "count"}]).next()
        mongo_result6 = DB['all_categories'].aggregate([{"$count": "count"}]).next()
        a = {"product_store": sql_result1[0][0]}, {"product_store": mongo_result1.get("count")}
        b = {"last - inserted id - inventories": sql_result2[0][0]}, {
            "last - inserted id - inventories": mongo_result2.get("id")}
        c = {"products": sql_result3[0][0]}, {"products": mongo_result3.get("count")}
        d = {"product_warehouse_stocks": sql_result4[0][0]}, {"product_warehouse_stocks": mongo_result4.get("count")}
        e = {"brands": sql_result5[0][0]}, {"brands": mongo_result5.get("count")}
        f = {"all_categories": sql_result6[0][0]}, {"all_categories": mongo_result6.get("count")}
        return {"MySQL": (a[0], b[0], c[0], d[0], e[0], f[0]), "MongoDB": (a[1], b[1], c[1], d[1], e[1], f[1])}

    return {
        'statusCode': 200,
        'body': {"data": product_log_check()}
    }
