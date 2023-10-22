import mysql.connector
from pymongo import MongoClient, UpdateOne,UpdateMany

CLIENT = MongoClient("mongodb+srv://sharded-search-service:KC2718oU0Jt9Qt7v@search-service.ynzkd.mongodb.net/test")
DB = CLIENT.product_search
connection = mysql.connector.connect(user='nagendra.kumar', password='EB91c7lNtPRdG5uD', host='127.0.0.1',
                                     database='pos', port='3310')
cur1 = connection.cursor()
Query1 = "SELECT product_id,count(id) as score FROM pos.order_items where status='fulfilled' group by product_id"
cur1.execute(Query1)
result = cur1.fetchall()
payload = []

for r in result:
    payload.append(UpdateOne({'id': str(r[0])}, {'$set': {'ps': int(r[1])}}))
# DB['search_products'].bulk_write(payload)