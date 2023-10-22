import mysql.connector
from collections import  defaultdict
from pymongo import MongoClient, UpdateOne

CLIENT = MongoClient("mongodb+srv://sharded-search-service:KC2718oU0Jt9Qt7v@search-service.ynzkd.mongodb.net/test")
DB = CLIENT.product_search
connection = mysql.connector.connect(user='nagendra.kumar', password='EB91c7lNtPRdG5uD', host='127.0.0.1',
                                     database='pos', port='3310')
cur1 = connection.cursor()
Query1 = "SELECT product_id,tag_id FROM pos.product_tag where deleted_at IS NULL;"
cur1.execute(Query1)
result = cur1.fetchall()
pid_map = defaultdict(list)
for i in result:
    pid_map[str(i[0])].append(str(i[1]))

payload = []

for key, value in pid_map.items():
    payload.append(UpdateOne({'id': key}, {'$set': {'tag_ids': value}}))
# DB['search_products'].bulk_write(payload)
