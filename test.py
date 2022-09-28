from mysql import connector
import mysql

from datetime import datetime, timedelta
from pymongo import MongoClient
CLIENT = MongoClient(
    "mongodb+srv://searchengine-appuser:qJSjAhUkcAlyuAwy@search-service.ynzkd.mongodb.net/?retryWrites=true&w=majority"
)
DB=CLIENT.search





current_time = datetime.now()
prev_time = current_time - timedelta(hours=180)
# import ipdb; ipdb.set_trace()
connection = mysql.connector.connect(host='127.0.0.1',
                                         database='pos',
                                         port=3310,
                                         user='nagendra.kumar',
                                         password='hwkUBlUpdM0G7ORQ')
cur=connection.cursor()
Query = "SELECT * FROM  pos.inventories WHERE inventories.updated_at > %s"

cur.execute(Query,(prev_time,))
result=cur.fetchall()
    
# keys=["id","product_id","store_id",'quantity','batch_number','unit_cost_price',"expiry_date","status","shipment_id","user_id","created_at","updated_at"]
data=[]
f = '%Y-%m-%d %H:%M:%S'

# for res in result:
#     payload=dict(zip(keys,res))
#     data.append(payload)
keys=["id","product_id","store_id",'quantity','batch_number','unit_cost_price',"expiry_date","status","shipment_id","user_id","created_at","updated_at"]
data=[]
for res in result:
    d={}
    for i in range(len(keys)):
        if keys[i]=='quantity':
            d[keys[i]]=float(res[i])
        elif keys[i] == "unit_cost_price":
                d[keys[i]] = float(res[i]) if res[i] else None
        elif keys[i]=='created_at':
            d[keys[i]]= res[i].strftime(f)
        elif keys[i]=='updated_at':
            d[keys[i]]=res[i].strftime(f)
        else:
            d[keys[i]]=res[i]
    
    data.append(d)
for res in data:
    query={}
    query['product_id']=str(res['product_id'])
    query['store_id']=str(res['store_id'])
    print(query)
    DB['inventories'].update_many(query,{'$set':res},**{'upsert':True})
    

#product store
Query1 = "SELECT * FROM  pos.product_store WHERE product_store.updated_at > %s"
cur.execute(Query1,(prev_time,))
result=cur.fetchall()
keys1=["id","store_id","product_id",'price','wholesale_price','wholesale_moq','old_price',"fast_sale","status","sale_app","sale_pos",'auto_pricing_at',"created_at","updated_at"]
data=[]
for res in result:
    d={}
    for i in range(len(keys1)):
        if keys1[i]=='price':
            d[keys1[i]]=float(res[i])
        elif keys1[i]=='wholesale_price':
            d[keys1[i]]=float(res[i]) if res[i] else None
        elif keys1[i]=='old_price':
            d[keys1[i]]=float(res[i]) if res[i] else None
        elif keys1[i]=='created_at':
            d[keys1[i]]= res[i].strftime(f)
        elif keys1[i]=='updated_at':
            d[keys1[i]]=res[i].strftime(f)
        elif keys1[i]=='auto_pricing_at':
            d[keys1[i]]=res[i].strftime(f)    
        else:
            d[keys1[i]]=res[i]
    
    data.append(d)
print(data)
for res in data:
    query={}
    query['product_id']=str(res['product_id'])
    query['store_id']=str(res['store_id'])
    print(query)
    DB['product_store'].update_many(query,{'$set':res},**{'upsert':True})




    
