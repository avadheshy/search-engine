# import json
# import base64
import json
import datetime
from mysql import connector
from pymongo import MongoClient, UpdateOne
from app import DB



# def lambda_handler1(event, context):
#     data = event['records']
#     product_payloads = []
#     if data and data.get('product-0'):
#         for product in data.get('product-0'):
#             value = product.get('value')
#             decoded_string = base64.b64decode(value)
#             product_payload = json.loads(decoded_string)
#             product_payloads.append(UpdateOne({"product_id": product_payload.get("product_id")}, {"$set": product_payload}, upsert=True))
#     DB['kafka_products'].bulk_write(product_payloads)
#     return {
#         'statusCode': 200,
#         'body': json.dumps('Hello from Lambda!')
#     }

# def lambda_handler2(event, context):
#     data = event['records']
#     product_payloads = []
#     if data and data.get('price-0'):
#         for product in data.get('price-0'):
#             value = product.get('value')
#             decoded_string = base64.b64decode(value)
#             product_payload = json.loads(decoded_string)
#             product_payloads.append(UpdateOne({"product_id": product_payload.get("product_id"),"store_id": product_payload.get("store_id")}, {"$set": product_payload}, upsert=True))
#     DB['kafka_product_store'].bulk_write(product_payloads)
#     return {
#         'statusCode': 200,
#         'body': json.dumps('Hello from Lambda!')
#     }
    
# def lambda_handler3(event, context):
#     data = event['records']
#     product_payloads = []
#     if data and data.get('inventory-0'):
#         for product in data.get('inventory-0'):
#             value = product.get('value')
#             decoded_string = base64.b64decode(value)
#             product_payload = json.loads(decoded_string)
#             product_payloads.append(UpdateOne({"product_id": product_payload.get("product_id"),"store_id": product_payload.get("store_id")}, {"$set": product_payload}, upsert=True))
#     DB['kafka_product_store'].bulk_write(product_payloads)
#     return {
#         'statusCode': 200,
#         'body': json.dumps('Hello from Lambda!')
#     }
    
    
# def lambda_handler4(event, context):
#     data = event['records']
#     product_payloads = []
#     if data and data.get('mall_inventory-0'):
#         for product in data.get('mall_inventory-0'):
#             value = product.get('value')
#             decoded_string = base64.b64decode(value)
#             product_payload = json.loads(decoded_string)
#             product_payloads.append(UpdateOne({"product_id": product_payload.get("product_id"),"warehouse_id": product_payload.get("warehouse_id")}, {"$set": product_payload}, upsert=True))
#     DB['kafka_warehouse_stocks'].bulk_write(product_payloads)
#     return {
#         'statusCode': 200,
#         'body': json.dumps('Hello from Lambda!')
#     }
    
# def fun():
#     data=list(DB['groups'].aggregate([
#     {
#         '$lookup': {
#             'from': 'products', 
#             'localField': 'id', 
#             'foreignField': 'group_id', 
#             'as': 'product_data'
#         }
#     }
#     ])) 
#     payload=[]
#     for i in data:
#         search_name=list(i.get('name',"").split(''))
#         for j in range(len(i['product_data'])):
#             name=i['product_data'][j]['name'].split()
#             for k in name:
#                 if k not in search_name:
#                     search_name.append(k)
#         search_name=' '.join(search_name)
#         payload.append({
#             'id':i['id'],
#             'name':i.get('name',""),
#             'search_name':search_name,
#             'variants':i['product_data']
            
#         })

     
def fun():
    ans = list(DB['product_store_sharded'].find({'barcode': None}, {'_id':0,'store_id':1,'product_id': 1}))
    product_ids=[]
    store_ids=[]
    for i in ans:
        product_ids.append(i.get('product_id'))
        store_ids.append(i.get('store_id'))
    abc=list(DB['products'].aggregate([{'$match':{'id':{'$in':product_ids}}},{'$project':{'_id':0,'id':1,'barcode':1, 'name': 1}}]))
    abc_map = {}
    for i in abc:
        abc_map[i.get('id')] = {'name':i.get('name'),'barcode':i.get('barcode')}
    
    payload=[]
    for i in range(len(product_ids)):
        payload.append(UpdateOne({'store_id':store_ids[i],'product_id':product_ids[i]},{'$set':abc_map.get(product_ids[i])}))
    DB['product_store_sharded'].bulk_write(payload) 

# p#id
# store_id
# product_id
# price
# wholesale_moq
# old_price
# fast_sale
# p#status
# p#sale_app
# p#sale_pos
# p#auto_pricing_at
# p#created_at
# p#updated_at
# #inv_qty
# p#barcode
# p#is_mall
# p#name

# def sync():
#     current_time = datetime.now()
#     prev_time = current_time - datetime.timedelta(hours=2)
#     connection = connector.connect(
#         host="pos-prod-aurora.cluster-ro-crvi1ow7nyif.ap-south-1.rds.amazonaws.com",
#         user="nagendra.kumar",
#         password="EB91c7lNtPRdG5uD"
#         )
#     cur2 = connection.cursor()
#     Query = '''SELECT * FROM  pos.product_store as ps
#     INNER JOIN pos.products  as p
#     ON p.id=ps.product_id
#     WHERE ps.updated_at > %s OR ps.created_at > %s'''
#     cur2.execute(Query, (prev_time, prev_time,))
#     result2 = cur2.fetchall()
#     connection.close()


    
        
        
        

            