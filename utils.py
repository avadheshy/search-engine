import csv
from email.mime import image
import json
import base64
from pymongo import MongoClient, UpdateOne

data_map = {
    "Uniq Id": "product_id",
    "Title": "name",
    "Manufacturer": "manufacturer",
    "Sku": "sku_code",
}


CLIENT = MongoClient(
    "mongodb+srv://searchengine-appuser:qJSjAhUkcAlyuAwy@search-service.ynzkd.mongodb.net/?retryWrites=true&w=majority"
)
DB = CLIENT.search_engine


def insert_file_data_products(collection_name, file_path):

    payload = []
    with open(file_path, mode="r") as file:
        csvFile = csv.DictReader(file)
        for lines in csvFile:
            data = {
                "product_id": lines.get("Uniq Id"),
                "name": lines.get("Title"),
                "manufacturer": lines.get("Manufacturer"),
                "sku_code": lines.get("Sku"),
            }
            payload.append(data)
    DB[collection_name].insert_many(payload)
    return True

def lambda_handler1(event, context):
    data = event['records']
    product_payloads = []
    if data and data.get('product-0'):
        for product in data.get('product-0'):
            value = product.get('value')
            decoded_string = base64.b64decode(value)
            product_payload = json.loads(decoded_string)
            product_payloads.append(UpdateOne({"product_id": product_payload.get("product_id")}, {"$set": product_payload}, upsert=True))
    DB['kafka_products'].bulk_write(product_payloads)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def lambda_handler2(event, context):
    data = event['records']
    product_payloads = []
    if data and data.get('price-0'):
        for product in data.get('price-0'):
            value = product.get('value')
            decoded_string = base64.b64decode(value)
            product_payload = json.loads(decoded_string)
            product_payloads.append(UpdateOne({"product_id": product_payload.get("product_id"),"store_id": product_payload.get("store_id")}, {"$set": product_payload}, upsert=True))
    DB['kafka_product_store'].bulk_write(product_payloads)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    
def lambda_handler3(event, context):
    data = event['records']
    product_payloads = []
    if data and data.get('inventory-0'):
        for product in data.get('inventory-0'):
            value = product.get('value')
            decoded_string = base64.b64decode(value)
            product_payload = json.loads(decoded_string)
            product_payloads.append(UpdateOne({"product_id": product_payload.get("product_id"),"store_id": product_payload.get("store_id")}, {"$set": product_payload}, upsert=True))
    DB['kafka_product_store'].bulk_write(product_payloads)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    
    
def lambda_handler4(event, context):
    data = event['records']
    product_payloads = []
    if data and data.get('mall_inventory-0'):
        for product in data.get('mall_inventory-0'):
            value = product.get('value')
            decoded_string = base64.b64decode(value)
            product_payload = json.loads(decoded_string)
            product_payloads.append(UpdateOne({"product_id": product_payload.get("product_id"),"warehouse_id": product_payload.get("warehouse_id")}, {"$set": product_payload}, upsert=True))
    DB['kafka_warehouse_stocks'].bulk_write(product_payloads)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    
def fun():
    data=list(DB['groups'].aggregate([
    {
        '$lookup': {
            'from': 'products', 
            'localField': 'id', 
            'foreignField': 'group_id', 
            'as': 'product_data'
        }
    }
    ])) 
    payload=[]
    for i in data:
        search_name=list(i.get('name',"").split(''))
        for j in range(len(i['product_data'])):
            name=i['product_data'][j]['name'].split()
            for k in name:
                if k not in search_name:
                    search_name.append(k)
        search_name=' '.join(search_name)
        payload.append({
            'id':i['id'],
            'name':i.get('name',""),
            'search_name':search_name,
            'variants':i['product_data']
            
        })
        
 
