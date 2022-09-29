from mysql import connector
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne

CLIENT = MongoClient(
    "mongodb+srv://searchengine-appuser:qJSjAhUkcAlyuAwy@search-service.ynzkd.mongodb.net/?retryWrites=true&w=majority"
)
DB = CLIENT.product_search

current_time = datetime.now()
prev_time = current_time - timedelta(hours=24)

f = "%Y-%m-%d %H:%M:%S"
def sync_product_data(prev_time):
    connection = connector.connect(
        host="pos-prod-aurora.cluster-ro-crvi1ow7nyif.ap-south-1.rds.amazonaws.com",
        user="nagendra.kumar",
        password="EB91c7lNtPRdG5uD"
    )
    cur = connection.cursor()
    Query1 = "SELECT * FROM  pos.products WHERE products.id = 136055"
    cur.execute(Query1, (prev_time,prev_time,))
    result = cur.fetchall()
    ans=result[0]
    
    keys1 = [
        "id",
        "chain_id",
        "name",
        "image_url",
        "group_id",
        "brand_id",
        "marketer_id",
        "category_id",
        "tax_group_id",
        "description",
        "uom",
        "mrp",
        "old_mrp",
        "price",
        "wholesale_price",
        "wholesale_moq",
        "old_price",
        "hsn_sac_code",
        "sku",
        "barcode",
        "color",
        'localization',
        'org_name',
        "status",
        "sale_app",
        "sale_pos",
        "is_mall",
        "is_returnable",
        "primary_return_window",
        "is_secondary_returnable",
        "secondary_return_window",
        "meta_description",
        "created_at",
        "updated_at",
        "last_price_change"
    ]
        
        
    data = []
    for res in result:
        d = {}
        for i in range(len(keys1)):
            if (keys1[i] == "mrp" or keys1[i] == "old_mrp" or keys1[i] == "price" or keys1[i] == "wholesale_price" or keys1[i] == "old_price"):
                d[keys1[i]] = float(res[i]) if res[i] else 0
            elif (keys1[i] == "created_at" or keys1[i] == "updated_at" or keys1[i] == "last_price_change"):
                d[keys1[i]] = res[i].strftime(f) if res[i] else None
            elif(keys1[i] == "id" or keys1[i] == "chain_id" or keys1[i] == "group_id" or keys1[i] == "brand_id" or keys1[i] == "marketer_id" or keys1[i] == "category_id" or keys1[i] == "tax_group_id"): 
                d[keys1[i]]= str(res[i]) if res[i] else "0" 
            elif keys1[i] == "sale_app":
                d[keys1[i]]= str(res[i]) if res[i] else "0"
            elif keys1[i] == "sale_pos":
                d[keys1[i]]= str(res[i]) if res[i] else "0"
            elif keys1[i] == "is_mall":
                d[keys1[i]]= str(res[i]) if res[i] else "0"
            else:
                d[keys1[i]] = res[i] if res[i] else None
        data.append(d)
    payload = []
    for res in data:
        query = {}
        query["id"] = str(res["id"])
        res["id"]=str(res["id"])
        payload.append(UpdateOne(query, {"$set": res}, upsert=True))
    if payload:
        DB['search_products'].bulk_write(payload)
        DB["products"].bulk_write(payload)
    connection.close()
    return True, "Syncing was successfull."

def lambda_handler(event, context):

    print("function started!")
    result, message = sync_product_data(prev_time)
    print("function ends!")
    return {"status": True}