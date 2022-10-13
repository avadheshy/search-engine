from pymongo import MongoClient,UpdateOne

CLIENT = MongoClient(
    "mongodb+srv://sharded-search-service:KC2718oU0Jt9Qt7v@search-service.ynzkd.mongodb.net/test")
DB = CLIENT.product_search


def get_boosting_products(product_name="", product_ids=[], category_ids=[], brand_ids=[], group_ids=[]):
    product_id = []
    filter_query = []
    match_query = {}
    if product_name:
        match_query= {
            'name': {'$regex': product_name, '$options': 'i'}
            }

    if product_ids:
        filter_query.append({'id': {'$in': product_ids}})
    if category_ids:
        filter_query.append({'category_id': {'$in': category_ids}})
    if brand_ids:
        filter_query.append({'brand_id': {'$in': brand_ids}})
    if group_ids:
        filter_query.append({'group_id': {'$in': group_ids}})
    products = list(DB['products'].aggregate([
        {'$match':match_query},
        {'$match':{'$or':filter_query}},
        {'$project':{'_id':0,'id':1}}
    ]))
    payload=[]
    for product in products:
        payload.append(UpdateOne({'id':product.get('id')},{'$set':{'Winter_sell':'1'}}))
    if payload:
        DB['products'].bulk_write(payload)
        
    
