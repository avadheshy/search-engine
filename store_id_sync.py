from unicodedata import lookup
from mysql import connector
from pymongo import MongoClient,UpdateMany,UpdateOne
CLIENT = MongoClient("mongodb+srv://sharded-search-service:KC2718oU0Jt9Qt7v@search-service.ynzkd.mongodb.net/test")
DB = CLIENT.product_search

def sync_all_product():
    for i in range(1,170000,10000):
        ids=list(map(str,range(i,i+10000)))
        products_list=DB['products'].aggregate([
            {'$match':{"id":{'$in':ids}}},
            {'$project':{'_id':0,'id':1,'group_id':1,'brand_id':1,'category_id':1}}
        ])
        product_map={}
        product_ids=[]
        for product in products_list:
            if product.get('group_id'):
                product_map[product.get('id')]={'group_id':int(product.get('group_id')),'brand_id':int(product.get('brand_id')),'category_id':int(product.get('category_id'))}
                product_ids.append(product.get('id'))
        payload=[]
        for pid in product_ids:
            payload.append(UpdateMany({'product_id':pid}, {"$set": product_map.get(pid)}))
        DB['product_store_sharded'].bulk_write(payload)
        print(i)
# def store_id_sync(store_id):
#     product_list=list(DB['inventories'].aggregate([
#         {'$match':{'store_id':store_id}},
#         {'$project': {
#             '_id': 0, 
#             'product_id': 1, 
#             'quantity': {
#                 '$toDouble': '$quantity'
#             }
#         }
#         },
#         {'$group': {
#             '_id': { 
#                 'product_id': '$product_id'
#             }, 
#             'data': {
#                 '$push': '$$ROOT'
#             }
#         }},
#         {'$lookup':{
#             'from':'products',
#             'localFields':'product_d',
#             'foreignField':'id',
#             'as':'data'
#         }},{ '$project':
#             {
#             '$product_id': 1, 
#             'store_id':store_id, 
#             'price': '$data.price', 
#             'wholesale_moq': '$data.wholesale_moq', 
#             'old_price': '$data.old_price', 
#             'fast_sale': '$data.fast_sale', 
#             'status': '$data.status', 
#             'sale_app':'$data.sale_app', 
#             'sale_pos': '$data.sale_pos', 
#             'auto_pricing_at': '$data.auto_pricing_at', 
#             'created_at': '$data.created_at', 
#             'updated_at': '$data.updated_at', 
#             'inv_qty': '$data.price', 
#             'barcode': '$data.barcode', 
#             'is_mall': '$data.is_mall', 
#             'name': '$data.name', 
#             'brand_id': '$data.brand_id', 
#             'category_id': '$data.category_id', 
#             'group_id': '$data.group_id',
#             'inv_qty': {
#                 '$sum': '$quantity'
#             }, 
#             '_id': 0
#             }
#         }
        
#     ]))

#     # product_list=list(DB['products'].aggregate(
#     # [ 
#     # {'$lookup':{
#     #     'from': 'inventories',
#     #     'let': { 'p_id': "$product_id"},
#     #     'pipeline': [
#     #           { '$match':
#     #              { '$expr':
#     #                 { '$and':
#     #                    [
#     #                      { '$eq': [ "$store_id",  store_id ] },
#     #                      { '$eq': [ "$product_id", "$$p_id" ] }
#     #                    ]
#     #                 }
#     #              }
#     #           },
#     #           { '$project': {'_id':0,'quantity': {
#     #             '$toDouble': '$quantity' }}}
#     #        ],
#     #     'as':'inventory_data'
#     #     }
#     # },
#     {'$project':{
#             'product_id': 1, 
#             'store_id':store_id, 
#             'price': 1, 
#             'wholesale_moq': 1, 
#             'old_price': 1, 
#             'fast_sale': 1, 
#             'status': 1, 
#             'sale_app': 1, 
#             'sale_pos': 1, 
#             'auto_pricing_at': 1, 
#             'created_at': 1, 
#             'updated_at': 1, 
#             'inv_qty': 1, 
#             'barcode': 1, 
#             'is_mall': 1, 
#             'name': 1, 
#             'brand_id': 1, 
#             'category_id': 1, 
#             'group_id': 1,
#             'inv_qty': {
#                 '$sum': '$inventory_data.quantity'
#             }, 
#             '_id': 0
#             }}
    
#     #         ]))
#     payload=[]
#     for product in product_list:
#         payload.append(UpdateOne({'store_id':product.get('store_id'),'product_id':product.get('product_id')},{'$set':product},upsert=True))
#     DB['product_store_sharded'].bulk_write(payload)

def sync_products(id:str,product_name:str,store_name:str):
    store_id=""
    product_list=""
    store_data=list(DB['stores'].aggregate([{'$match':{'branding':id}}]))
    if store_data[0].get('name')==store_name:
        store_id=store_data[0].get('id')
    if store_id=="":
        store_data=list(DB['stores'].aggregate([{'$match':{'id':id}}]))
        if store_data[0].get('name')==store_name:
            store_id=store_data[0].get('id')
    if store_id!="":
        product_list=list(DB['products'].aggregate([
            {'$match':{'name':{'$regex' : product_name, '$options' : 'i'}}},
            {'$lookup':{
                'from': 'inventories',
                'let': { 'p_id': "$product_id"},
                'pipeline': [
                    { '$match':
                        { '$expr':
                            { '$and':
                                [
                                    { '$eq': [ "$store_id",  store_id ] },
                                    { '$eq': [ "$product_id", "$$p_id" ] }
                                ]
                             }
                        }
                    },
              { '$project': {'_id':0,'quantity': {
                '$toDouble': '$quantity' }}}
           ],
        'as':'inventory_data'
        }
        },
        {'$project':{
            'product_id': '$id', 
            'store_id':store_id, 
            'price': 1, 
            'wholesale_moq': 1, 
            'old_price': 1, 
            'fast_sale': 1, 
            'status': 1, 
            'sale_app': 1, 
            'sale_pos': 1, 
            'auto_pricing_at': 1, 
            'created_at': 1, 
            'updated_at': 1, 
            'inv_qty': 1, 
            'barcode': 1, 
            'is_mall': 1, 
            'name': 1, 
            'brand_id': 1, 
            'category_id': 1, 
            'group_id': 1,
            'inv_qty': {
                '$sum': '$inventory_data.quantity'
            }, 
            '_id': 0
            }}
            ]))
    payload=[]
    for i in product_list:
        payload.append(UpdateOne({'product_id':i.get('product_id'),'store_id':i.get('store_id')},{'$set':i},upsert=True))
    # if payload:
    #     DB['product_store_sharded'].bulk_write(payload)
            
        
        
        