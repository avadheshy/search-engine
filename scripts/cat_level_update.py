from pymongo import MongoClient, UpdateOne

CLIENT = MongoClient("mongodb+srv://sharded-search-service:KC2718oU0Jt9Qt7v@search-service.ynzkd.mongodb.net/test")
DB = CLIENT.product_search

pipeline = [
    {
        '$match': {
            'cat_level': {
                '$exists': False
            }
        }
    }, {
        '$project': {
            'id': 1,
            'category_id': {
                '$toString': '$category_id'
            },
            '_id': 0
        }
    }, {
        '$lookup': {
            'from': 'all_categories',
            'localField': 'category_id',
            'foreignField': 'id',
            'as': 'data'
        }
    }, {
        '$unwind': {
            'path': '$data'
        }
    }, {
        '$project': {
            'id': 1,
            'cat_level': '$data.cat_level'
        }
    }
]

products = list(DB['search_products'].aggregate(pipeline))
payload = []
for product in products:
    payload.append(UpdateOne({'id': product.get('id')}, {'$set': {'cat_level': product.get('cat_level')}}))
# if payload:
#     DB['search_products'].bulk_write(payload)
#