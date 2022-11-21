from pymongo import MongoClient
import csv
from email.mime import image

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

def test_code():
    
    for store_id in range(1, 2255):
        if store_id != 2:
            PIPE = [
                {
                    '$match': {
                        'store_id': str(store_id)
                    }
                }, {
                    '$project': {
                        '_id': 0, 
                        'product_id': 1, 
                        'store_id': 1
                    }
                }
            ]
            data = list(DB['product_store_sharded'].aggregate(PIPE))
            if data:
                payload = []
                for i in data:
                    payload.append(i.get('product_id'))
                if payload:
                    p_data = list(DB['search_products'].find({'id': {'$in': payload}}, {'_id': 0, 'id': 1, 'name': 1, 'status': 1, 'is_mall': 1, 'barcode': 1}))
                    updated_data_map = {}
                    for k in p_data:
                        p_id = k.pop('id')
                        updated_data_map[p_id] = k
                    if updated_data_map:
                        updated_data = []
                        for j in data:
                            updated_data.append(UpdateOne({'product_id': j.get('product_id'), 'store_id': j.get('store_id')}, {'$set': updated_data_map[j.get('product_id')]}))
                        if updated_data:
                            DB['product_store_sharded'].bulk_write(updated_data)
                            print('COUNT', store_id)

PIPE = [
    {
        '$search': {
            'compound': {
                'should': [
                    {
                        'autocomplete': {
                            'query': 'oil', 
                            'path': 'name'
                        }
                    }, {
                        'autocomplete': {
                            'query': 'oil', 
                            'path': 'barcode'
                        }
                    }
                ]
            }
        }
    }, {
        '$match': {
            'store_id': '2', 
            'is_mall': '0', 
            'sale_app': '1', 
            'status': "1"
        }
    }, {
        '$sort': {
            'inv_qty': -1
        }
    }, {
        '$facet': {
            'total': [
                {
                    '$count': 'count'
                }
            ], 
            'data': [
                {
                    '$skip': 0
                }, {
                    '$limit': 10
                }
            ]
        }
    }
]

PIPE1 = [
    {
        '$search': {
            'compound': {
                'must': [
                    {
                        'text': {
                            'query': '2', 
                            'path': 'store_id'
                        }
                    }
                ], 
                'should': [
                    {
                        'autocomplete': {
                            'query': 'oil', 
                            'path': 'name'
                        }
                    }, {
                        'autocomplete': {
                            'query': 'oil', 
                            'path': 'barcode'
                        }
                    }
                ], 
                'minimumShouldMatch': 1
            }
        }
    }, {
        '$match': {
            'store_id': '2', 
            'is_mall': '0', 
            'sale_app': '1', 
            'status': '1'
        }
    }, {
        '$project': {
            'product_id': 1, 
            'inv_qty': 1, 
            '_id': 0
        }
    }, {
        '$sort': {
            'inv_qty': -1
        }
    }, {
        '$facet': {
            'total': [
                {
                    '$count': 'count'
                }
            ], 
            'data': [
                {
                    '$skip': 0
                }, {
                    '$limit': 10
                }
            ]
        }
    }
]

# connection = mysql.connector.connect(user='nagendra.kumar', password='EB91c7lNtPRdG5uD', host='127.0.0.1', database='pos',port='3310')