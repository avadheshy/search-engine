import csv
from email.mime import image
import json
import base64
from pymongo import UpdateOne
from app import DB
#
# data_map = {
#     "Uniq Id": "product_id",
#     "Title": "name",
#     "Manufacturer": "manufacturer",
#     "Sku": "sku_code",
# }
#
#

#
#
# def insert_file_data_products(collection_name, file_path):
#
#     payload = []
#     with open(file_path, mode="r") as file:
#         csvFile = csv.DictReader(file)
#         for lines in csvFile:
#             data = {
#                 "product_id": lines.get("Uniq Id"),
#                 "name": lines.get("Title"),
#                 "manufacturer": lines.get("Manufacturer"),
#                 "sku_code": lines.get("Sku"),
#             }
#             payload.append(data)
#     DB[collection_name].insert_many(payload)
#     return True
#
# def test_code():
#
#     for store_id in range(1, 2255):
#         if store_id != 2:
#             PIPE = [
#                 {
#                     '$match': {
#                         'store_id': str(store_id)
#                     }
#                 }, {
#                     '$project': {
#                         '_id': 0,
#                         'product_id': 1,
#                         'store_id': 1
#                     }
#                 }
#             ]
#             data = list(DB['product_store_sharded'].aggregate(PIPE))
#             if data:
#                 payload = []
#                 for i in data:
#                     payload.append(i.get('product_id'))
#                 if payload:
#                     p_data = list(DB['search_products'].find({'id': {'$in': payload}}, {'_id': 0, 'id': 1, 'name': 1, 'status': 1, 'is_mall': 1, 'barcode': 1}))
#                     updated_data_map = {}
#                     for k in p_data:
#                         p_id = k.pop('id')
#                         updated_data_map[p_id] = k
#                     if updated_data_map:
#                         updated_data = []
#                         for j in data:
#                             updated_data.append(UpdateOne({'product_id': j.get('product_id'), 'store_id': j.get('store_id')}, {'$set': updated_data_map[j.get('product_id')]}))
#                         if updated_data:
#                             DB['product_store_sharded'].bulk_write(updated_data)
#                             print('COUNT', store_id)
#
# PIPE = [
#     {
#         '$search': {
#             'compound': {
#                 'should': [
#                     {
#                         'autocomplete': {
#                             'query': 'oil',
#                             'path': 'name'
#                         }
#                     }, {
#                         'autocomplete': {
#                             'query': 'oil',
#                             'path': 'barcode'
#                         }
#                     }
#                 ]
#             }
#         }
#     }, {
#         '$match': {
#             'store_id': '2',
#             'is_mall': '0',
#             'sale_app': '1',
#             'status': "1"
#         }
#     }, {
#         '$sort': {
#             'inv_qty': -1
#         }
#     }, {
#         '$facet': {
#             'total': [
#                 {
#                     '$count': 'count'
#                 }
#             ],
#             'data': [
#                 {
#                     '$skip': 0
#                 }, {
#                     '$limit': 10
#                 }
#             ]
#         }
#     }
# ]
#
# PIPE1 = [
#     {
#         '$search': {
#             'compound': {
#                 'must': [
#                     {
#                         'text': {
#                             'query': '2',
#                             'path': 'store_id'
#                         }
#                     }
#                 ],
#                 'should': [
#                     {
#                         'autocomplete': {
#                             'query': 'oil',
#                             'path': 'name'
#                         }
#                     }, {
#                         'autocomplete': {
#                             'query': 'oil',
#                             'path': 'barcode'
#                         }
#                     }
#                 ],
#                 'minimumShouldMatch': 1
#             }
#         }
#     }, {
#         '$match': {
#             'store_id': '2',
#             'is_mall': '0',
#             'sale_app': '1',
#             'status': '1'
#         }
#     }, {
#         '$project': {
#             'product_id': 1,
#             'inv_qty': 1,
#             '_id': 0
#         }
#     }, {
#         '$sort': {
#             'inv_qty': -1
#         }
#     }, {
#         '$facet': {
#             'total': [
#                 {
#                     '$count': 'count'
#                 }
#             ],
#             'data': [
#                 {
#                     '$skip': 0
#                 }, {
#                     '$limit': 10
#                 }
#             ]
#         }
#     }
# ]


def filter_fun():
    data = []

    with open("/home/dell/Desktop/csv_data/keyword.csv", encoding='unicode_escape') as file:
        csvFile = csv.DictReader(file)
        for lines in csvFile:
            my_dict = {'keyword': lines.get('keyword'), 'name': ''}
            cl1 = lines.get('CL1', '')
            cl2 = lines.get('CL3/4', '')
            if cl1 != '#N/A' and cl1 != 'M&E' and cl1 != '0':
                my_dict['name'] += cl1
            if cl2 != '' and cl2 != '0':
                if my_dict['name'] == '':
                    my_dict['name'] += cl2
                else:
                    my_dict['name'] = my_dict['name']+' '+cl2
            if my_dict['name'] != '':
                data.append(my_dict)
    # toCSV = [{'name': 'bob', 'age': 25, 'weight': 200},
    #          {'name': 'jim', 'age': 31, 'weight': 180}]
    with open('/home/dell/Desktop/csv_data/keyword1.csv', 'w', encoding='utf8', newline='') as output_file:
        fc = csv.DictWriter(output_file,
                            fieldnames=data[0].keys(),

                            )
        fc.writeheader()
        fc.writerows(data)


# filter_fun()


def get_pipeline(
             keyword="", store_id="168", platform="app", order_type="retail", skip=0, limit=10
     ):
    search_terms_len = len(keyword.split(" "))
    SEARCH_PIPE = []

    if search_terms_len == 1:
        SEARCH_PIPE = [
            {
            "$search": {
            "compound": {
            "must": [{"text": {"query": store_id, "path": "store_id"}}],
            "should": [
            {"autocomplete": {"query": keyword, "path": "name"}},
            {"autocomplete": {"query": keyword, "path": "barcode"}},

            ]
            }
            }
            }
             ]
    else:
        keyword = " ".join(
            list(
                filter(
                    lambda x: x not in ["rs", "Rs",
                                        "RS", "rS", "gm", "ml", "kg"],
                    keyword.split(" "),
                )
            )
        )
        SEARCH_PIPE = [
            {
                "$search": {
                    "compound": {
                        "must": [
                            {"text": {"query": store_id, "path": "store_id"}},
                            {"text": {"query": keyword, "path": "name"}},
                        ],

                    }
                }
            }
        ]
    match_filter = {}
    is_mall = "0"
    match_filter["is_mall"] = is_mall

    if platform == "app":
            match_filter["sale_app"] = "1"
    else:
            match_filter["sale_pos"] = "1"

    match_filter["store_id"] = store_id
    match_filter["status"] = "1"
    PIPELINE = SEARCH_PIPE + [
            {"$match": match_filter},
            {"$project": {"id": "$product_id", "inv_qty": 1, "_id": 0, 'name': 1}},
            {"$skip": skip},
            {"$limit": limit}
        ]
    print(PIPELINE)
    return PIPELINE

get_pipeline(keyword='rice')


for i in range(1,170000,10000):
    ids=list(map(str,range(i,i+10000)))

    pipeline=[
    {'$match':{'id':{'$in':ids}}},
    {
        '$lookup': {
            'from': 'product_tag', 
            'localField': 'id', 
            'foreignField': 'product_id', 
            'as': 'data'
        }
    }, {
        '$project': {
            'id': 1, 
            'name': 1, 
            'group_id': 1, 
            'mrp': 1, 
            'price': 1, 
            'barcode': 1, 
            'status': 1, 
            'sale_app': 1, 
            'sale_pos': 1, 
            'is_mall': 1, 
            'updated_at': 1, 
            'brand_id': 1, 
            'category_id': {
                '$toString': '$category_id'
            }, 
            'chain_id': 1, 
            'created_at': 1, 
            'tags': '$data.tag_id'
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
            'name': 1, 
            'group_id': 1, 
            'mrp': 1, 
            'price': 1, 
            'barcode': 1, 
            'status': 1, 
            'sale_app': 1, 
            'sale_pos': 1, 
            'is_mall': 1, 
            'updated_at': 1, 
            'brand_id': 1, 
            'category_id': {
                '$toInt': '$category_id'
            }, 
            'created_at': 1, 
            'tags': 1, 
            'cat_level': '$data.cat_level'
        }
    }
    ]
    payload=list(DB['search_products'].aggregate(pipeline))
    if payload:
        DB['list_products'].insert_many(payload)
    print(i)
    print(len(payload))
    
'response'[
    #count, total number of products
    #rows,
    #currentPage, page (bydefault 1)
    numFound,
    #lastPage , count/pagesize(bydefault 15)
    # productIds,product ids
    # groupIds , groupids
    filters
]

  
    


