import csv
from email.mime import image
import json
import base64
from pymongo import UpdateOne,UpdateMany
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


data_map = {
    "Uniq Id": "product_id",
    "Title": "name",
    "Manufacturer": "manufacturer",
    "Sku": "sku_code",
}


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
        '$project': {
            'id': 1, 
            'category_id': {
                '$toString': '$category_id'
            }
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
            'cat_level': '$data.cat_level', 
            'id': 1
        }
    }, {
        '$lookup': {
            'from': 'product_tag', 
            'localField': 'id', 
            'foreignField': 'product_id', 
            'as': 'data'
        }
    }, {
        '$project': {
            'id': 1, 
            'cat_level': 1, 
            'tag_ids': '$data.tag_id'
        }
    }
    ]
    data=list(DB['products'].aggregate(pipeline))
    payload=[]
    for i in data:
        dict_data={}
        dict_data['cat_level']=i.get('cat_level') or None
        dict_data['tag_ids']=i.get('tag_ids')  or []
        payload.append(UpdateOne({'id':i.get('id')},{'$set':dict_data}))
    if payload:
        DB['search_products'].bulk_write(payload)
        payload = []
    print(i)
    print(len(payload))
    
# 'response'[
#     #count, total number of products
#     #rows,
#     #currentPage, page (bydefault 1)
#     numFound,
#     #lastPage , count/pagesize(bydefault 15)
#     # productIds,product ids
#     # groupIds , groupids
#     filters
# ]
data=list(DB['products'].aggregate([
    {
        '$match': {}
    }, {
        '$project': {
            '_id': 0, 
            'id': 1, 
            'brand_id': 1, 
            'category_id': 1
        }
    }
]))
payload=[]
for i in data:
    dict_data={}
    dict_data['brand_id']=int(i.get('brand_id'))
    dict_data['category_id']=int(i.get('category_id'))
    payload.append(UpdateOne({'id':i.get(id)},{'$set':dict_data}))
DB['search_products'].bulk_write(payload)
  
    


