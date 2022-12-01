import csv
import re
from pymongo import MongoClient, UpdateOne, UpdateMany


def abc():
    CLIENT = MongoClient(
        "mongodb+srv://searchengine-appuser:qJSjAhUkcAlyuAwy@search-service.ynzkd.mongodb.net/?retryWrites=true&w=majority"
    )
    DB = CLIENT.product_search

    data = []
    with open('/home/dell/Desktop/1k_data/keyword.csv', mode='r') as file:
        csvFile = csv.DictReader(file)
        for lines in csvFile:
            my_dict = {}
            my_dict['keyword'] = lines.get('keyword')
            cl1 = lines.get('CL1').title()
            cl34 = lines.get('CL3/4').title()
            if cl1 not in ['#N/A', 'M&E', '0', '']:
                my_dict['cl1'] = cl1
            if cl34 not in ['', '0', '#N/A']:
                my_dict['cl34'] = lines.get('CL3/4')
            if len(my_dict) > 1:
                data.append(my_dict)
    pipeline = [
        {
            '$project': {
                'id': 1,
                '_id': 0,
                'name': 1,
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
                'id': 1,
                '_id': 0,
                'name': 1,
                'category_id': 1,
                'cat_level': '$data.cat_level',
                'cat_name': '$data.name'
            }
        }
    ]
    products = list(DB['dummy_mall'].aggregate(pipeline))
    payload = []
    for product in products:
        product_id = product.get('id')
        product_name = product.get('name')
        cat_level = product.get('cat_level')
        cat_name = product.get('cat_name')
        new_name = ""
        for dt in data:
            cl1 = dt.get('cl1')
            cl34 = dt.get('cl34')
            keyword = dt.get('keyword')
            if cl1 and '1' == cat_level:
                if cl1 in cat_name:
                    new_name = keyword

            elif cl34 and '3' == cat_level:
                if cl34 in cat_name:
                    new_name = keyword

            elif cl34 and '4' == cat_level:
                if cl34 in cat_name:
                    new_name = keyword
        c_name = product_name + " " + " ".join(list(set(new_name.split(' '))))
        payload.append(UpdateOne({'id': product_id}, {'$set': {'c_name': c_name, 'name': product_name}}))
