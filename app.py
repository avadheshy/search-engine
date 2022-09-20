
from multiprocessing import managers
from multiprocessing.sharedctypes import Value
import os
from io import StringIO
from fastapi import FastAPI, Body, HTTPException, status, Query, File, UploadFile
from fastapi.responses import Response, JSONResponse
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field, EmailStr
from bson import ObjectId
from typing import Optional, List
from pymongo import MongoClient
import time
import csv
from io import BytesIO
import codecs


app = FastAPI()
CLIENT = MongoClient(
    'mongodb+srv://searchengine-appuser:qJSjAhUkcAlyuAwy@search-service.ynzkd.mongodb.net/?retryWrites=true&w=majority')
DB = CLIENT.search_engine
PAGE_SIZE = 20


def get_boosting_stage(keyword, store_id,skip):

    PIPELINE = [
            {
        '$search': {
            'text': {
                'query': keyword, 
                'path': [
                    'name', 'brand.name', 'category.name'
                ]
            }
        }
        },

            {
            '$lookup': {
                'from': 'store',
                'let': {
                    'product_id': '$id'
                },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                        '$and': [
                            {
                            '$eq': [
                            '$product_id', '$$product_id'
                            ]
                            }, {
                             '$eq': [
                                 '$store_id', store_id
                            ]
                                    }
                        ]
                            }
                        }
                    }, 
            {
                '$project': {
                'store_id': 1,
                '_id': 0
                        }
                    }
                ],
                'as': 'store'
            }
        }, {
            '$match': {
                'store.store_id': store_id
            }
        },
         {'$project': {
                '_id': 0,
                }
            },
        {"$skip": skip},
        {'$limit': PAGE_SIZE}
    ]
    return PIPELINE


def store_search_terms(user_id, search_term, search_results):
    DB['hst_search'].insert_one(
        {'user_id': user_id, 'search_term': search_term, 'search_results': search_results})
    return True


def store_autocomplete_results(user_id, search_term, search_results):
    DB['hst_autocomplete'].insert_one(
        {'user_id': user_id, 'search_term': search_term, 'search_results': search_results})
    return True


def get_autocomplete_pipeline(search_term, skip, limit):
    """
    This is autocomplete helper function
    """
    return [
        {
            '$search': {
                'index': 'default',
                'autocomplete': {
                    'path': 'name',
                    'query': search_term
                }
            }
        },
        {
            '$skip': skip
        },
        {
            '$limit': limit
        },
        {
            '$project': {
                '_id': 0,
                'name': 1,
                'product_id': 1
            }
        }
    ]


# @app.post('/boost')
# def add_booster(attribute_booster: dict):
#     DB['product_booster'].insert_one(attribute_booster)
#     return True


@app.get("/search")
def product_search(store_id: str,page: str,keyword:Optional[str] = None):
    """
    Product Search API, This will help to discover the relevant products
    """

    start_time = time.time()
    user_id = 1
    skip = (int(page) - 1) * PAGE_SIZE
    pipe_line = get_boosting_stage(keyword,store_id,skip)
    
    ans=list(DB["search_products"].aggregate(pipe_line))
    end_time=time.time()
    print(len(ans))
    print((end_time-start_time)*1000)
    return ans


@ app.get("/autocomplete")
def search_autocomplete(search_term: str, page: str):
    """
    This API helps to auto complete the searched term
    """
    user_id=1
    skip=(int(page) - 1) * PAGE_SIZE
    pipeline=get_autocomplete_pipeline(search_term, skip, PAGE_SIZE)
    products=list(DB["search_products"].aggregate(pipeline))
    # store_autocomplete_results(user_id, search_term, products)
    return products


@ app.get("/filter_category")
def filter_product(filters_for:str,page:str,filters_for_id:str,sort_by:Optional[str]=None,categories: list = Query(None),brandIds: list = Query(None)):
    # import ipdb; ipdb.set_trace()
    filter_query={}
    if filters_for=='brand':
        filter_query['brand.id']= filters_for_id
    elif filters_for=='cl1':
        filter_query['$and']=[{'category_level.cat_level':'1'},{'category_level.cl1_id':filters_for_id}]
    elif filters_for=='cl2':
        filter_query['$and']=[{'category_level.cat_level':'2'},{'category_level.cl2_id':filters_for_id}]
    elif filters_for=='cl3':
        filter_query['$and']=[{'category_level.cat_level':'3'},{'category_level.cl3_id':filters_for_id}]
    elif filters_for=='cl4':
        filter_query['$and']=[{'category_level.cat_level':'4'},{'category_level.cl4_id':filters_for_id}]
    aggregation_pipeline = [

        {'$match': filter_query},
        {"$project": {"_id": 0}},
        
    ]
    # aggregation_pipeline.append({"$sort": 'created_at'})
    sort_query={}
    category_ids=[]
    brand_ids=[]
    if sort_by:
        if sort_by=='min_price':
            sort_query['price']=1
        elif sort_by=='max_price':
            sort_query['price']=-1
        aggregation_pipeline.append({'$sort':sort_query})
    if categories:
        for category in categories:
            category_ids.append(category)
        aggregation_pipeline.append({'$match':{'category.id':{'$in':category_ids}}})
    if brandIds:
        for brandId in brandIds:
            brand_ids.append(brandId)
        aggregation_pipeline.append({'$match':{'brand.id':{'$in':brand_ids}}})



    print(aggregation_pipeline)    
    aggregation_pipeline.append({'$limit': PAGE_SIZE})
    result=list(DB["my_data"].aggregate(aggregation_pipeline))
    return result




