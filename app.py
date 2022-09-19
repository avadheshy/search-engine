from calendar import day_abbr
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


def get_boosting_stage(keyword, store_id,skip,brand_id,category_id,group_id):
    filtrer_query={}
    if brand_id is not None:
        filtrer_query['brand.id']=brand_id
    if category_id is not None:
        filtrer_query['category.id']=category_id
    if group_id is not None:
        filtrer_query['group_id']=group_id
    booster={'Patanjali':5}
    arr=[]
    for key,value in booster.items():
        arr.append({'text': {
                            'query': key,
                            'path': 'brand.name',
                            'score': {'boost': {'value': value}}
                       }})

    PIPELINE = [
           {"$search": {
                        'compound': {
                        'must': [
                            {'text': {
                            'query': keyword,
                            'path': ['name', 'brand.name', 'category.name']
                                    }
                            }
                        ],
                        'should':arr,
                        "minimumShouldMatch": 0,
                        }

                    }},

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
                    }, {
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
        {'$match':filtrer_query},
         {
                        '$project': {
                            '_id': 0,
                        }},
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
def product_search(store_id: str, keyword: str, page: str,brand_id:Optional[str]=None,category_id:Optional[str]=None,group_id:Optional[str]=None):
    """
    Product Search API, This will help to discover the relevant products
    """

    start_time = time.time()
    user_id = 1
    skip = (int(page) - 1) * PAGE_SIZE
    pipe_line = get_boosting_stage(keyword,store_id,skip,brand_id,category_id,group_id)
    
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
def filter_product(category_name: Optional[str]=None, brand_name: Optional[str]=None,):
    filter_query={}
    if category_name:
        filter_query['category.name']=category_name
    if brand_name:
        filter_query['brand.name']=brand_name
    print(filter_query)
    result=list(DB["search_products"].aggregate([

        {'$match': filter_query},
        {"$project": {"_id": 0}},
        {'$limit': PAGE_SIZE}
    ]))
    return result




