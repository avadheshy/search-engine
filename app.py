
from multiprocessing import managers
from multiprocessing.sharedctypes import Value
import os
from io import StringIO
from fastapi import FastAPI, Body, HTTPException, status, Query, File, UploadFile, Header, Request
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
from typing import Union




app = FastAPI()
CLIENT = MongoClient(
    'mongodb+srv://searchengine-appuser:qJSjAhUkcAlyuAwy@search-service.ynzkd.mongodb.net/?retryWrites=true&w=majority')
DB = CLIENT.search_engine
PAGE_SIZE = 10


def get_boosting_stage(order_type, keyword, store_id, platform):
    
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
        {"$match": {"is_mall": is_mall}},

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
                'id': 1
                }
            }
    ]
    print(PIPELINE)
    return PIPELINE


def store_search_terms(user_id, search_term, search_results):
    DB['hst_search'].insert_one(
        {'user_id': user_id, 'search_term': search_term, 'search_results': search_results})
    return True


def store_autocomplete_results(user_id, search_term, search_results):
    DB['hst_autocomplete'].insert_one(
        {'user_id': user_id, 'search_term': search_term, 'search_results': search_results})
    return True


def get_autocomplete_pipeline(search_term, skip):
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


@app.post("/v1/search")
async def product_search(request: Request):
    """
    Product Search API, This will help to discover the relevant products
    """
    # headers = request.headers
    # store_id = headers.get('storeid')

    ans=list(DB["search_products"].aggregate(pipe_line))
    pipe_line.pop()
    pipe_line.pop()
    pipe_line.append({"$count": "count"})
    data_count = list(DB["search_products"].aggregate(pipe_line))
    print('NAGA COUNT', data_count)
    total_count=data_count[0].get("count")
    total_pages=((total_count+PAGE_SIZE)//PAGE_SIZE)
    result={
    'data':{
        'header_data':[],
        'products':ans,
    },
    'links':{
        'first':path + '?page=1',
        'last': path + '?page='+str(total_pages),
        'prev':None if int(page)-1==0 else path + '?page='+str(int(page)-1),
        'next':path + '?page='+str(min(int(page)+1,total_pages)),
    },
    'meta':{
    'current_page':page,
    'from':1,
    'last_page':total_pages,
    'links':[
        {
        'url':None,
        'label':1,
        'active':False,
        },
        {
        'url':path,
        'label':1,
        'active':True,
        },
        {
        'url':None,
        'label':1,
        'active':False,
        },
        {
        'url':path,
        'label':1,
        'active':False,
        },
    ],
    'path': path,
    'per_page':PAGE_SIZE,
    'to':skip,
    'total':total_count
    }
    }
    additional_data = {"discount_label": None, "stock": {"available": True}}
    for i in result["data"]["products"]:
        if i:
            i.update(additional_data)
    
    return result
    


@app.get("/autocomplete")
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
def filter_product(filters_for:str,page:str,filters_for_id:str,storeid:str,sort_by:Optional[str]=None,categories: list = Query(None),brandIds: list = Query(None)):
    filter_query={}
    if filters_for=='brand':
        filter_query['brand.id']= filters_for_id
    elif filters_for=='cl1':
        filter_query['category_level.cl1_id']=filters_for_id
    elif filters_for=='cl2':
        filter_query['category_level.cl2_id'] = filters_for_id
    elif filters_for=='cl3':
        filter_query['category_level.cl3_id']=filters_for_id
    elif filters_for=='cl4':
        filter_query['category_level.cl4_id']=filters_for_id
    
    aggregation_pipeline = [

        {'$match': filter_query},
        {"$project": {"_id": 0,'category_level':0}},
        
    ]
    filter_data=[
            {
                "name": "Brands",
                "key": "brand",
                "data":None,
              },
            {
                "name": "Categories",
                "key": "category",
                "data": None
            }
        ],
    sort_data= [
            {
                "key": "new",
                "type": "Latest",
                "is_active": False
            },
            {
                "key": "min_price",
                "type": "Lowest Price",
                "is_active": False
            },
            {
                "key": "max_price",
                "type": "Highest Price",
                "is_active": False
            },
            {
                "key": "popular",
                "type": "Highest Popularity",
                "is_active": False
            },
            {
                "key": "relevance",
                "type": "Relevance",
                "is_active": False
            }
        ]
    for i in range(len(sort_data)):
        if sort_data[i]['key']==sort_by:
            sort_data[i]['is_active']=True
    sort_query={}
    category_ids=[]
    brand_ids=[]
  
    if categories:
        for category in categories:
            category_ids.append(category)
        aggregation_pipeline.append({'$match':{'category.id':{'$in':category_ids}}})
    if brandIds:
        for brandId in brandIds:
            brand_ids.append(brandId)
        aggregation_pipeline.append({'$match':{'brand.id':{'$in':brand_ids}}})
    aggregation_pipeline.append({"$lookup" : {
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
                                 '$store_id', storeid
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
            }},{"$skip": skip},
        {'$limit': PAGE_SIZE}

)
    if sort_by:
        if sort_by=='min_price':
            sort_query['price']=1
        elif sort_by=='max_price':
            sort_query['price']=-1
        aggregation_pipeline.append({'$sort':sort_query})


        
    aggregation_pipeline.append({'$limit': PAGE_SIZE})
    print(aggregation_pipeline)
    ans=list(DB["my_data"].aggregate(aggregation_pipeline))
   
    result={
        'data':ans,
        'links':{
            'first':None,
            'last':None,
            'prev':None,
            'next':None,
                },
        'meta':{
            'current_page':page,
            'from':1,
            'last_page':int(page)-1,
            'links':[
                  {
                    'url':None,
                    'label':1,
                    'active':True,
                    }
                ],
        'path':'',
        'per_page':PAGE_SIZE,
        'to':len(ans),
        'total':len(ans),
        'filters':filter_data,
        'sorts':sort_data
    }
    
    }
    return result




