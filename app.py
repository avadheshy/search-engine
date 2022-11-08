import codecs
import json
from fastapi import FastAPI, Request, Query, HTTPException, File, UploadFile
from pymongo import MongoClient, UpdateOne, UpdateMany
from pipelines import get_search_pipeline, group_autocomplete_stage, listing_pipeline_mall,listing_pipeline_retail
from constants import PAGE_SIZE
import csv
from io import BytesIO
from io import StringIO
import sentry_sdk

sentry_sdk.init(
    # dsn="https://fa760c10052c4da88ba731dc48c9020e@o1194835.ingest.sentry.io/6768729",

    # # Set traces_sample_rate to 1.0 to capture 100%
    # # of transactions for performance monitoring.
    # # We recommend adjusting this value in production,
    # traces_sample_rate=1.0,
)

app = FastAPI()
CLIENT = MongoClient(
    "mongodb+srv://sharded-search-service:KC2718oU0Jt9Qt7v@search-service.ynzkd.mongodb.net/test")

DB = CLIENT.product_search


@app.post('/boost')
async def add_booster(request: Request, file: UploadFile = File(...)):
    csvReader = csv.DictReader(codecs.iterdecode(file.file, 'utf-8'))
    product_ids = []
    for rows in csvReader:
        print('hello')
        product_ids.append(rows.get('PID'))
    payload1 = []
    payload2 = []
    for product_id in product_ids:
        payload1.append(UpdateOne({'id': product_id}, {
                        '$set': {'winter_sale': 1}}))
        payload1.append(UpdateMany({'id': product_id}, {
                        '$set': {'winter_sale': 1}}))
    print(len(payload1))
    # if payload1:
    #     DB['search_products'].bulk_write(payload1)
    # if payload2:
    #     DB['product_store_sharded'].bulk_write(payload2)
    return True


@app.post("/v1/search")
async def product_search(request: Request):
    """
    Product Search API, This will help to discover the relevant products
    """
    # raise HTTPException(status_code=400, detail="Request is not accepted!")
    request_data = await request.json()
    response = {"total": 0, "data": []}
    error_message = ""
    try:
        # Request Parsing
        user_id = request_data.get("user_id")
        order_type = request_data.get("type")
        store_id = request_data.get("store_id")  # mall / retail
        keyword = request_data.get("keyword")
        platform = request_data.get("platform")  # pos / app
        skip = int(request_data.get("skip")) if request_data.get("skip") else 0
        limit = int(request_data.get("limit")
                    ) if request_data.get("limit") else 10
        # DB Query
        pipe_line = get_search_pipeline(
            keyword, store_id, platform, order_type, skip, limit)
        print(pipe_line)

        if order_type == 'mall':
            response = DB["search_products"].aggregate(pipe_line).next()
        else:
            response = DB["product_store_sharded"].aggregate(pipe_line).next()

        # Response Formatting
        response["data"] = (
            [i.get("id") for i in response["data"]] if response["data"] else []
        )
        count = response["total"][0] if response["total"] else {}
        response["total"] = count.get("count") if count else 0
    except Exception as error:
        error_message = "{0}".format(error)

    # ...............REQUEST, RESPONSE, ERROR DB LOG ...................

    DB["search_log_5"].insert_one(
        {"request": request_data, "response": response, "msg": error_message}
    )

    # ...................................................
    return response


@app.get("/v2/search")
def product_search_v2(request: Request):
    """
    Product Search API, This will help to discover the relevant products
    """
    # raise HTTPException(status_code=400, detail="Request is not accepted!")
    request_data = dict(request.query_params.items())
    response = {"total": 0, "data": []}
    error_message = ""
    # Request Parsing
    user_id = request_data.get("user_id")
    order_type = request_data.get("type")  # mall / retail
    store_id = request_data.get("store_id")
    keyword = request_data.get("keyword")
    platform = request_data.get("platform")  # pos / app
    skip = int(request_data.get("skip")) if request_data.get("skip") else 0
    limit = int(request_data.get("limit")) if request_data.get("limit") else 10

    # MongoDB Aggregation Pipeline
    pipe_line = group_autocomplete_stage(
        keyword, store_id, platform, order_type, skip, limit
    )
    print(pipe_line)

    # DB Query
    if order_type == 'mall':
        response = DB["search_products"].aggregate(pipe_line).next()
    else:
        response = DB["product_store_sharded"].aggregate(pipe_line).next()

    # Response Formatting
    response["data"] = (
        [str(i.get("id")) for i in response["data"]] if response["data"] else []
    )
    count = response["total"][0] if response["total"] else {}
    response["total"] = count.get("count") if count else 0

    DB["group_log_2"].insert_one(
        {"request": request_data, "response": response}
    )

    # ...................................................

    return response


@app.get("/store_map")
def store_warehouse_map(request: Request):
    wh_store_map = list(DB['stores'].find(
        {}, {"fulfil_warehouse_id": 1, "id": 1, "_id": 0}))
    WAREHOUSE_KIRANA_MAP = {}
    for i in wh_store_map:
        WAREHOUSE_KIRANA_MAP[i.get('id')] = i.get('fulfil_warehouse_id')
    return WAREHOUSE_KIRANA_MAP


@app.get("/v3/product_listing")
def filter_product(request: Request):
    print('hello3')
    request_data = dict(request.query_params.items())
    filters_for=request_data.get('filters_for')
    filters_for_id=request_data.get('filters_for_id')
    categories = request_data.get('categories')
    brandIds = request_data.get('brandIds')
    store_id = request_data.get('store_id')
    sort_by = request_data.get('sort_by')
    user_id = request_data.get("user_id")
    type = request_data.get("type")
    page=int(request_data.get("page")) if request_data.get("page") else 0 
    per_page = int(request_data.get("per_page")) if request_data.get("per_page") else 15
    skip = (int(page)-1)*15
    limit = per_page
    brands_input=[]
    if filters_for=='brand':
        brands_input.append(filters_for_id)
    if brandIds:
        brands_input.extend(brandIds)
    category_input=[]
    if filters_for in ('cl1','cl2','cl3','cl4'):
        category_input.append(filters_for_id)
    if categories:
        category_input.extend(categories)
    response=[]
    if type=='mall':
        print('hello1')
        LISTING_PIPELINE=listing_pipeline_mall(filters_for,filters_for_id,store_id, brandIds,categories,sort_by,skip, limit)
        print(LISTING_PIPELINE)
        print('hello')
        response = DB['search_products'].aggregate(LISTING_PIPELINE).next()
    else:
        LISTING_PIPELINE = listing_pipeline_retail(filters_for,filters_for_id,store_id, brandIds,categories,sort_by,skip, limit)
        print(LISTING_PIPELINE)
        response = DB['list_products'].aggregate(LISTING_PIPELINE).next()
    Response={}
    
    # print(response)
    dict_category_ids={}
    Response["productIds"] = (
        [i.get("id") for i in response["data"]] if response["data"] else []
    )
    Response["groupIds"] = (
        [i.get("group_id") for i in response["data"]] if response["data"] else []
    )
    brands=[]
    dict_brand_ids={}
    brand_data=[]
    category=[]
    dict_category_ids={}
    category_data=[]
    print(brands_input)
    if response['data']:
        for i in response['data']:
            brands.append(i.get('brand_id'))
            category.append(i.get('category_id'))
        for id in brands:
            if dict_brand_ids.get(id):
                dict_brand_ids[id]+=1
            else:
                dict_brand_ids[id]=1
    
        brand_result=list(DB['brands'].aggregate([{'$match':{'id':{'$in':list(map(str,dict_brand_ids.keys()))}}},{'$project':{'id':1,'_id':0,'name':1,'logo':1}}]))       
        for i in brand_result:
            brand_data.append({
            'id':i.get('id'),
            'name':i.get('name'),
            'active':True if i.get('id') in brands_input else False,
            'logo':f"https://s3-ap-south-1.amazonaws.com/niyoos-test/media/brand/{i.get('id')}/{i.get('logo')}",
            'icon':f"https://s3-ap-south-1.amazonaws.com/niyoos-test/media/brand/{i.get('id')}/{i.get('logo')}",
            'count':dict_brand_ids.get(int(i.get('id'))),
            "type": "brand",
            "filter_key": "brandIds[]"
                
            })
        for id in category:
            if dict_category_ids.get(id):
                dict_category_ids[id]+=1
            else:
                dict_category_ids[id]=1
        category_result=list(DB['categories'].aggregate([{'$match':{'id':{'$in':list(map(str,dict_category_ids.keys()))}}},{'$project':{'id':1,'_id':0,'name':1}}]))       
        for i in category_result:
            category_data.append({
            'id':i.get('id'),
            'name':i.get('name'),
            'active':True if i.get('id') in category_input else False,
            'icon':None,
            'slug':None,
            'count':dict_category_ids.get(int(i.get('id'))),
                
            })
        
   
    count = response["total"][0] if response["total"] else {}
    last_page=Response["total"] = count.get("count") if count else 0
    Response['numFound']=int(last_page)
    Response['rows']=limit
    Response['currentPage']=page
    Response['lastPage']=(int(last_page)+int(per_page)-1)//int(per_page)
    Response['filters']={"brands":brand_data,
        "categories": category_data,
        "categories_l2": None
    }
    return Response
