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
db=CLIENT.search


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
    request_data = dict(request.query_params.items())
    filters_for=request_data.get('filters_for')
    filters_for_id=request_data.get('filters_for_id')
    category_ids = request_data.get('category_ids')
    brand_ids = request_data.get('brand_ids')
    store_id = request_data.get('store_id')
    sort_by = request_data.get('sort_by')
    user_id = request_data.get("user_id")
    type = request_data.get("type")
    page = request_data.get("page",0) 
    skip = (int(page)-1)*10
    limit = int(page)*10
    response=[]
    if type=='mall':
        LISTING_PIPELINE=listing_pipeline_mall(filters_for,filters_for_id,store_id, brand_ids,category_ids,sort_by,skip, limit)
        print(LISTING_PIPELINE)
        response = db['list_product'].aggregate(LISTING_PIPELINE).next()
    else:
        LISTING_PIPELINE = listing_pipeline_retail(filters_for,filters_for_id,store_id, brand_ids,category_ids,sort_by,skip, limit)
        print(LISTING_PIPELINE)
        response = db['list_product'].aggregate(LISTING_PIPELINE).next()
    
    response["data"] = (
        [i.get("id") for i in response["data"]] if response["data"] else []
    )
    count = response["total"][0] if response["total"] else {}
    response["total"] = count.get("count") if count else 0
    return response
