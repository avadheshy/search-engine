import codecs
import json
from fastapi import FastAPI, Request, Query, HTTPException,File, UploadFile
from typing import Optional, List
from pymongo import MongoClient,UpdateOne,UpdateMany
from pipelines import get_search_pipeline, group_autocomplete_stage, listing_pipeline
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
CLIENT = MongoClient("mongodb+srv://sharded-search-service:KC2718oU0Jt9Qt7v@search-service.ynzkd.mongodb.net/test")

DB = CLIENT.product_search


@app.post('/boost')
async def add_booster(file: UploadFile = File(...)):
    print(file)
    data = csv.DictReader(codecs.iterdecode(file.file, 'utf-8'))
    return list(data)
    # print(data)
    # new_data = []
    # for d in data:
    #     new_data.append(d)
    # return new_data
    # file_obj = files.file
    # timestamp = str(int(datetime.timestamp(datetime.now())))
    # upload_file_name = '/tmp/' + timestamp + files.filename
    # async with aiofiles.open(upload_file_name, 'wb+') as temp_file:
    #     content = await files.read()
    #     temp_file.write(content)
    #     # for chunk in files.:
    #     #     temp_file.write(chunk)
    # print(temp_file)
    # async with aiofiles.open(file_obj, 'r') as file:
    #     contents = await file.read()
    # file_data = csv.DictReader(contents)
    # print(file_data, "Files ")
    # for row in file_data:
    #     c = 1
    #     print("HEYYY")  
    #     # key = row['Id']  # Assuming a column named 'Id' to be the primary key
    #     # data[key] = row  
    #     print(row)
    #     break
    # print(c)
    # # data = {}
    # # contents = files.file.read()
    # # buffer = StringIO(contents.decode('utf-8'))
    # # csvReader = csv.DictReader(buffer)
    # # for row in file_data:
    # #     print("HEYYY")  
    # #     # key = row['Id']  # Assuming a column named 'Id' to be the primary key
    # #     # data[key] = row  
    # #     print(row)
    # #     break
    
    # # buffer.close()
    # # files.file.close()
    # return True


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
        store_id = request_data.get("store_id") # mall / retail
        keyword = request_data.get("keyword")
        platform = request_data.get("platform")  # pos / app
        skip = int(request_data.get("skip")) if request_data.get("skip") else 0
        limit = int(request_data.get("limit")) if request_data.get("limit") else 10
        # DB Query
        pipe_line = get_search_pipeline(keyword, store_id, platform, order_type, skip, limit)
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

    # DB["search_log_3"].insert_one(
    #     {"request": request_data, "response": response, "msg": error_message}
    # )

    # ...................................................
    return response

@app.get("/v2/search")
def product_search(request: Request):
    """
    Product Search API, This will help to discover the relevant products
    """
    # raise HTTPException(status_code=400, detail="Request is not accepted!")
    request_data = dict(request.query_params.items())
    response = {"total": 0, "data": []}
    error_message = ""
    # Request Parsing
    user_id = request_data.get("user_id")
    order_type = request_data.get("type") # mall / retail
    store_id = request_data.get("store_id")  
    keyword = request_data.get("keyword")
    platform = request_data.get("platform")  # pos / app
    skip = int(request_data.get("skip")) if request_data.get("skip") else 0
    limit = int(request_data.get("limit")) if request_data.get("limit") else 10

    # MongoDB Aggregation Pipeline
    pipe_line = group_autocomplete_stage(
        keyword, store_id, platform, order_type, skip, limit
    )

    # DB Query
    response = DB["product_store_sharded"].aggregate(pipe_line).next()

    # Response Formatting
    response["data"] = (
        [i.get("id") for i in response["data"]] if response["data"] else []
    )
    count = response["total"][0] if response["total"] else {}
    response["total"] = count.get("count") if count else 0

    DB["group_log_1"].insert_one(
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



@app.get("/v1/products")
def filter_product(request: Request):
    request_data = dict(request.query_params.items())
    category_ids = request_data.get('category_ids')
    brand_ids = request_data.get('brand_ids')
    store_id = request_data.get('store_id')
    user_id = request_data.get("user_id")
    order_type = request_data.get("type")
    platform = request_data.get("platform")
    skip = int(request_data.get('skip') or 0)
    limit = int(request_data.get('limit') or 10)

    match_filter = {'store_id': store_id}
    if category_ids:
        category_ids = json.loads(category_ids)
        match_filter['category_id'] = {'$in': category_ids}
    if brand_ids:
        brand_ids = json.loads(brand_ids)
        match_filter['brand_id'] = {'$in': brand_ids}

    LISTING_PIPELINE = listing_pipeline(skip, limit, match_filter)

    response = DB['product_store_sharded'].aggregate(LISTING_PIPELINE).next()
    response["data"] = (
            [i.get("id") for i in response["data"]] if response["data"] else []
        )
    count = response["total"][0] if response["total"] else {}
    response["total"] = count.get("count") if count else 0
    return response

 
