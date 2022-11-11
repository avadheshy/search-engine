import codecs
import math
import collections
from fastapi import FastAPI, Request,UploadFile,File
import csv
import sentry_sdk
from pymongo import UpdateMany,UpdateOne
from constants import ERROR_RESPONSE_DICT_FORMAT, CATEGORY_LEVEL_MAPPING, STORE_WH_MAP
from pipelines import get_search_pipeline, group_autocomplete_stage, get_listing_pipeline_for_retail, get_listing_pipeline_for_mall
from search_utils import SearchUtils
from settings import DB
import json

sentry_sdk.init(
    # dsn="https://fa760c10052c4da88ba731dc48c9020e@o1194835.ingest.sentry.io/6768729",

    # # Set traces_sample_rate to 1.0 to capture 100%
    # # of transactions for performance monitoring.
    # # We recommend adjusting this value in production,
    # traces_sample_rate=1.0,
)

app = FastAPI()




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



@app.post("/v1/product-listing/")
async def product_listing_v1(request: Request):
    error_response_dict = ERROR_RESPONSE_DICT_FORMAT
    request_data = await request.json()
    def get_typcasted_data(request_data):
        typcasted_data = dict()
        try:
            typcasted_data["store_id"] = str(request_data.get("store_id"))
            typcasted_data["page"] = int(request_data.get('page')) if request_data.get('page') else 1
            typcasted_data["per_page"] = int(request_data.get('per_page')) if request_data.get(
                'per_page') else 15
            typcasted_data["filters_for"] = request_data.get("filters_for")
            typcasted_data["filters_for_id"] = int(request_data.get("filters_for_id"))
            typcasted_data["sort_by"] = request_data.get("sort_by") if request_data.get("sort_by") else None
            typcasted_data["type"] = request_data.get("type")
            if isinstance(request_data.get("brandIds"), list):
                typcasted_data["brandIds"] = list(map(int, request_data.get("brandIds")))
            
            if isinstance(request_data.get("categories"), list):
                typcasted_data["categories"] = list(map(int, request_data.get("categories")))
        except Exception as error:
            typcasted_data["error_msg"] = f"{error}"
        return typcasted_data

    typcasted_data = get_typcasted_data(request_data)
    if typcasted_data.get("error_msg"):
        error_response_dict["message"] = typcasted_data.get("error_msg")
        return error_response_dict
    sort_by = typcasted_data.get("sort_by")
    offset = 0
    limit = 15
    
    if typcasted_data["page"] and typcasted_data["per_page"]:
        offset = (typcasted_data["page"] - 1) * typcasted_data["per_page"]
        limit = typcasted_data["per_page"]
    if typcasted_data.get("type") not in ["mall", "retail"]:
        error_response_dict["message"] = "Invalid type"
        return error_response_dict
    if typcasted_data.get("filters_for") not in ["brand", "category", "tag", "group", "cl1",
                                                 "cl2", "cl3", "cl4"]:
        error_response_dict["message"] = "Invalid filters_for"
        return error_response_dict
    # if typcasted_data.get("sort_by") not in ["new", "min_price", "max_price", "popular", "relevance",
    #                                          "product_created_at"]:
        # error_response_dict["message"] = "Invalid sort_by"
        # return error_response_dict

    # sorting based on given name
    sort_query = {}
    if sort_by:
        if sort_by == 'new':
            sort_query['updated_at'] = -1
        elif sort_by == 'min_price':
            sort_query['price'] = 1
        elif sort_by == 'max_price':
            sort_query['price'] = -1
        # elif sort_by == 'relevance':
        #     sort_query['score'] = -1
        else:
            sort_query['created_at'] = -1

    filter_kwargs = dict(
        store_id=typcasted_data["store_id"],
        is_mall="0",
        status="1"
    )
    
    warehouse_id = STORE_WH_MAP.get(typcasted_data.get("store_id"))
    filter_kwargs_for_mall = dict(
        is_mall="1",
        status=1
    )
    brand_ids_input=[]
    category_ids_input=[]
    only_brand_data, only_category_data, both_brand_and_category_data = False, False, False
    if typcasted_data.get("brandIds"):
        only_category_data = True
        filter_kwargs["brand_id"] = {"$in": typcasted_data.get("brandIds")}
        filter_kwargs_for_mall["brand_id"] = {"$in": typcasted_data.get("brandIds")}
        brand_ids_input.extend(typcasted_data.get("brandIds"))
    if typcasted_data.get("categories"):
        only_brand_data = True
        filter_kwargs["category_id"] = {"$in": typcasted_data.get("categories")}
        filter_kwargs_for_mall["category_id"] = {"$in": typcasted_data.get("categories")}
        category_ids_input.extend(typcasted_data.get("categories"))
    if typcasted_data.get("filters_for") == "brand":
        only_category_data = True
        filter_kwargs["brand_id"] = typcasted_data.get("filters_for_id")
        filter_kwargs_for_mall["brand_id"] = typcasted_data.get("filters_for_id")
        brand_ids_input.append(typcasted_data.get("filters_for_id"))
    elif typcasted_data.get("filters_for") == "category":
        only_brand_data = True
        filter_kwargs["category_id"] = typcasted_data.get("filters_for_id")
        filter_kwargs_for_mall["category_id"] = get_typcasted_data.get("filters_for_id")
        category_ids_input.append(get_typcasted_data.get("filters_for_id"))
    elif typcasted_data.get("filters_for") == "group":
        both_brand_and_category_data = True
        filter_kwargs["group_id"] = typcasted_data.get("filters_for_id")
        filter_kwargs_for_mall["group_id"] = typcasted_data.get("filters_for_id")
    elif typcasted_data.get("filters_for") == "tag":
        both_brand_and_category_data = True
        filter_kwargs["tag_ids"] = str(typcasted_data.get("filters_for_id"))
        filter_kwargs_for_mall["tag_ids"] = str(typcasted_data.get("filters_for_id"))
    elif typcasted_data.get("filters_for") in ("cl1", "cl2", "cl3", "cl4"):
        only_brand_data = True
        filter_kwargs["cat_level"] = CATEGORY_LEVEL_MAPPING.get(typcasted_data.get("filters_for"))
        filter_kwargs["category_id"] = typcasted_data.get("filters_for_id")
        filter_kwargs_for_mall["cat_level"] = CATEGORY_LEVEL_MAPPING.get(typcasted_data.get("filters_for"))
        filter_kwargs_for_mall["category_id"] = typcasted_data.get("filters_for_id")
        category_ids_input.append(typcasted_data.get("filters_for_id"))

    
    if typcasted_data.get("type") == "retail":
        pipeline = get_listing_pipeline_for_retail(filter_kwargs, sort_query, offset, limit)
        # print(pipeline)
        data = list(DB["product_store_sharded"].aggregate(pipeline))
    elif typcasted_data.get("type") == "mall":
        pipeline = get_listing_pipeline_for_mall(warehouse_id, filter_kwargs_for_mall, sort_query, offset, limit)
        # print(pipeline)
        data = list(DB["search_products"].aggregate(pipeline))
    data_to_return = data[0].get("data")
    # print(data_to_return)
    num_found = data[0].get("numFound") or 0
    brand_ids = list(([str(product.get('brand_id')) for product in data_to_return if product.get('brand_id')])) 
    category_ids = list(([str(product.get('category_id')) for product in data_to_return if product.get('category_id')]))
    dict_brand_ids=collections.Counter(brand_ids)
    dict_category_ids=collections.Counter(category_ids)
    brand_filter = {
        "id": {"$in": list(dict_brand_ids.keys())}
    }
    brand_projection = {"_id": 0, "id": 1, "name": 1, "logo": 1}
    category_filter = {
        "id": {"$in": list(dict_category_ids.keys())},
        # "cat_level": "2"
    }
    category_projection = {"_id": 0, "id": 1, "name": 1, "icon": 1}
    brand_data, category_data = [], []
    if only_brand_data:
        brand_data = list(DB["brands"].find(brand_filter, brand_projection)) or []
    if only_category_data:
        category_data = list(DB["all_categories"].find(category_filter, category_projection)) or []
    if both_brand_and_category_data:
        brand_data = list(DB["brands"].find(brand_filter, brand_projection)) or []
        category_data = list(DB["all_categories"].find(category_filter, category_projection)) or []
    brand_data_to_return = SearchUtils.make_brand_data(brand_data,dict_brand_ids,brand_ids_input)
    category_data_to_return = SearchUtils.make_category_data(category_data,dict_category_ids,category_ids_input)
    
    final_result = {
        "count": len(data_to_return),
        "rows": typcasted_data.get("per_page"),
        "currentPage": typcasted_data.get("page"),
        "numFound": num_found,
        "lastPage": math.ceil(num_found/typcasted_data.get("per_page")),
        "productIds": [int(product.get('product_id')) for product in data_to_return],
        "groupIds": [product.get('group_id') for product in data_to_return],
        "filters": {
            "brands": brand_data_to_return,
            "categories": category_data_to_return
        }
    }
    return final_result




