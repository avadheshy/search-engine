import asyncio
from collections import Counter
from datetime import datetime
import json
import math

from fastapi import FastAPI, Request

from api_constants import ApiUrlConstants
from constants import ERROR_RESPONSE_DICT_FORMAT, CATEGORY_LEVEL_MAPPING, STORE_WH_MAP
from pipelines import get_search_pipeline, group_autocomplete_stage, listing_pipeline, get_listing_pipeline_for_retail, \
    get_listing_pipeline_for_mall, get_brand_and_category_ids_for_retail, get_brand_and_category_ids_for_mall, \
    get_brand_and_category_pipeline_for_mall
from settings import SHARDED_SEARCH_DB, loop, ASYNC_SHARDED_SEARCH_DB
from search_utils import SearchUtils

app = FastAPI()


@app.get("/")
async def read_main():
    return {"msg": "Hello World"}


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
        limit = int(request_data.get("limit")) if request_data.get("limit") else 10
        # DB Query
        pipe_line = get_search_pipeline(keyword, store_id, platform, order_type, skip, limit)
        if order_type == 'mall':
            response = SHARDED_SEARCH_DB["search_products"].aggregate(pipe_line).next()
        else:
            response = SHARDED_SEARCH_DB["product_store_sharded"].aggregate(pipe_line).next()

        # Response Formatting
        response["data"] = (
            [i.get("id") for i in response["data"]] if response["data"] else []
        )
        count = response["total"][0] if response["total"] else {}
        response["total"] = count.get("count") if count else 0
    except Exception as error:
        error_message = "{0}".format(error)

    # ...............REQUEST, RESPONSE, ERROR DB LOG ...................

    SHARDED_SEARCH_DB["search_log_5"].insert_one(
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
    is_group = request_data.get("should_group", "").lower()
    if is_group == 'false':
        pipe_line = get_search_pipeline(keyword, store_id, platform, order_type, skip, limit)
        if order_type == 'mall':
            response = SHARDED_SEARCH_DB["search_products"].aggregate(pipe_line).next()
        else:
            response = SHARDED_SEARCH_DB["product_store_sharded"].aggregate(pipe_line).next()
    else:
        pipe_line = group_autocomplete_stage(
            keyword, store_id, platform, order_type, skip, limit
        )
        # print("pipe_line : ", pipe_line)
        if order_type == 'mall':
            response = SHARDED_SEARCH_DB["search_products"].aggregate(pipe_line).next()
        else:
            response = SHARDED_SEARCH_DB["product_store_sharded"].aggregate(pipe_line).next()

    # Response Formatting
    response["data"] = (
        [str(i.get("id")) for i in response["data"]] if response["data"] else []
    )
    count = response["total"][0] if response["total"] else {}
    response["total"] = count.get("count") if count else 0

    SHARDED_SEARCH_DB["group_log_2"].insert_one(
        {"request": request_data, "response": response}
    )

    # ...................................................

    return response


@app.get("/store_map")
def store_warehouse_map(request: Request):
    wh_store_map = list(SHARDED_SEARCH_DB['stores'].find({}, {"fulfil_warehouse_id": 1, "id": 1, "_id": 0}))
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

    response = SHARDED_SEARCH_DB['product_store_sharded'].aggregate(LISTING_PIPELINE).next()
    response["data"] = (
        [i.get("id") for i in response["data"]] if response["data"] else []
    )
    count = response["total"][0] if response["total"] else {}
    response["total"] = count.get("count") if count else 0
    return response


@app.post(ApiUrlConstants.V1_PRODUCT_LISTING)
async def product_listing_v1(request: Request):
    error_response_dict = ERROR_RESPONSE_DICT_FORMAT
    request_data = await request.json()
    x_source = request_data.get('x_source')

    def get_typcasted_data(request_data):
        typcasted_data = dict()
        try:
            typcasted_data["store_id"] = str(request_data.get("store_id"))
            typcasted_data["page"] = int(request_data.get('page')) if request_data.get('page') else 1
            typcasted_data["per_page"] = int(request_data.get('per_page')) if request_data.get(
                'per_page') else 15
            typcasted_data["filters_for"] = request_data.get("filters_for")
            typcasted_data["filter_id"] = int(request_data.get("filter_id"))
            typcasted_data["sort_by"] = request_data.get("sort_by") if request_data.get("sort_by") else None
            typcasted_data["type"] = request_data.get("type")
            if isinstance(request_data.get("brandIds"), dict):
                typcasted_data["brandIds"] = list(map(int, request_data.get("brandIds").values())) if request_data.get(
                    "brandIds") else None

            if isinstance(request_data.get("categories"), dict):
                typcasted_data["categories"] = list(
                    map(int, request_data.get("categories").values())) if request_data.get("categories") else None
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
    if sort_by == 'new':
        sort_query['created_at'] = 1
    elif sort_by == 'min_price':
        sort_query['price'] = 1
        sort_query['updated_at'] = -1
    elif sort_by == 'max_price':
        sort_query['price'] = -1
        sort_query['updated_at'] = -1
    elif sort_by == 'relevance':
        sort_query['updated_at'] = 1
    elif sort_by == 'popular':
        sort_query['ps'] = -1
        sort_query['updated_at'] = -1
    else:
        sort_query['updated_at'] = -1

    filter_kwargs = dict(
        store_id=typcasted_data["store_id"],
        is_mall="0",
        status="1",
        inv_qty={"$gt": 0}
    )
    filter_kwargs_for_brand_and_cat = dict(
        is_mall="1" if typcasted_data.get("type") == "mall" else "0",
        status=1
    )
    if typcasted_data.get("type") == "retail":
        filter_kwargs_for_brand_and_cat["store_id"] = typcasted_data["store_id"]

    warehouse_id = STORE_WH_MAP.get(typcasted_data.get("store_id"))
    filter_kwargs_for_mall = dict(
        is_mall="1",
        status=1
    )
    brand_ids_input = []
    category_ids_input = []
    only_brand_data, only_category_data, both_brand_and_category_data = False, False, False
    if typcasted_data.get("brandIds"):
        only_category_data = True
        filter_kwargs["brand_id"] = {"$in": typcasted_data.get("brandIds")}
        filter_kwargs_for_mall["brand_id"] = {"$in": typcasted_data.get("brandIds")}
        # filter_kwargs_for_brand_and_cat["brand_id"] = {"$in": typcasted_data.get("brandIds")}
        brand_ids_input.extend(typcasted_data.get("brandIds"))
    if typcasted_data.get("categories"):
        only_brand_data = True
        filter_kwargs["category_id"] = {"$in": typcasted_data.get("categories")}
        filter_kwargs_for_mall["category_id"] = {"$in": typcasted_data.get("categories")}
        # filter_kwargs_for_brand_and_cat["category_id"] = {"$in": typcasted_data.get("categories")}
        category_ids_input.extend(typcasted_data.get("categories"))
    if typcasted_data.get("filters_for") == "brand":
        only_category_data = True
        filter_kwargs["brand_id"] = typcasted_data.get("filter_id")
        filter_kwargs_for_brand_and_cat["brand_id"] = typcasted_data.get("filter_id")
        filter_kwargs_for_mall["brand_id"] = typcasted_data.get("filter_id")
        brand_ids_input.append(typcasted_data.get("filter_id"))
    elif typcasted_data.get("filters_for") == "category":
        only_brand_data = True
        filter_kwargs["category_id"] = typcasted_data.get("filter_id")
        filter_kwargs_for_brand_and_cat["category_id"] = typcasted_data.get("filter_id")
        filter_kwargs_for_mall["category_id"] = typcasted_data.get("filter_id")
        category_ids_input.append(typcasted_data.get("filter_id"))
    elif typcasted_data.get("filters_for") == "group":
        both_brand_and_category_data = True
        filter_kwargs["group_id"] = typcasted_data.get("filter_id")
        filter_kwargs_for_brand_and_cat["group_id"] = typcasted_data.get("filter_id")
        filter_kwargs_for_mall["group_id"] = typcasted_data.get("filter_id")
    elif typcasted_data.get("filters_for") == "tag":
        both_brand_and_category_data = True
        filter_kwargs["tag_ids"] = str(typcasted_data.get("filter_id"))
        filter_kwargs_for_brand_and_cat["tag_ids"] = str(typcasted_data.get("filter_id"))
        filter_kwargs_for_mall["tag_ids"] = str(typcasted_data.get("filter_id"))
    elif typcasted_data.get("filters_for") in ("cl1", "cl2", "cl3", "cl4"):
        only_brand_data = True
        filter_kwargs["cat_level"] = CATEGORY_LEVEL_MAPPING.get(typcasted_data.get("filters_for"))
        filter_kwargs["category_id"] = typcasted_data.get("filter_id")
        filter_kwargs_for_mall["cat_level"] = CATEGORY_LEVEL_MAPPING.get(typcasted_data.get("filters_for"))
        filter_kwargs_for_mall["category_id"] = typcasted_data.get("filter_id")
        filter_kwargs_for_brand_and_cat["cat_level"] = CATEGORY_LEVEL_MAPPING.get(typcasted_data.get("filters_for"))
        filter_kwargs_for_brand_and_cat["category_id"] = typcasted_data.get("filter_id")
        category_ids_input.append(typcasted_data.get("filter_id"))

    brand_ids, category_ids = [], []
    if typcasted_data.get("type") == "retail":
        pipeline = get_listing_pipeline_for_retail(filter_kwargs, sort_query, offset, limit)
        data = list(SHARDED_SEARCH_DB["product_store_sharded"].aggregate(pipeline))
        brand_ids, category_ids = get_brand_and_category_ids_for_retail(filter_kwargs_for_brand_and_cat)
    elif typcasted_data.get("type") == "mall":
        pipeline = get_listing_pipeline_for_mall(warehouse_id, filter_kwargs_for_mall, sort_query, offset, limit)
        brand_category_pipeline = get_brand_and_category_pipeline_for_mall(filter_kwargs_for_brand_and_cat,
                                                                           warehouse_id)
        # TODO code of parallel DB calls
        # combined_data = loop.run_until_complete(
        #     asyncio.gather(*[
        #         ASYNC_SHARDED_SEARCH_DB["search_products"].aggregate(pipeline).to_list(None),
        #         ASYNC_SHARDED_SEARCH_DB["search_products"].aggregate(brand_category_pipeline).to_list(None),
        #     ]))
            # parallel_db_calls_for_mall_listing_api(pipeline, filter_kwargs_for_brand_and_cat, warehouse_id))
        # data = list(combined_data[0])
        # brand_category_data = list(combined_data[1])
        data = list(SHARDED_SEARCH_DB["search_products"].aggregate(pipeline))
        brand_category_data = list(SHARDED_SEARCH_DB["search_products"].aggregate(brand_category_pipeline))
        brand_ids, category_ids = get_brand_and_category_ids_for_mall(brand_category_data)

    data_to_return = data[0].get("data")
    num_found = data[0].get("numFound") or 0

    dict_brand_ids = Counter(brand_ids)
    dict_category_ids = Counter(category_ids)
    brand_filter = {
        "id": {"$in": brand_ids}
    }
    brand_projection = {"_id": 0, "id": 1, "name": 1, "logo": 1}
    category_filter = {
        "id": {"$in": category_ids},
        "cat_level": "2"
    }
    category_projection = {"_id": 0, "id": 1, "name": 1, "icon": 1}
    brand_data, category_data = [], []
    if only_brand_data:
        brand_data = list(SHARDED_SEARCH_DB["brands"].find(brand_filter, brand_projection)) or []
    if only_category_data:
        category_data = list(SHARDED_SEARCH_DB["all_categories"].find(category_filter, category_projection)) or []
    if both_brand_and_category_data:
        brand_data = list(SHARDED_SEARCH_DB["brands"].find(brand_filter, brand_projection)) or []
        category_data = list(SHARDED_SEARCH_DB["all_categories"].find(category_filter, category_projection)) or []
    brand_data_to_return = SearchUtils.make_brand_data(brand_data, dict_brand_ids, brand_ids_input)
    category_data_to_return = SearchUtils.make_category_data(category_data, dict_category_ids, category_ids_input)

    if x_source == "android_app":
        filters_data = [
            {
                "name": "Brands",
                "key": "brand",
                "data": brand_data_to_return
            },
            {
                "name": "Categories",
                "key": "category",
                "data": category_data_to_return
            }
        ]
    else:
        filters_data = {
            "brands": brand_data_to_return,
            "categories": category_data_to_return
        }

    final_result = {
        "count": len(data_to_return),
        "rows": typcasted_data.get("per_page"),
        "currentPage": typcasted_data.get("page"),
        "numFound": num_found,
        "lastPage": math.ceil(num_found / typcasted_data.get("per_page")),
        "productIds": [int(product.get('product_id')) for product in data_to_return],
        "groupIds": [product.get('group_id') for product in data_to_return],
        "filters": filters_data
    }

    log_payload = {'created_at': datetime.now(), 'headers': {"x_source": x_source}, 'request': request_data,
                   'response': final_result}
    SHARDED_SEARCH_DB['product_listing_log'].insert_one(log_payload)
    return final_result
