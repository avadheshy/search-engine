import asyncio
from collections import Counter
from datetime import datetime
import json
import math

from fastapi import FastAPI, Request

from api_constants import ApiUrlConstants
from constants import ERROR_RESPONSE_DICT_FORMAT, CATEGORY_LEVEL_MAPPING, STORE_WH_MAP
from pipelines import get_search_pipeline, listing_pipeline, get_listing_pipeline_for_retail, \
    get_listing_pipeline_for_mall, get_brand_and_category_ids_for_retail, get_brand_and_category_ids_for_mall, \
    get_brand_and_category_pipeline_for_mall, group_pipeline_for_mall, group_pipeline_for_retail
from settings import SHARDED_SEARCH_DB, loop, ASYNC_SHARDED_SEARCH_DB
from search_utils import SearchUtils
from utils import CommonUtils

app = FastAPI()


@app.get("/")
async def read_main():
    return {"msg": "Hello World"}


@app.post(ApiUrlConstants.V1_PRODUCT_SEARCH)
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

    SHARDED_SEARCH_DB["search_log_6"].insert_one(
        {"created_at": datetime.now(), "request": request_data, "response": response, "msg": error_message}
    )

    # ...................................................

    return response


@app.get(ApiUrlConstants.V2_PRODUCT_SEARCH_FOR_GROUP)
def product_search_v2(request: Request):
    """
    Product Search API, This will help to discover the relevant products
    """
    request_data = dict(request.query_params.items())

    order_type = request_data.get("type")  # mall / retail
    store_id = request_data.get("store_id")
    keyword = request_data.get("keyword")
    platform = request_data.get("platform")  # pos / app
    skip = int(request_data.get("skip")) if request_data.get("skip") else 0
    limit = int(request_data.get("limit")) if request_data.get("limit") else 10
    is_group = request_data.get("should_group", "").lower()
    if is_group == 'false':     # calling v1/search api pipeline here
        pipe_line = get_search_pipeline(keyword, store_id, platform, order_type, skip, limit)
    else:
        if order_type == 'mall':
            pipe_line = group_pipeline_for_mall(keyword, store_id, platform, skip, limit)
        else:
            pipe_line = group_pipeline_for_retail(keyword, store_id, platform, skip, limit)

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


@app.get(ApiUrlConstants.STORE_MAP)
def store_warehouse_map(request: Request):
    wh_store_map = list(SHARDED_SEARCH_DB['stores'].find({}, {"fulfil_warehouse_id": 1, "id": 1, "_id": 0}))
    warehouse_kirana_map = {}
    for i in wh_store_map:
        warehouse_kirana_map[i.get('id')] = i.get('fulfil_warehouse_id')
    return warehouse_kirana_map


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

    def get_typecasted_data(request_data):
        typecasted_data = dict()
        try:
            typecasted_data["store_id"] = str(request_data.get("store_id"))
            typecasted_data["page"] = int(request_data.get('page')) if request_data.get('page') else 1
            typecasted_data["per_page"] = int(request_data.get('per_page')) if request_data.get(
                'per_page') else 15
<<<<<<< HEAD
            typcasted_data["filters_for"] = request_data.get("filters_for")
            typcasted_data["filter_id"] = int(request_data.get("filter_id"))
            typcasted_data["sort_by"] = request_data.get("sort_by") if request_data.get("sort_by") else None
            typcasted_data["type"] = request_data.get("type")
            if isinstance(request_data.get("brandIds"), list):
                typcasted_data["brandIds"] = list(map(int, request_data.get("brandIds")))
            if isinstance(request_data.get("categories"), list):
                typcasted_data["categories"] = list(map(int, request_data.get("categories")))
        except Exception as error:
            typcasted_data["error_msg"] = f"{error}"
        return typcasted_data
||||||| 0c653b4
            typcasted_data["filters_for"] = request_data.get("filters_for")
            typcasted_data["filter_id"] = int(request_data.get("filter_id"))
            typcasted_data["sort_by"] = request_data.get("sort_by")
            typcasted_data["type"] = request_data.get("type")
            if isinstance(request_data.get("brandIds"), list):
                typcasted_data["brandIds"] = list(map(int, request_data.get("brandIds")))
            if isinstance(request_data.get("categories"), list):
                typcasted_data["categories"] = list(map(int, request_data.get("categories")))
        except Exception as error:
            typcasted_data["error_msg"] = f"{error}"
        return typcasted_data
=======
            typecasted_data["filters_for"] = request_data.get("filters_for")
            typecasted_data["filter_id"] = int(request_data.get("filter_id"))
            typecasted_data["sort_by"] = request_data.get("sort_by") if request_data.get("sort_by") else None
            typecasted_data["type"] = request_data.get("type")
            if isinstance(request_data.get("brandIds"), dict):
                typecasted_data["brandIds"] = list(map(int, request_data.get("brandIds").values())) if request_data.get(
                    "brandIds") else None
>>>>>>> 20881d4fec90bad72d702b70b0b31843f2ec0903

<<<<<<< HEAD
    typcasted_data = get_typcasted_data(request_data)
    if typcasted_data.get("error_msg"):
        error_response_dict["message"] = typcasted_data.get("error_msg")
        return error_response_dict
    
    sort_by = typcasted_data.get("sort_by")
||||||| 0c653b4
    typcasted_data = get_typcasted_data(request_data)
    sort_by = typcasted_data.get("sort_by")
=======
            if isinstance(request_data.get("categories"), dict):
                typecasted_data["categories"] = list(
                    map(int, request_data.get("categories").values())) if request_data.get("categories") else None
        except Exception as error:
            typecasted_data["error_msg"] = f"{error}"
        return typecasted_data

    typecasted_data = get_typecasted_data(request_data)
    if typecasted_data.get("error_msg"):
        error_response_dict["message"] = typecasted_data.get("error_msg")
        return error_response_dict
    sort_by = typecasted_data.get("sort_by")
>>>>>>> 20881d4fec90bad72d702b70b0b31843f2ec0903
    offset = 0
    limit = 15
<<<<<<< HEAD
    if typcasted_data["page"] and typcasted_data["per_page"]:
        offset = (typcasted_data["page"] - 1) * typcasted_data["per_page"]
        limit = typcasted_data["per_page"]
    if typcasted_data.get("type") not in ["mall", "retail"]:
||||||| 0c653b4
    if typcasted_data["page"] and typcasted_data["per_page"]:
        offset = (typcasted_data["page"] - 1) * typcasted_data["per_page"]
        limit = typcasted_data["per_page"]
    if typcasted_data.get("error_msg"):
        error_response_dict["message"] = typcasted_data.get("error_msg")
        return error_response_dict
    if typcasted_data.get("type") not in ["mall", "retail"]:
=======

    if typecasted_data["page"] and typecasted_data["per_page"]:
        offset = (typecasted_data["page"] - 1) * typecasted_data["per_page"]
        limit = typecasted_data["per_page"]
    if typecasted_data.get("type") not in ["mall", "retail"]:
>>>>>>> 20881d4fec90bad72d702b70b0b31843f2ec0903
        error_response_dict["message"] = "Invalid type"
        return error_response_dict
    if typecasted_data.get("filters_for") not in ["brand", "category", "tag", "group", "cl1",
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
        store_id=typecasted_data["store_id"],
        is_mall="0",
        status="1",
        inv_qty={"$gt": 0}
    )
    filter_kwargs_for_brand_and_cat = dict(
        is_mall="1" if typecasted_data.get("type") == "mall" else "0",
        status=1
    )
    if typecasted_data.get("type") == "retail":
        filter_kwargs_for_brand_and_cat["store_id"] = typecasted_data["store_id"]

    warehouse_id = STORE_WH_MAP.get(typecasted_data.get("store_id"))
    filter_kwargs_for_mall = dict(
        is_mall="1",
        status=1
    )
    brand_ids_input = []
    category_ids_input = []
    only_brand_data, only_category_data, both_brand_and_category_data = False, False, False
    if typecasted_data.get("brandIds"):
        only_category_data = True
        filter_kwargs["brand_id"] = {"$in": typecasted_data.get("brandIds")}
        filter_kwargs_for_mall["brand_id"] = {"$in": typecasted_data.get("brandIds")}
        # filter_kwargs_for_brand_and_cat["brand_id"] = {"$in": typcasted_data.get("brandIds")}
        brand_ids_input.extend(typecasted_data.get("brandIds"))
    if typecasted_data.get("categories"):
        only_brand_data = True
        filter_kwargs["category_id"] = {"$in": typecasted_data.get("categories")}
        filter_kwargs_for_mall["category_id"] = {"$in": typecasted_data.get("categories")}
        # filter_kwargs_for_brand_and_cat["category_id"] = {"$in": typcasted_data.get("categories")}
        category_ids_input.extend(typecasted_data.get("categories"))
    if typecasted_data.get("filters_for") == "brand":
        only_category_data = True
        filter_kwargs["brand_id"] = typecasted_data.get("filter_id")
        filter_kwargs_for_brand_and_cat["brand_id"] = typecasted_data.get("filter_id")
        filter_kwargs_for_mall["brand_id"] = typecasted_data.get("filter_id")
        brand_ids_input.append(typecasted_data.get("filter_id"))
    elif typecasted_data.get("filters_for") == "category":
        only_brand_data = True
        filter_kwargs["category_id"] = typecasted_data.get("filter_id")
        filter_kwargs_for_brand_and_cat["category_id"] = typecasted_data.get("filter_id")
        filter_kwargs_for_mall["category_id"] = typecasted_data.get("filter_id")
        category_ids_input.append(typecasted_data.get("filter_id"))
    elif typecasted_data.get("filters_for") == "group":
        both_brand_and_category_data = True
        filter_kwargs["group_id"] = typecasted_data.get("filter_id")
        filter_kwargs_for_brand_and_cat["group_id"] = typecasted_data.get("filter_id")
        filter_kwargs_for_mall["group_id"] = typecasted_data.get("filter_id")
    elif typecasted_data.get("filters_for") == "tag":
        both_brand_and_category_data = True
        filter_kwargs["tag_ids"] = str(typecasted_data.get("filter_id"))
        filter_kwargs_for_brand_and_cat["tag_ids"] = str(typecasted_data.get("filter_id"))
        filter_kwargs_for_mall["tag_ids"] = str(typecasted_data.get("filter_id"))
    elif typecasted_data.get("filters_for") in ("cl1", "cl2", "cl3", "cl4"):
        only_brand_data = True
        filter_kwargs["cat_level"] = CATEGORY_LEVEL_MAPPING.get(typecasted_data.get("filters_for"))
        filter_kwargs["category_id"] = typecasted_data.get("filter_id")
        filter_kwargs_for_mall["cat_level"] = CATEGORY_LEVEL_MAPPING.get(typecasted_data.get("filters_for"))
        filter_kwargs_for_mall["category_id"] = typecasted_data.get("filter_id")
        filter_kwargs_for_brand_and_cat["cat_level"] = CATEGORY_LEVEL_MAPPING.get(typecasted_data.get("filters_for"))
        filter_kwargs_for_brand_and_cat["category_id"] = typecasted_data.get("filter_id")
        category_ids_input.append(typecasted_data.get("filter_id"))

    brand_ids, category_ids = [], []
    if typecasted_data.get("type") == "retail":
        pipeline = get_listing_pipeline_for_retail(filter_kwargs, sort_query, offset, limit)
        data = list(SHARDED_SEARCH_DB["product_store_sharded"].aggregate(pipeline))
        brand_ids, category_ids = get_brand_and_category_ids_for_retail(filter_kwargs_for_brand_and_cat)
    elif typecasted_data.get("type") == "mall":
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
<<<<<<< HEAD
    brand_ids = list(set([str(product.get('brand_id')) for product in data_to_return if product.get('brand_id')]))
    category_ids = list(set([str(product.get('category_id')) for product in data_to_return if product.get('category_id')]))
    print(brand_ids)
    print(category_ids)
||||||| 0c653b4
    brand_ids = list(set([str(product.get('brand_id')) for product in data_to_return]))
    category_ids = list(set([str(product.get('category_id')) for product in data_to_return]))
    print(brand_ids)
    print(category_ids)
=======

    dict_brand_ids = Counter(brand_ids)
    dict_category_ids = Counter(category_ids)
>>>>>>> 20881d4fec90bad72d702b70b0b31843f2ec0903
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
<<<<<<< HEAD
        # print("Hye : ", category_data)
||||||| 0c653b4
        print("Hye : ", category_data)
=======
>>>>>>> 20881d4fec90bad72d702b70b0b31843f2ec0903
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
        "rows": typecasted_data.get("per_page"),
        "currentPage": typecasted_data.get("page"),
        "numFound": num_found,
        "lastPage": math.ceil(num_found / typecasted_data.get("per_page")),
        "productIds": [int(product.get('product_id')) for product in data_to_return],
        "groupIds": [product.get('group_id') for product in data_to_return],
        "filters": filters_data
    }

    log_payload = {'created_at': datetime.now(), 'headers': {"x_source": x_source}, 'request': request_data,
                   'response': final_result}
    SHARDED_SEARCH_DB['product_listing_log'].insert_one(log_payload)
    return final_result


@app.post(ApiUrlConstants.RETAIL_V1_PRODUCT_SEARCH)
async def retail_product_search(request: Request):
    error_response_dict = ERROR_RESPONSE_DICT_FORMAT
    request_data = await request.json()

    def get_typecasted_data(request_data):
        typecasted_data = dict()
        try:
            typecasted_data["wh_id"] = str(request_data.get("wh_id"))
            typecasted_data["page"] = int(request_data.get('page')) if request_data.get('page') else None
            typecasted_data["per_page"] = int(request_data.get('per_page')) if request_data.get(
                'per_page') else None
            typecasted_data["keyword"] = str(request_data.get("keyword", ""))
        except Exception as error:
            typecasted_data["error_msg"] = f"{error}"
        return typecasted_data

    typecasted_data = get_typecasted_data(request_data)
    if typecasted_data.get("error_msg"):
        error_response_dict["message"] = typecasted_data.get("error_msg")
        return error_response_dict
    if not typecasted_data.get("wh_id") or not typecasted_data.get("keyword"):
        error_response_dict["message"] = "invalid wh_id/keyword"
        return error_response_dict

    limit, offset = CommonUtils.get_limit_offset_with_page_no_and_page_limit(typecasted_data["page"],
                                                                             typecasted_data["per_page"])
    keyword = typecasted_data["keyword"]
    keyword_len = len(typecasted_data["keyword"].split(" "))
    if keyword_len == 1:
        compound_dict = {
            "must": [{"text": {"query": typecasted_data["wh_id"], "path": "wh_id"}}],
            "should": [
                {"autocomplete": {"query": keyword, "path": "name"}},
            ],
            "minimumShouldMatch": 1
        }
    else:
        keyword = SearchUtils.get_filtered_rs_kg_keyword(keyword=keyword)
        compound_dict = {
            "must": [
                {"text": {"query": typecasted_data["wh_id"], "path": "wh_id"}},
                {"text": {"query": keyword, "path": "name"}}
            ]
        }

    pipeline = [
        {
            "$search": {
                "compound": compound_dict
            }
        },
        {
            "$project": {
                "_id": 0,
                "g_id": 1,
                "new_score": SearchUtils.get_score_boosting_dict(key="inv_qty", boost_value=5)
            }
        },
        {
            "$group": {
                "_id": "$g_id",
                "new_score": {"$first": "$new_score"}
            }
        },
        {"$sort": {"new_score": -1}},
        {
            "$facet": {
                "total": [{"$count": "count"}],
                "data": [{"$skip": offset}, {"$limit": limit}],
            }
        },
        {
            "$project": {
                "data": "$data",
                "count": {"$arrayElemAt": ['$total.count', 0]},
            }
        }
    ]
    result = list(SHARDED_SEARCH_DB["group_sellable_inventory"].aggregate(pipeline))[0]
    g_data = [data.get("_id") for data in result.get("data")]
    final_result = {"total": result.get("count"), "data": g_data}

    log_payload = {"created_at": datetime.now(), "request": request_data, "response": final_result}
    SHARDED_SEARCH_DB["retail_search_log_1"].insert_one(log_payload)
    return final_result
