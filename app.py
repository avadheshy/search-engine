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
    is_group = request_data.get("is_group")
    if is_group == '0':
        pipe_line = get_search_pipeline(keyword, store_id, platform, order_type, skip, limit)
        if order_type == 'mall':
            response = SHARDED_SEARCH_DB["search_products"].aggregate(pipe_line).next()
        else:
            response = SHARDED_SEARCH_DB["product_store_sharded"].aggregate(pipe_line).next()
    else:
        pipe_line = group_autocomplete_stage(
            keyword, store_id, platform, order_type, skip, limit
        )
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


@app.get("/Dummy-code")
def product_listing(request: Request):
    error_response_dict = ERROR_RESPONSE_DICT_FORMAT
    request_data = dict(request.query_params.items())

    def get_typcasted_query_params(request_data):
        typcasted_query_params = dict()
        try:
            typcasted_query_params["store_id"] = request_data.get("store_id")
            typcasted_query_params["page"] = int(request_data.get('page')) if request_data.get('page') else 1
            typcasted_query_params["per_page"] = int(request_data.get('per_page')) if request_data.get(
                'per_page') else 15
            typcasted_query_params["filters_for"] = request_data.get("filters_for")
            typcasted_query_params["filter_id"] = int(request_data.get("filter_id"))
            typcasted_query_params["sort_by"] = request_data.get("sort_by")
            typcasted_query_params["type"] = request_data.get("type")
            if request_data.get("brandIds"):
                typcasted_query_params["brandIds"] = str(request_data.get("brandIds")).split(",")
                typcasted_query_params["brandIds"] = list(map(int, typcasted_query_params["brandIds"])) or []
            if request_data.get("categories"):
                typcasted_query_params["categories"] = str(request_data.get("categories")).split(",")
                typcasted_query_params["categories"] = list(map(int, typcasted_query_params["categories"])) or []
        except Exception as error:
            typcasted_query_params["error_msg"] = f"{error}"
        return typcasted_query_params

    typcasted_query_params = get_typcasted_query_params(request_data)
    sort_by = typcasted_query_params.get("sort_by")
    offset = 0
    limit = 15
    if typcasted_query_params["page"] and typcasted_query_params["per_page"]:
        offset = (typcasted_query_params["page"] - 1) * typcasted_query_params["per_page"]
        limit = typcasted_query_params["per_page"]
    if typcasted_query_params.get("error_msg"):
        error_response_dict["message"] = typcasted_query_params.get("error_msg")
        return error_response_dict
    if typcasted_query_params.get("type") not in ["mall", "retail"]:
        error_response_dict["message"] = "Invalid type"
        return error_response_dict
    if typcasted_query_params.get("filters_for") not in ["brand", "category", "tag", "group", "cl1",
                                                         "cl2", "cl3", "cl4"]:
        error_response_dict["message"] = "Invalid filters_for"
        return error_response_dict
    if typcasted_query_params.get("sort_by") not in ["new", "min_price", "max_price", "popular", "relevance",
                                                     "product_created_at"]:
        error_response_dict["message"] = "Invalid sort_by"
        return error_response_dict

    # sorting based on given name
    sort_query = {}
    if sort_by == 'new':
        sort_query['updated_at'] = -1
    elif sort_by == 'min_price':
        sort_query['price'] = 1
    elif sort_by == 'max_price':
        sort_query['price'] = -1
    elif sort_by == 'relevance':
        sort_query['score'] = -1
    elif sort_by == 'product_created_at':
        sort_query['created_at'] = -1

    filter_kwargs = dict()
    filter_kwargs["store_id"] = typcasted_query_params["store_id"]
    final_filter = {}

    if typcasted_query_params.get("brandIds"):
        filter_kwargs["brand_id"] = {"$in": typcasted_query_params.get("brandIds")}
    if typcasted_query_params.get("categories"):
        filter_kwargs["category_id"] = {"$in": typcasted_query_params.get("categories")}

    if typcasted_query_params.get("filters_for") == "brand":
        filter_kwargs["brand_id"] = typcasted_query_params.get("filter_id")
    elif typcasted_query_params.get("filters_for") == "category":
        filter_kwargs["category_id"] = str(typcasted_query_params.get("filter_id"))
    elif typcasted_query_params.get("filters_for") == "group":
        filter_kwargs["group_id"] = typcasted_query_params.get("filter_id")
    elif typcasted_query_params.get("filters_for") == "tag":
        final_filter["tag_ids"] = str(typcasted_query_params.get("filter_id"))

    elif typcasted_query_params.get("filters_for") == "cl1":
        final_filter["cat_level"] = "1"
    elif typcasted_query_params.get("filters_for") == "cl2":
        final_filter["cat_level"] = "2"
    elif typcasted_query_params.get("filters_for") == "cl3":
        final_filter["cat_level"] = "3"
    elif typcasted_query_params.get("filters_for") == "cl4":
        final_filter["cat_level"] = "4"

    if typcasted_query_params.get("filters_for") in ("cl1", "cl2", "cl3", "cl4"):
        filter_kwargs["category_id"] = typcasted_query_params.get("filter_id")

    pipeline = [
        {
            "$match": filter_kwargs
        },
        {
            "$project": {
                "_id": 0,
                "store_id": 1,
                "category_id": {"$toString": "$category_id"},
                "brand_id": 1,
                "group_id": 1,
                "product_id": 1,
                "created_at": 1,
                "updated_at": 1,
                "price": 1,
                "str_brand_id": {"$toString": "$brand_id"}
            }
        },
        {
            "$lookup": {
                "from": "brands",
                "let": {"brandID": {"$ifNull": ["$str_brand_id", None]}},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$eq": ["$id", "$$brandID"]
                            },
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "id": 1,
                            "name": 1,
                            "logo": 1
                        }
                    }
                ],
                "as": "brands_data"
            }
        },
        {
            "$unwind": {
                "path": "$brands_data",
                "preserveNullAndEmptyArrays": False
            }
        },
        {
            "$lookup": {
                "from": "product_tag",
                "let": {"productID": "$product_id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$eq": ["$product_id", "$$productID"]
                            }
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "id": 1,
                            "tag_id": 1,
                            "product_id": {"$toString": "$product_id"},
                        }
                    }
                ],
                "as": "product_tag_data"
            },
        },
        {
            "$lookup": {
                "from": "all_categories",
                "let": {"categoryID": {"$ifNull": ["$category_id", None]}},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$eq": ["$id", "$$categoryID"]
                            },
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "id": 1,
                            "name": 1,
                            "cat_level": 1,
                            "icon": 1
                        }
                    }
                ],
                "as": "all_categories_data"
            }
        },
        {
            "$unwind": {
                "path": "$all_categories_data",
                "preserveNullAndEmptyArrays": False
            }
        },
        {
            "$project": {
                "_id": 0,
                "product_id": "$product_id",
                "price": {"$toDouble": "$price"},
                "created_at": {
                    "$dateFromString": {
                        "dateString": '$created_at',
                    }
                },
                "updated_at": {
                    "$dateFromString": {
                        "dateString": '$updated_at',
                    }
                },
                # "score": {"$meta": "textScore"},
                "group_id": "$group_id",
                "brand_id": "$brand_id",
                "str_brand_id": "$str_brand_id",
                "category_id_in_pss": "$category_id",
                "tag_ids": "$product_tag_data.tag_id",
                "category_id": "$all_categories_data.id",
                "category_name": "$all_categories_data.name",
                "cat_level": "$all_categories_data.cat_level",
                "category_icon": "$all_categories_data.icon",
                # "brands_data": "$brands_data",
                "brand_name": "$brands_data.name",
                "brand_logo": "$brands_data.logo"
            }
        },
        {
            "$match": final_filter
        },
        {
            '$facet': {
                'total': [
                    {
                        '$count': 'count'
                    }
                ],
                # "brand_data": [
                #     {
                #         "$group": {
                #             "_id": None,
                #             "count": {'$sum': 1},
                #             "brands_data": {"$push": {
                #                 "id": "$str_brand_id",
                #                 "name": "$brand_name",
                #                 "logo": {"$concat": [S3_BRAND_URL, "$str_brand_id", "/", "$brand_logo"]}
                #             }}
                #         }
                #     }
                # ],
                # "category_data": [
                #     {
                #         "$group": {
                #             "_id": None,
                #             "count": {'$sum': 1},
                #             "categories_data": {"$push": {
                #                 "id": "$category_id",
                #                 "name": "$category_name",
                #                 "icon": {"$concat": ["Have_to_add_category_url", "$str_brand_id", "/", "$brand_logo"]}
                #             }}
                #         }
                #     }
                # ],
                'data': [
                    {
                        "$sort": sort_query
                    },
                    {
                        '$skip': offset
                    },
                    {
                        '$limit': limit
                    }
                ]
            }
        },
        {
            "$project": {
                "data": "$data",
                "brand_data": {"$arrayElemAt": ['$brand_data', 0]},
                "category_data": {"$arrayElemAt": ['$category_data', 0]},
                "numFound": {"$arrayElemAt": ['$total.count', 0]},
            }
        }
    ]
    # print(pipeline)
    data = list(SHARDED_SEARCH_DB["product_store_sharded"].aggregate(pipeline))
    data_to_return = data[0].get("data")
    brand_data = data[0].get("brand_data", {}).get("brands_data", {}) or {}
    category_data = data[0].get("category_data", {}).get("categories_data", {}) or {}
    num_found = data[0].get("numFound") or 0
    brands_array_with_count = SearchUtils.remove_duplicates_and_add_count_of_each_item(brand_data)
    categories_array_with_count = SearchUtils.remove_duplicates_and_add_count_of_each_item(category_data)
    final_result = {
        "count": len(data_to_return),
        "rows": typcasted_query_params.get("per_page"),
        "currentPage": typcasted_query_params.get("page"),
        "numFound": num_found,
        "lastPage": math.ceil(num_found / typcasted_query_params.get("per_page")),
        "productIds": [int(product.get('product_id')) for product in data_to_return],
        "groupIds": [product.get('group_id') for product in data_to_return],
        # "data": data_to_return,
        # "brand_data": brand_data,
        # "category_data": category_data,
        "filters": {
            "brands": brands_array_with_count,
            "categories": categories_array_with_count
        }
    }
    return final_result

    # request_response = {
    #     "request": {
    #         "store_id": "1",
    #         "page": "1",
    #         "filters_for": "brand/category/collection/tag/group/cl1/cl2/cl3/cl4",
    #         "filter_id": "123",
    #         "sort_by": "new/min_price/max_price/popular/relevance/product_created_at",
    #         "type": "mall/retail",
    #         "brandIds": "1234",
    #         "categories": "456",
    #         "per_page": "15default"
    #     },
    #     "response": ["count   len(returned productIds)",
    #                  "rows - per_page frontend",
    #                  "currentPage - frontend page",
    #                  # "numFound -  if type=mall ",
    #                  "lastPage - count divided by rows",
    #                  "productIds [integers]",
    #                  "groupIds [integers]",
    #                  "filters "
    #                  ]
    # }
    #
    # return request_response


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
    # if sort_by:
    if sort_by == 'new':
        sort_query['created_at'] = 1
    elif sort_by == 'min_price':
        sort_query['price'] = 1
    elif sort_by == 'max_price':
        sort_query['price'] = -1
    elif sort_by == 'relevance':
        sort_query['updated_at'] = 1
    elif sort_by == 'popular':
        sort_query['ps'] = -1
    else:
        sort_query['updated_at'] = 1

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
        print(pipeline)
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
