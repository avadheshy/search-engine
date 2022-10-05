from pipelines import get_boosting_stage, get_listing_stage
import sentry_sdk
from constants import PAGE_SIZE
from pipelines import get_boosting_stage, group_autocomplete_stage
from pipelines import get_boosting_stage
from fastapi import FastAPI, Request, Query, HTTPException
from typing import Optional, List
from pymongo import MongoClient
from new_pipeline import get_boosting_stage1



sentry_sdk.init(
    # dsn="https://fa760c10052c4da88ba731dc48c9020e@o1194835.ingest.sentry.io/6768729",

    # # Set traces_sample_rate to 1.0 to capture 100%
    # # of transactions for performance monitoring.
    # # We recommend adjusting this value in production,
    # traces_sample_rate=1.0,
)

app = FastAPI()
CLIENT = MongoClient(
    "mongodb+srv://searchengine-appuser:qJSjAhUkcAlyuAwy@search-service.ynzkd.mongodb.net/?retryWrites=true&w=majority"
)
DB = CLIENT.product_search


def store_search_terms(user_id, search_term, search_results):
    DB["hst_search"].insert_one(
        {
            "user_id": user_id,
            "search_term": search_term,
            "search_results": search_results,
        }
    )
    return True


def store_autocomplete_results(user_id, search_term, search_results):
    DB["hst_autocomplete"].insert_one(
        {
            "user_id": user_id,
            "search_term": search_term,
            "search_results": search_results,
        }
    )
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

        # MongoDB Aggregation Pipeline
        pipe_line = get_boosting_stage(
            keyword, store_id, platform, order_type, skip, limit
        )

        # DB Query
        response = DB["search_products"].aggregate(pipe_line).next()

        # Response Formatting
        response["data"] = (
            [i.get("id") for i in response["data"]] if response["data"] else []
        )
        count = response["total"][0] if response["total"] else {}
        response["total"] = count.get("count") if count else 0
    except Exception as error:
        error_message = "{0}".format(error)

    # ...............REQUEST, RESPONSE, ERROR DB LOG ...................

    DB["search_log_1"].insert_one(
        {"request": request_data, "response": response, "msg": error_message}
    )

    # ...................................................

    return response


@app.post("/v2/search")
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

        # MongoDB Aggregation Pipeline
        pipe_line = group_autocomplete_stage(
            keyword, store_id, platform, order_type, skip, limit
        )

        # DB Query
        response = DB["groups"].aggregate(pipe_line).next()

        # Response Formatting
        response["data"] = (
            [i.get("id") for i in response["data"]] if response["data"] else []
        )
        count = response["total"][0] if response["total"] else {}
        response["total"] = count.get("count") if count else 0
    except Exception as error:
        error_message = "{0}".format(error)

    # ...............REQUEST, RESPONSE, ERROR DB LOG ...................

    DB["search_log_1"].insert_one(
        {"request": request_data, "response": response, "msg": error_message}
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


def get_autocomplete_pipeline(search_term, skip):
    """
    This is autocomplete helper function
    """
    return [
        {
            "$search": {
                "index": "default",
                "autocomplete": {"path": "name", "query": search_term},
            }
        },
        {"$project": {"_id": 0, "name": 1, "product_id": 1}},
    ]


# @app.post('/boost')
# def add_booster(attribute_booster: dict):
#     DB['product_booster'].insert_one(attribute_booster)
#     return True


@app.post("/search")
async def product_search(request: Request):
    """
    Product Search API, This will help to discover the relevant products
    """
    # headers = request.headers
    # store_id = headers.get('storeid')
    request_data = await request.json()
    request_data.update({"request": True})
    # DB['search_log'].insert_one(request_data)
    request_data.pop("_id", None)
    query_params = request_data

    user_id = query_params.get("user_id")
    order_type = query_params.get("type")
    store_id = query_params.get("store_id")  # mall / retail
    keyword = query_params.get("keyword")
    platform = query_params.get("platform")  # pos/app
    skip = int(query_params.get("skip")) if query_params.get("skip") else 0
    limit = int(query_params.get("limit")) if query_params.get("limit") else 10

    # host = headers.get('host')
    # path = host + '/v1/search'
    # skip = (int(page) - 1) * PAGE_SIZE
    # pipe_line1 = get_boosting_stage(keyword, store_id, platform, order_type, skip)
    pipe_line = get_boosting_stage(
        keyword, store_id, platform, order_type, skip, limit)

    # pipe_line.append({"$skip": skip})
    # pipe_line.append({"$limit": limit})
    result = list(DB["search_products"].aggregate(pipe_line))
    ## [{'total_count': [{'count': 81}], 'result': [{'id': '41407'}, {'id': '5423'}, {'id': '23865'}, {'id': '25147'}, {'id': '5067'}, {'id': '7193'}, {'id': '7194'}, {'id': '10426'}, {'id': '11027'}, {'id': '11028'}]}]

    # pipe_line.append({"$count": "count"})
    # result_count = len(list(DB["search_products"].aggregate(pipe_line1)))
    # pipe_line.pop()
    # pipe_line.pop()
    # pipe_line.append({"$count": "count"})

    # total_pages=((len(ans)+PAGE_SIZE)//PAGE_SIZE)
    # result={
    # 'data':{
    #     'header_data':[],
    #     'products':ans,
    # },
    # 'links':{
    #     'first':path + '?page=1',
    #     'last': path + '?page='+str(total_pages),
    #     'prev':None,
    #     'next':None,
    # },
    # 'meta':{
    # 'current_page':page,
    # 'from':1,
    # 'last_page':int(page)-1,
    # 'links':[
    #     {
    #     'url':None,
    #     'label':1,
    #     'active':True,
    #     }
    # ],
    # 'path': path,
    # 'per_page':PAGE_SIZE,
    # 'to':len(ans),
    # 'total':len(ans)
    # }
    # }
    # additional_data = {"discount_label": None, "stock": {"available": True}}
    # for i in result["data"]["products"]:
    #     if i:
    #         i.update(additional_data)
    if result[0] and result[0]["result"]:
        result_count = result[0]["total_count"][0]["count"]
        result = result[0]["result"]
        product_ids = [i.get("id") for i in result] if result else []
        response = {"data": product_ids, "total": result_count}
    else:
        response = {"data": [], "total": 0}
    # DB['search_log'].insert_one(response)
    # response.pop('_id', None)
    return response


@app.get("/autocomplete")
def search_autocomplete(search_term: str, page: str):
    """
    This API helps to auto complete the searched term
    """
    user_id = 1
    skip = (int(page) - 1) * PAGE_SIZE
    pipeline = get_autocomplete_pipeline(search_term, skip)
    products = list(DB["search_products"].aggregate(pipeline))
    # store_autocomplete_results(user_id, search_term, products)
    return products


@app.get("/filter_category")
def filter_product(
    filters_for: str,
    page: str,
    filters_for_id: str,
    storeid: str,
    sort_by: Optional[str] = None,
    categories: list = Query(None),
    brandIds: list = Query(None),
):
    skip = (int(page)-1)*PAGE_SIZE
    pipeline = get_listing_stage(
        filters_for, filters_for_id, storeid, sort_by, categories, brandIds, skip, PAGE_SIZE
    )

    filter_data = (
        [
            {
                "name": "Brands",
                "key": "brand",
                "data": None,
            },
            {"name": "Categories", "key": "category", "data": None},
        ],
    )
    sort_data = [
        {"key": "new", "type": "Latest", "is_active": False},
        {"key": "min_price", "type": "Lowest Price", "is_active": False},
        {"key": "max_price", "type": "Highest Price", "is_active": False},
        {"key": "popular", "type": "Highest Popularity", "is_active": False},
        {"key": "relevance", "type": "Relevance", "is_active": False},
    ]

    print(pipeline)
    ans = list(DB["search_data"].aggregate(pipeline))

    result = {
        "data": ans,
        "links": {
            "first": None,
            "last": None,
            "prev": None,
            "next": None,
        },
        "meta": {
            "current_page": page,
            "from": 1,
            "last_page": int(page) - 1,
            "links": [
                {
                    "url": None,
                    "label": 1,
                    "active": True,
                }
            ],
            "path": "",
            "per_page": PAGE_SIZE,
            "to": len(ans),
            "total": len(ans),
            "filters": filter_data,
            "sorts": sort_data,
        },
    }
    return ans


# new test 

@app.post("/v3/search")
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

        # MongoDB Aggregation Pipeline
        pipe_line = get_boosting_stage1(
            keyword, store_id, platform, order_type, skip, limit
        )

        # DB Query
        response = DB["search_products"].aggregate(pipe_line).next()

        # Response Formatting
        response["data"] = (
            [i.get("id") for i in response["data"]] if response["data"] else []
        )
        count = response["total"][0] if response["total"] else {}
        response["total"] = count.get("count") if count else 0
    except Exception as error:
        error_message = "{0}".format(error)

    # ...............REQUEST, RESPONSE, ERROR DB LOG ...................

    # DB["search_log_1"].insert_one(
    #     {"request": request_data, "response": response, "msg": error_message}
    # )

    # ...................................................

    return response


