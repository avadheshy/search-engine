from fastapi import FastAPI, Request, Query
from typing import Optional, List
from pymongo import MongoClient
from pipelines import get_boosting_stage
from constants import PAGE_SIZE


app = FastAPI()
CLIENT = MongoClient(
    "mongodb+srv://searchengine-appuser:qJSjAhUkcAlyuAwy@search-service.ynzkd.mongodb.net/?retryWrites=true&w=majority"
)
# DB = CLIENT.search_engine
DB = CLIENT.search


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
    request_data = await request.json()

    # ............... REQUEST DB LOG ...................
    DB['search_log'].insert_one(request_data)
    request_data.pop('_id', None)
    # ..................................................

    user_id = request_data.get("user_id")
    order_type = request_data.get("type")
    store_id = request_data.get("store_id")  # mall / retail
    keyword = request_data.get("keyword")
    platform = request_data.get("platform")  # pos / app
    skip = int(request_data.get("skip")) if request_data.get("skip") else 0
    limit = int(request_data.get("limit")) if request_data.get("limit") else 10

    pipe_line = get_boosting_stage(keyword, store_id, platform, order_type, skip, limit)
    response = DB["search_products"].aggregate(pipe_line).next()

    response["data"] = (
        [i.get("id") for i in response["data"]] if response["data"] else []
    )
    count = response["total"][0] if response["total"] else {}
    response["total"] = count.get("count") if count else 0

    # ............... RESPONSE DB LOG ...................
    DB['search_log'].insert_one(response)
    response.pop('_id', None)
    # ...................................................

    return response


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
    pipe_line = get_boosting_stage(keyword, store_id, platform, order_type, skip, limit)

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
    pipeline = get_autocomplete_pipeline(search_term, skip, PAGE_SIZE)
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
    filter_query = {}
    if filters_for == "brand":
        filter_query["brand.id"] = filters_for_id
    elif filters_for == "cl1":
        filter_query["category_level.cl1_id"] = filters_for_id
    elif filters_for == "cl2":
        filter_query["category_level.cl2_id"] = filters_for_id
    elif filters_for == "cl3":
        filter_query["category_level.cl3_id"] = filters_for_id
    elif filters_for == "cl4":
        filter_query["category_level.cl4_id"] = filters_for_id

    aggregation_pipeline = [
        {"$match": filter_query},
        {"$project": {"_id": 0, "category_level": 0}},
    ]
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
    for i in range(len(sort_data)):
        if sort_data[i]["key"] == sort_by:
            sort_data[i]["is_active"] = True
    sort_query = {}
    category_ids = []
    brand_ids = []

    if categories:
        for category in categories:
            category_ids.append(category)
        aggregation_pipeline.append({"$match": {"category.id": {"$in": category_ids}}})
    if brandIds:
        for brandId in brandIds:
            brand_ids.append(brandId)
        aggregation_pipeline.append({"$match": {"brand.id": {"$in": brand_ids}}})
    aggregation_pipeline.append(
        {
            "$lookup": {
                "from": "store",
                "let": {"product_id": "$id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$product_id", "$$product_id"]},
                                    {"$eq": ["$store_id", storeid]},
                                ]
                            }
                        }
                    },
                    {"$project": {"store_id": 1, "_id": 0}},
                ],
                "as": "store",
            }
        },
        {"$skip": skip},
        {"$limit": PAGE_SIZE},
    )
    if sort_by:
        if sort_by == "min_price":
            sort_query["price"] = 1
        elif sort_by == "max_price":
            sort_query["price"] = -1
        aggregation_pipeline.append({"$sort": sort_query})

    aggregation_pipeline.append({"$limit": PAGE_SIZE})
    print(aggregation_pipeline)
    ans = list(DB["my_data"].aggregate(aggregation_pipeline))

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
    return result
