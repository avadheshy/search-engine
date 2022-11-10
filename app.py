import codecs
import math
from fastapi import FastAPI, Request,
import csv
import sentry_sdk
from constants import ERROR_RESPONSE_DICT_FORMAT, CATEGORY_LEVEL_MAPPING, STORE_WH_MAP
from pipelines import get_search_pipeline, group_autocomplete_stage, listing_pipeline, get_listing_pipeline_for_retail, get_listing_pipeline_for_mall
from settings import SHARDED_SEARCH_DB
from search_utils import SearchUtils


sentry_sdk.init(
    # dsn="https://fa760c10052c4da88ba731dc48c9020e@o1194835.ingest.sentry.io/6768729",

    # # Set traces_sample_rate to 1.0 to capture 100%
    # # of transactions for performance monitoring.
    # # We recommend adjusting this value in production,
    # traces_sample_rate=1.0,
)

app = FastAPI()

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
    page=int(request_data.get("page")) if request_data.get("page") else 1
    per_page = int(request_data.get("per_page")) if request_data.get("per_page") else 15
    skip = (int(page)-1)*15
    limit = per_page
    brands_input=[]
    print(brandIds)
    if filters_for=='brand':
        brands_input.append(filters_for_id)
    if brandIds:
        brands_input.extend(brandIds.split(','))
    category_input=[]
    if filters_for in ('cl1','cl2','cl3','cl4'):
        category_input.append(filters_for_id)
    if categories:
        category_input.extend(categories.split(','))
    response=[]
    if type=='mall':
        print('hello1')
        LISTING_PIPELINE=listing_pipeline_mall(filters_for,filters_for_id,store_id, brandIds,categories,sort_by,skip, limit)
        print(LISTING_PIPELINE)
        print('hello')
        import time
        a = time.time()
        response = DB['search_products'].aggregate(LISTING_PIPELINE).next()
        b = time.time()
        print("mall query time", b-a)
        print(response)
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
    count= count.get("count") if count else 0
    Response['count']=len(Response['productIds'])
    Response['numFound']=int(count)
    Response['rows']=limit
    Response['currentPage']=page
    Response['lastPage']=math.ceil((int(count))/limit)
    Response['filters']={"brands":brand_data,
        "categories": category_data,
        "categories_l2": None
    }
    return Response



@app.get("/v1/product-listing/")
def product_listing(request: Request):
    
    request_data = dict(request.query_params.items())

    def get_typcasted_query_params(request_data):
        typcasted_query_params = dict()
        try:
            typcasted_query_params["store_id"] = request_data.get("store_id")
            typcasted_query_params["page"] = int(request_data.get('page')) if request_data.get('page') else 1
            typcasted_query_params["per_page"] = int(request_data.get('per_page')) if request_data.get(
                'per_page') else 15
            typcasted_query_params["filters_for"] = request_data.get("filters_for")
            typcasted_query_params["filters_for_id"] = int(request_data.get("filters_for_id"))
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
    
    

    data=[]

    if typcasted_query_params.get("type")=='retail':
        pipeline=listing_pipeline_retail(typcasted_query_params)
        data = list(DB["product_store_sharded"].aggregate(pipeline))
    # print(data)
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
        "lastPage": math.ceil(num_found/typcasted_query_params.get("per_page")),
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

@app.get("/v2/product-listing/")
def product_listing__v2(request: Request):
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

    filter_kwargs = dict(
        store_id=typcasted_query_params["store_id"],
        is_mall="0",
        status="1"
    )
    
    warehouse_id = STORE_WH_MAP.get(typcasted_query_params.get("store_id"))
    filter_kwargs_for_mall = dict(
        is_mall="1",
        status="1"
    )

    only_brand_data, only_category_data, both_brand_and_category_data = False, False, False
    if typcasted_query_params.get("brandIds"):
        only_category_data = True
        filter_kwargs["brand_id"] = {"$in": typcasted_query_params.get("brandIds")}
        filter_kwargs_for_mall["brand_id"] = {"$in": typcasted_query_params.get("brandIds")}
    if typcasted_query_params.get("categories"):
        only_brand_data = True
        filter_kwargs["category_id"] = {"$in": typcasted_query_params.get("categories")}
        filter_kwargs_for_mall["category_id"] = {"$in": typcasted_query_params.get("categories")}

    if typcasted_query_params.get("filters_for") == "brand":
        only_category_data = True
        filter_kwargs["brand_id"] = typcasted_query_params.get("filter_id")
        filter_kwargs_for_mall["brand_id"] = typcasted_query_params.get("filter_id")
    elif typcasted_query_params.get("filters_for") == "category":
        only_brand_data = True
        filter_kwargs["category_id"] = typcasted_query_params.get("filter_id")
        filter_kwargs_for_mall["category_id"] = typcasted_query_params.get("filter_id")
    elif typcasted_query_params.get("filters_for") == "group":
        both_brand_and_category_data = True
        filter_kwargs["group_id"] = typcasted_query_params.get("filter_id")
        filter_kwargs_for_mall["group_id"] = typcasted_query_params.get("filter_id")
    elif typcasted_query_params.get("filters_for") == "tag":
        both_brand_and_category_data = True
        filter_kwargs["tag_ids"] = str(typcasted_query_params.get("filter_id"))
        filter_kwargs_for_mall["tag_ids"] = str(typcasted_query_params.get("filter_id"))
    elif typcasted_query_params.get("filters_for") in ("cl1", "cl2", "cl3", "cl4"):
        only_brand_data = True
        filter_kwargs["cat_level"] = CATEGORY_LEVEL_MAPPING.get(typcasted_query_params.get("filters_for"))
        filter_kwargs["category_id"] = typcasted_query_params.get("filter_id")
        filter_kwargs_for_mall["cat_level"] = CATEGORY_LEVEL_MAPPING.get(typcasted_query_params.get("filters_for"))
        filter_kwargs_for_mall["category_id"] = typcasted_query_params.get("filter_id")

    print("filter_kwargs : ", filter_kwargs)
    print("filter_kwargs_for_mall : ", filter_kwargs_for_mall)

<<<<<<< HEAD
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
                "category_id": "$category_id",
                "category_name": "$all_categories_data.name",
                "cat_level": "$all_categories_data.cat_level",
                "category_icon": "$all_categories_data.icon",
                "brand_name": "$brands_data.name",
                "brand_logo": "$brands_data.logo"
            }
        },
        {
            '$facet': {
                'total': [
                    {
                        '$count': 'count'
                    }
                ],
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
                "numFound": {"$arrayElemAt": ['$total.count', 0]},
            }
        }
    ]
    data = list(DB["product_store_sharded"].aggregate(pipeline))
||||||| 48b2eeb
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
                "category_id": "$category_id",
                "category_name": "$all_categories_data.name",
                "cat_level": "$all_categories_data.cat_level",
                "category_icon": "$all_categories_data.icon",
                "brand_name": "$brands_data.name",
                "brand_logo": "$brands_data.logo"
            }
        },
        {
            '$facet': {
                'total': [
                    {
                        '$count': 'count'
                    }
                ],
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
                "numFound": {"$arrayElemAt": ['$total.count', 0]},
            }
        }
    ]
    data = list(SHARDED_SEARCH_DB["product_store_sharded"].aggregate(pipeline))
=======
    if typcasted_query_params.get("type") == "retail":
        pipeline = get_listing_pipeline_for_retail(filter_kwargs, sort_query, offset, limit)
        data = list(SHARDED_SEARCH_DB["product_store_sharded"].aggregate(pipeline))
    elif typcasted_query_params.get("type") == "mall":
        pipeline = get_listing_pipeline_for_mall(warehouse_id, filter_kwargs_for_mall, sort_query, offset, limit)
        data = list(SHARDED_SEARCH_DB["search_products"].aggregate(pipeline))
>>>>>>> e6853f214856ac7574e1df5b525c6e44d2538bf0
    data_to_return = data[0].get("data")
    num_found = data[0].get("numFound") or 0
    brand_ids = list(set([str(product.get('brand_id')) for product in data_to_return]))
    category_ids = list(set([str(product.get('category_id')) for product in data_to_return]))
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
        brand_data = list(DB["brands"].find(brand_filter, brand_projection)) or []
    if only_category_data:
        category_data = list(DB["all_categories"].find(category_filter, category_projection)) or []
    if both_brand_and_category_data:
        brand_data = list(DB["brands"].find(brand_filter, brand_projection)) or []
        category_data = list(DB["all_categories"].find(category_filter, category_projection)) or []

    brand_data_to_return = SearchUtils.make_brand_data(brand_data)
    category_data_to_return = SearchUtils.make_category_data(category_data)

    final_result = {
        "count": len(data_to_return),
        "rows": typcasted_query_params.get("per_page"),
        "currentPage": typcasted_query_params.get("page"),
        "numFound": num_found,
        "lastPage": math.ceil(num_found/typcasted_query_params.get("per_page")),
        "productIds": [int(product.get('product_id')) for product in data_to_return],
        "groupIds": [product.get('group_id') for product in data_to_return],
        "filters": {
            "brands": brand_data_to_return,
            "categories": category_data_to_return
        }
    }
    return final_result


