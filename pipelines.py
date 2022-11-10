from constants import ERROR_RESPONSE_DICT_FORMAT, S3_BRAND_URL, STORE_WH_MAP

GROUP_ADDITIONAL_STAGE = [
    {'$project': {'id': '$product_id', 'inv_qty': 1, '_id': 0, 'group_id': 1}},
    {'$sort': {'inv_qty': -1}},
    {
        '$group': {
            '_id': '$group_id',
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$match': {
            '_id': {
                '$ne': None
            }
        }
    }, {
        '$project': {
            'id': '$_id',
            '_id': 0
        }
    }]


def listing_pipeline_mall(filters_for, filters_for_id, store_id, brandIds, categories, sort_by, skip, limit):
    wh_id = STORE_WH_MAP.get(store_id)
    filter_query = {}
    sort_query = {}
    filter_query['is_mall'] = "1"
    filter_query['sale_app'] = "1"
    # filter for category ids
    if categories:

        # categories = list(map(int, categories))
        categories = list(map(int, categories.split(',')))
        filter_query['category_id'] = {'$in': categories}
    # filter for brand ids
    if brandIds:
        brandIds = list(map(int, brandIds.split(',')))
        filter_query['brand_id'] = {'$in': brandIds}
    if filters_for == 'cl1':
        filter_query['cat_level'] = '1'
        filter_query["category_id"] = int(filters_for_id)
    elif filters_for == 'cl2':
        filter_query['cat_level'] = '2'
        filter_query["category_id"] = int(filters_for_id)
    elif filters_for == 'cl3':
        filter_query['cat_level'] = '3'
        filter_query["category_id"] = int(filters_for_id)
    elif filters_for == 'cl4':
        filter_query['cat_level'] = '4'
        filter_query["category_id"] = int(filters_for_id)
    elif filters_for == 'brand':
        filter_query["brand_id"] = int(filters_for_id)
    elif filters_for == 'category':
        filter_query["category_id"] = int(filters_for_id)
    elif filters_for == 'group':
        filter_query['group_id'] = int(filters_for_id)
    elif filters_for == 'tag':
        filter_query['tag_ids'] = filters_for_id
    # sorting based on given name
    if sort_by == 'max_price':
        sort_query['price'] = -1
    elif sort_by == 'min_price':
        sort_query['price'] = 1
    elif sort_by == 'new':
        sort_query['updated_at'] = -1
    elif sort_by == 'relevance':
        sort_query['score'] = {"$meta": "textScore"}
    else:
        sort_query['created_at'] = -1

    pipeline=[
    {'$match':filter_query},
    {
        '$lookup': {
            'from': 'product_warehouse_stocks', 
            'localField': 'id', 
            'foreignField': 'product_id', 
            'as': 'data', 
            'pipeline': [
                {
                    '$match': {
                        'warehouse_id': '3'
                    }
                }, {
                    '$project': {
                        'warehouse_id': 1
                    }
                }
            ]
        }
    }, {
        '$match': {
            'data': {
                '$ne': []
            }
        }
    }, {
        '$project': {
            'id': {
                '$toInt': '$id'
            }, 
            'brand_id': {
                '$toString': '$brand_id'
            }, 
            'category_id': {
                '$toString': '$category_id'
            }, 
            'group_id': 1, 
            'price': 1, 
            'created_at': {
                '$dateFromString': {
                    'dateString': '$created_at'
                }
            }, 
            'updated_at': {
                '$dateFromString': {
                    'dateString': '$updated_at'
                }
            }
        }
    }, {
        '$sort': {
            'price': -1
        }
    }, {
        '$project': {
            '_id': 0, 
            'id': 1, 
            'brand_id': 1, 
            'category_id': 1, 
            'group_id': 1
        }
    }, {
        '$facet': {
            'total': [
                {
                    '$count': 'count'
                }
            ], 
            'data': [
                {
                    '$skip': 0
                }, {
                    '$limit': 15
                }
            ]
        }
    }
    ]
        
    
   
    
    return pipeline


def listing_pipeline_retail(typcasted_query_params):
    error_response_dict = ERROR_RESPONSE_DICT_FORMAT
    sort_by = typcasted_query_params.get("sort_by")
    offset = 0
    limit = 15
    if typcasted_query_params["page"] and typcasted_query_params["per_page"]:
        offset = (typcasted_query_params["page"] - 1) * \
            typcasted_query_params["per_page"]
        limit = typcasted_query_params["per_page"]
    if typcasted_query_params.get("error_msg"):
        error_response_dict["message"] = typcasted_query_params.get(
            "error_msg")
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
        filter_kwargs["brand_id"] = {
            "$in": typcasted_query_params.get("brandIds")}
    if typcasted_query_params.get("categories"):
        filter_kwargs["category_id"] = {
            "$in": typcasted_query_params.get("categories")}

    if typcasted_query_params.get("filters_for") == "brand":
        filter_kwargs["brand_id"] = typcasted_query_params.get(
            "filters_for_id")
    elif typcasted_query_params.get("filters_for") == "category":
        filter_kwargs["category_id"] = str(
            typcasted_query_params.get("filters_for_id"))
    elif typcasted_query_params.get("filters_for") == "group":
        filter_kwargs["group_id"] = typcasted_query_params.get(
            "filters_for_id")
    elif typcasted_query_params.get("filters_for") == "tag":
        final_filter["tag_ids"] = str(
            typcasted_query_params.get("filters_for_id"))

    elif typcasted_query_params.get("filters_for") == "cl1":
        final_filter["cat_level"] = "1"
    elif typcasted_query_params.get("filters_for") == "cl2":
        final_filter["cat_level"] = "2"
    elif typcasted_query_params.get("filters_for") == "cl3":
        final_filter["cat_level"] = "3"
    elif typcasted_query_params.get("filters_for") == "cl4":
        final_filter["cat_level"] = "4"

    if typcasted_query_params.get("filters_for") in ("cl1", "cl2", "cl3", "cl4"):
        filter_kwargs["category_id"] = typcasted_query_params.get(
            "filters_for_id")

    return [
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
                "brand_data": [
                    {
                        "$group": {
                            "_id": None,
                            "count": {'$sum': 1},
                            "brands_data": {"$push": {
                                "id": "$str_brand_id",
                                "name": "$brand_name",
                                "logo": {"$concat": [S3_BRAND_URL, "$str_brand_id", "/", "$brand_logo"]}
                            }}
                        }
                    }
                ],
                "category_data": [
                    {
                        "$group": {
                            "_id": None,
                            "count": {'$sum': 1},
                            "categories_data": {"$push": {
                                "id": "$category_id",
                                "name": "$category_name",
                                "icon": {"$concat": ["Have_to_add_category_url", "$str_brand_id", "/", "$brand_logo"]}
                            }}
                        }
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
                "brand_data": {"$arrayElemAt": ['$brand_data', 0]},
                "category_data": {"$arrayElemAt": ['$category_data', 0]},
                "numFound": {"$arrayElemAt": ['$total.count', 0]},
            }
        }
    ]


def get_boosting_stage(
    keyword="", store_id="", platform="pos", order_type="retail", skip=0, limit=10
):
    search_terms_len = len(keyword.split(" "))
    SEARCH_PIPE = []

    if search_terms_len == 1:
        SEARCH_PIPE = [
            {
                "$search": {
                    "compound": {
                        "should": [
                            {
                                "autocomplete": {
                                    "query": keyword,
                                    "path": "name",
                                },
                            },
                            {
                                "autocomplete": {
                                    "query": keyword,
                                    "path": "barcode",
                                },
                            },
                            {
                                "exists": {
                                    "path": "winter_sale",
                                    "score": {"constant": {"value": 3}}
                                },
                            }
                        ],
                    },
                }
            }
        ]
    else:
        keyword = " ".join(
            list(
                filter(lambda x: x not in [
                       "rs", "Rs", "RS", "rS", 'kg', 'ml', 'gm'], keyword.split(" "))
            )
        )
        SEARCH_PIPE = [
            {
                "$search": {
                    'compound': {
                        'must': [{
                            "text": {
                                "query": keyword,
                                "path": "name",
                            }}
                        ],
                        'should': [{
                            "exists": {
                                "path": "winter_sale",
                                "score": {"constant": {"value": 3}}
                            },
                        }]
                    }
                }
            }

        ]
    is_mall = "0"
    if order_type == "mall":
        is_mall = "1"
    match_filter = {"is_mall": is_mall}

    if platform == "app":
        match_filter["sale_app"] = "1"
    else:
        match_filter["sale_pos"] = "1"

    if is_mall == "1":
        wh_id = STORE_WH_MAP.get(store_id)
        PIPELINE = SEARCH_PIPE + [
            {"$match": match_filter},
            {
                "$lookup": {
                    "from": "product_warehouse_stocks",
                    "localField": "id",
                    "foreignField": "product_id",
                    "as": "data",
                    "pipeline": [
                        {"$match": {"warehouse_id": wh_id}},
                        {"$project": {"warehouse_id": 1, "stock": 1}},
                    ],
                }
            },
            {"$project": {"_id": 0, "id": 1, "stock": {
                "$first": "$data.stock"}, "score": {'$meta': "searchScore"}}},
            {"$sort": {"stock": -1, 'score': -1}},
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "data": [{"$skip": skip}, {"$limit": limit}],
                }
            },
        ]

    else:

        PIPELINE = SEARCH_PIPE + [
            {"$match": match_filter},
            {
                "$lookup": {
                    "from": "product_store",
                    "let": {"product_id": "$id"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$product_id",
                                                 "$$product_id"]},
                                        {"$eq": ["$store_id", store_id]},
                                    ]
                                }
                            }
                        },
                        {"$project": {"store_id": 1, "_id": 0, "inv_qty": 1,
                                      "score": {'$meta': "searchScore"}}},
                    ],
                    "as": "store",
                }
            },
            {"$match": {"store.store_id": store_id}},
            {"$project": {"_id": 0, "id": 1, "inv_qty": {
                "$first": "$store.inv_qty"}, "score": 1}},
            {"$sort": {"inv_qty": -1, 'score': -1}},
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "data": [{"$skip": skip}, {"$limit": limit}],
                }
            },
        ]
    return PIPELINE


def get_pipeline_from_sharded_collection(
    keyword="", store_id="", platform="pos", order_type="retail", skip=0, limit=10
):
    search_terms_len = len(keyword.split(" "))
    SEARCH_PIPE = []

    if search_terms_len == 1:
        SEARCH_PIPE = [
            {
                "$search": {
                    "compound": {
                        "must": [{"text": {"query": store_id, "path": "store_id"}}],
                        "should": [
                            {"autocomplete": {"query": keyword, "path": "name"}},
                            {"autocomplete": {"query": keyword, "path": "barcode"}},
                            {
                                "exists": {
                                    "path": "winter_sale",
                                    "score": {"constant": {"value": 3}}
                                }
                            },
                        ],
                        "minimumShouldMatch": 1,
                    }
                }
            }
        ]
    else:
        keyword = " ".join(
            list(
                filter(
                    lambda x: x not in ["rs", "Rs",
                                        "RS", "rS", "gm", "ml", "kg"],
                    keyword.split(" "),
                )
            )
        )
        SEARCH_PIPE = [
            {
                "$search": {
                    "compound": {
                        "must": [
                            {"text": {"query": store_id, "path": "store_id"}},
                            {"text": {"query": keyword, "path": "name"}},
                        ],
                        'should': [{
                            "exists": {
                                "path": "winter_sale",
                                "score": {"constant": {"value": 3}}
                            },
                        }]
                    }
                }
            }
        ]
    match_filter = {}
    is_mall = "0"
    if order_type == "mall":
        is_mall = "1"
    match_filter["is_mall"] = is_mall

    if platform == "app":
        match_filter["sale_app"] = "1"
    else:
        match_filter["sale_pos"] = "1"
    if is_mall == "0":
        match_filter["store_id"] = store_id
        match_filter["status"] = "1"
        PIPELINE = SEARCH_PIPE + [
            {"$match": match_filter},
            {"$project": {"id": "$product_id", "inv_qty": 1,
                          "_id": 0, "score": {'$meta': "searchScore"}}},
            {"$sort": {"inv_qty": -1, 'score': -1}},
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "data": [{"$skip": skip}, {"$limit": limit}],
                }
            },
        ]
    else:
        match_filter["status"] = "1"
        wh_id = STORE_WH_MAP.get(store_id)
        PIPELINE = SEARCH_PIPE + [
            {"$match": match_filter},
            {
                "$lookup": {
                    "from": "product_warehouse_stocks",
                    "localField": "id",
                    "foreignField": "product_id",
                    "as": "data",
                    "pipeline": [
                        {"$match": {"warehouse_id": wh_id}},
                        {"$project": {"warehouse_id": 1, "stock": 1}},
                    ],
                }
            },
            {"$project": {"_id": 0, "id": 1, "stock": {
                "$first": "$data.stock"}, "score": {'$meta': "searchScore"}}},
            {"$sort": {"stock": -1, 'score': -1}},
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "data": [{"$skip": skip}, {"$limit": limit}],
                }
            },
        ]
    return PIPELINE


def get_search_pipeline(keyword, store_id, platform, order_type, skip, limit):
    pipe_line = []
    if order_type == 'mall':
        pipe_line = get_boosting_stage(
            keyword, store_id, platform, order_type, skip, limit)
    else:
        pipe_line = get_pipeline_from_sharded_collection(
            keyword, store_id, platform, order_type, skip, limit)

    return pipe_line


def group_autocomplete_stage(
    keyword="", store_id="", platform="pos", order_type="retail", skip=0, limit=10
):
    PIPELINE = get_search_pipeline(
        keyword, store_id, platform, order_type, skip, limit)
    NEW_GROUP_PIPELINE = PIPELINE[:-3] + \
        GROUP_ADDITIONAL_STAGE + [PIPELINE[-1]]
    # print(NEW_GROUP_PIPELINE)
    return NEW_GROUP_PIPELINE
