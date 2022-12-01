<<<<<<< HEAD
from constants import ERROR_RESPONSE_DICT_FORMAT, S3_BRAND_URL, STORE_WH_MAP
||||||| 0c653b4
from constants import STORE_WH_MAP

=======
from constants import STORE_WH_MAP
from settings import SHARDED_SEARCH_DB, ASYNC_SHARDED_SEARCH_DB
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510

GROUP_ADDITIONAL_STAGE = [
<<<<<<< HEAD
    {'$project': {'id': '$product_id', 'inv_qty': 1, '_id': 0, 'group_id': 1}},
    {'$sort': {'inv_qty': -1}},
    {
        '$group': {
            '_id': '$group_id',
            'count': {
                '$sum': 1
            }
||||||| 0c653b4
 {'$project': {'id': '$product_id', 'inv_qty': 1, '_id': 0, 'group_id': 1}},
 {'$sort': {'inv_qty': -1}},
 {
        '$group': {
            '_id': '$group_id', 
            'count': {
                '$sum': 1
            }
=======
    {
        '$project': {
            '_id': 0, 'group_id': 1,
            # 'stock': {"$first": "$data.stock"}
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510
        }
    },
    # {'$sort': {'inv_qty': -1}},
    {
        '$group': {
            '_id': '$group_id',
            # 'count': {
            #     '$sum': "$stock"
            # }
        }
    },
    {
        '$match': {
            '_id': {
                '$ne': None
            }
        }
    },
    {
        '$project': {
<<<<<<< HEAD
            'id': '$_id',
            '_id': 0
||||||| 0c653b4
            'id': '$_id', 
            '_id': 0
=======
            'id': '$_id',
            '_id': 0,
            # "count": 1
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510
        }
    },
    # {
    #     "$sort": {
    #         "count": -1
    #     }
    # }
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
                        {"$match": {"warehouse_id": wh_id, 'stock': {"$gt": 0}}},
                        {"$project": {"warehouse_id": 1, "stock": 1}},
                    ],
                }
            },
<<<<<<< HEAD
            {"$project": {"_id": 0, "id": 1, "stock": {
                "$first": "$data.stock"}, "score": {'$meta': "searchScore"}}},
            {"$sort": {"stock": -1, 'score': -1}},
||||||| 0c653b4
            {"$project": {"_id": 0, "id": 1, "stock": {"$first": "$data.stock"}}},
            {"$sort": {"stock": -1}},
=======
            {"$project": {"_id": 0, "id": 1, "stock": {"$first": "$data.stock"}}},
            # {"$sort": {"stock": -1}},
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510
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
<<<<<<< HEAD
            {"$project": {"_id": 0, "id": 1, "inv_qty": {
                "$first": "$store.inv_qty"}, "score": 1}},
            {"$sort": {"inv_qty": -1, 'score': -1}},
||||||| 0c653b4
            {"$project": {"_id": 0, "id": 1, "inv_qty": {"$first": "$store.inv_qty"}}},
            {"$sort": {"inv_qty": -1}},
=======
            {"$project": {"_id": 0, "id": 1, "inv_qty": {"$first": "$store.inv_qty"}}},
            # {"$sort": {"inv_qty": -1}},
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510
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
<<<<<<< HEAD
            {"$project": {"id": "$product_id", "inv_qty": 1,
                          "_id": 0, "score": {'$meta': "searchScore"}}},
            {"$sort": {"inv_qty": -1, 'score': -1}},
||||||| 0c653b4
            {"$project": {"id": "$product_id", "inv_qty": 1, "_id": 0}},
            {"$sort": {"inv_qty": -1}},
=======
            {"$project": {"id": "$product_id", "inv_qty": 1, "_id": 0}},
            # {"$sort": {"inv_qty": -1}},
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510
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
                        {"$match": {"warehouse_id": wh_id, 'stock': {"$gt": 0}}},
                        {"$project": {"warehouse_id": 1, "stock": 1}},
                    ],
                }
            },
<<<<<<< HEAD
            {"$project": {"_id": 0, "id": 1, "stock": {
                "$first": "$data.stock"}, "score": {'$meta': "searchScore"}}},
            {"$sort": {"stock": -1, 'score': -1}},
||||||| 0c653b4
            {"$project": {"_id": 0, "id": 1, "stock": {"$first": "$data.stock"}}},
            {"$sort": {"stock": -1}},
=======
            {"$project": {"_id": 0, "id": 1, "stock": {"$first": "$data.stock"}}},
            # {"$sort": {"stock": -1}},
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510
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
<<<<<<< HEAD
    PIPELINE = get_search_pipeline(
        keyword, store_id, platform, order_type, skip, limit)
    NEW_GROUP_PIPELINE = PIPELINE[:-3] + \
        GROUP_ADDITIONAL_STAGE + [PIPELINE[-1]]
||||||| 0c653b4
    PIPELINE = get_search_pipeline(keyword, store_id, platform, order_type, skip, limit)
    NEW_GROUP_PIPELINE = PIPELINE[:-3] + GROUP_ADDITIONAL_STAGE + [PIPELINE[-1]]
=======
    PIPELINE = get_search_pipeline(keyword, store_id, platform, order_type, skip, limit)
    NEW_GROUP_PIPELINE = PIPELINE[:-2] + GROUP_ADDITIONAL_STAGE + [PIPELINE[-1]]
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510
    # print(NEW_GROUP_PIPELINE)

    return NEW_GROUP_PIPELINE


def get_listing_pipeline_for_retail(filter_kwargs, sort_query, offset, limit):
    if offset is not None and limit is not None:
        data_array = [
            {
                '$skip': offset
            },
            {
                '$limit': limit
            }
        ]
    else:
        data_array = []
    pipeline = [
        {
            "$match": filter_kwargs
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
                "group_id": "$group_id",
                "brand_id": "$brand_id",
                "category_id": "$category_id",
                "cl2_id": "$cl2_id"
            }
        },
        {
            '$facet': {
                'total': [
                    {
                        '$count': 'count'
                    }
                ],
<<<<<<< HEAD
                'data': [
                    {
                        '$skip': offset
                    },
                    {
                        '$limit': limit
                    }
                ]
||||||| 0c653b4
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
=======
                'data': data_array
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510
            }
        },
        {
            "$project": {
                "data": "$data",
                "numFound": {"$arrayElemAt": ['$total.count', 0]},
            }
        }
    ]
    if sort_query:
        pipeline.insert(-3,{'$sort':sort_query})
    return pipeline


def get_listing_pipeline_for_mall(warehouse_id, filter_kwargs_for_mall, sort_query, offset, limit):
    if offset is not None and limit is not None:
        data_array = [
            {
                '$skip': offset
            },
            {
                '$limit': limit
            }
        ]
    else:
        data_array = []
    pipeline = [
        {'$match': filter_kwargs_for_mall},
        {
            '$group': {
                '_id': '$group_id',
                'id': {
                    '$first': '$id'
                },
                'mrp': {
                    '$first': '$mrp'
                },
                'price': {
                    '$first': '$price'
                },
                'updated_at': {
                    '$first': '$updated_at'
                },
                'brand_id': {
                    '$first': '$brand_id'
                },
                'category_id': {
                    '$first': '$category_id'
                },
                'created_at': {
                    '$first': '$created_at'
                },
                'cat_level': {
                    '$first': '$cat_level'
                },
                'tag_ids': {
                    '$first': '$tag_ids'
                },
                'ps': {
                    '$first': '$ps'
                }
            }
        },
        # {
        #     '$project': {
        #         'group_id': '$_id',
        #         'id': 1,
        #         'mrp': 1,
        #         'price': 1,
        #         'updated_at': 1,
        #         'brand_id': 1,
        #         'category_id': 1,
        #         'created_at': 1,
        #         'cat_level': 1,
        #         'tag_ids': 1,
        #         'ps': 1
        #     }
        # },
        {
            '$lookup': {
                'from': 'product_warehouse_stocks',
                'localField': 'id',
                'foreignField': 'product_id',
                'as': 'data',
                'pipeline': [
                    {
                        '$match': {
                            'warehouse_id': warehouse_id,
                            'stock': {"$gt": 0}
                        }
                    }, {
                        '$project': {
                            "_id": 0,
                            'warehouse_id': 1
                        }
                    }
                ]
            }
        },
        {
            '$match': {
                'data': {
                    '$ne': []
                }
            }
        },
        {
            '$project': {
                "_id": 0,
                'product_id': "$id",
                'price': "$price",
                'created_at': {
                    '$dateFromString': {
                        'dateString': '$created_at'
                    }
                },
                'updated_at': {
                    '$dateFromString': {
                        'dateString': '$updated_at'
                    }
                },
                'group_id': {
                    "$toInt": "$_id"
                },
                "brand_id": "$brand_id",
                "category_id": "$category_id",
                "cl2_id": "$cl2_id"
            }
        },
        {
            '$facet': {
                'total': [
                    {
                        '$count': 'count'
                    }
                ],
<<<<<<< HEAD
                'data': [
                    {
                        '$skip': offset
                    },
                    {
                        '$limit': limit
                    }
                ]
||||||| 0c653b4
                'data': [
                    {
                        '$sort': sort_query
                    },
                    {
                        '$skip': offset
                    },
                    {
                        '$limit': limit
                    }
                ]
=======
                'data': data_array
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510
            }
        },
        {
            "$project": {
                "data": "$data",
                "numFound": {"$arrayElemAt": ['$total.count', 0]},
            }
        }
    ]
<<<<<<< HEAD
    print('hello')
    if sort_query:
        pipeline.insert(-3,{'$sort':sort_query})

||||||| 0c653b4

=======
    if sort_query:
        pipeline.insert(-3, {'$sort': sort_query})
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510
    return pipeline
<<<<<<< HEAD

||||||| 0c653b4
=======


def get_brand_and_category_ids_for_retail(filter_kwargs):
    filter_kwargs.update(inv_qty={"$gt": 0})
    all_data = list(SHARDED_SEARCH_DB["product_store_sharded"].find(filter_kwargs, {"_id": 0, "brand_id": 1, "category_id": 1}))
    brand_ids = list(set([str(data.get('brand_id'))for data in all_data if data.get('brand_id')]))
    category_ids = list(set([str(data.get('category_id'))for data in all_data if data.get('category_id')]))
    return brand_ids, category_ids


def get_brand_and_category_pipeline_for_mall(filter_kwargs, warehouse_id):
    pipeline = [
        {'$match': filter_kwargs},
        {
            '$lookup': {
                'from': 'product_warehouse_stocks',
                'localField': 'id',
                'foreignField': 'product_id',
                'as': 'data',
                'pipeline': [
                    {
                        '$match': {
                            'warehouse_id': warehouse_id,
                            'stock': {"$gt": 0}
                        }
                    }, {
                        '$project': {
                            "_id": 0,
                            'warehouse_id': 1
                        }
                    }
                ]
            }
        },
        {
            '$match': {
                'data': {
                    '$ne': []
                }
            }
        },
        {
            '$project': {
                "_id": 0,
                "brand_id": "$brand_id",
                "category_id": "$category_id"
            }
        }
    ]
    return pipeline


def get_brand_and_category_ids_for_mall(brand_category_data):
    # brand_category_data = SHARDED_SEARCH_DB["search_products"].aggregate(
    #     [
    #         {'$match': filter_kwargs},
    #         {
    #             '$lookup': {
    #                 'from': 'product_warehouse_stocks',
    #                 'localField': 'id',
    #                 'foreignField': 'product_id',
    #                 'as': 'data',
    #                 'pipeline': [
    #                     {
    #                         '$match': {
    #                             'warehouse_id': warehouse_id
    #                         }
    #                     }, {
    #                         '$project': {
    #                             "_id": 0,
    #                             'warehouse_id': 1
    #                         }
    #                     }
    #                 ]
    #             }
    #         },
    #         {
    #             '$match': {
    #                 'data': {
    #                     '$ne': []
    #                 }
    #             }
    #         },
    #         {
    #             '$project': {
    #                 "_id": 0,
    #                 "brand_id": "$brand_id",
    #                 "category_id": "$category_id"
    #             }
    #         }
    #     ]
    # )
    brand_ids = list(set([str(data.get('brand_id')) for data in brand_category_data if data.get('brand_id')]))
    category_ids = list(set([str(data.get('category_id')) for data in brand_category_data if data.get('category_id')]))
    return brand_ids, category_ids
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510
