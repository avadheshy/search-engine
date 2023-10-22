from constants import STORE_WH_MAP
from search_utils import SearchUtils
from settings import SHARDED_SEARCH_DB


def listing_pipeline(skip, limit, match_filter):
    return [
        {
            '$match': match_filter
        }, {
            '$project': {
                '_id': 0,
                'group_id': 1
            }
        }, {
            '$group': {
                '_id': '$group_id',
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$project': {
                'id': '$_id',
                '_id': 0
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
                        '$skip': skip
                    }, {
                        '$limit': limit
                    }
                ]
            }
        }
    ]


def get_boosting_stage(keyword="", store_id="", platform="pos", order_type="mall", skip=0, limit=10):
    search_pipe = SearchUtils.get_search_pipeline_for_mall(keyword)
    is_mall = "0"
    if order_type == "mall":
        is_mall = "1"
    match_filter = {"is_mall": is_mall, "status": 1}

    if platform == "app":
        match_filter["sale_app"] = "1"
    else:
        match_filter["sale_pos"] = "1"

    if is_mall == "1":
        wh_id = STORE_WH_MAP.get(store_id)
        pipeline = search_pipe + [
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
            {"$project": {
                '_id': 0,
                'id': 1,
                'stock': {
                    '$first': '$data.stock'
                },
                "new_score": SearchUtils.get_score_boosting_dict(key={'$first': '$data.stock'})
            }},
            {"$sort": {"new_score": -1}},
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "data": [{"$skip": skip}, {"$limit": limit}],
                }
            },
        ]
    else:
        pipeline = search_pipe + [
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
                                        {"$eq": ["$product_id", "$$product_id"]},
                                        {"$eq": ["$store_id", store_id]},
                                    ]
                                }
                            }
                        },
                        {"$project": {"store_id": 1, "_id": 0, "inv_qty": 1}},
                    ],
                    "as": "store",
                }
            },
            {"$match": {"store.store_id": store_id}},
            {
                "$project": {
                    'id': 1,
                    'inv_qty': {"$first": "$store.inv_qty"},
                    '_id': 0,
                    "new_score": SearchUtils.get_score_boosting_dict(key={"$first": "$store.inv_qty"})
                }
            },
            {"$sort": {"new_score": -1}},
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "data": [{"$skip": skip}, {"$limit": limit}],
                }
            },
        ]
    return pipeline


def get_pipeline_from_sharded_collection(keyword="", store_id="", platform="pos", order_type="retail", skip=0,
                                         limit=10):
    search_pipe = SearchUtils.get_search_pipeline_for_retail(store_id, keyword)
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
        pipeline = search_pipe + [
            {"$match": match_filter},
            {"$project": {
                'id': '$product_id',
                'inv_qty': 1,
                '_id': 0,
                "new_score": SearchUtils.get_score_boosting_dict(key="$inv_qty")
            }},
            {"$sort": {"new_score": -1}},
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
        pipeline = search_pipe + [
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
            {"$project": {"_id": 0, "id": 1, "stock": {"$first": "$data.stock"},
                          "new_score": SearchUtils.get_score_boosting_dict(key={"$first": "$data.stock"})
                          }},
            {"$sort": {"new_score": -1}},
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "data": [{"$skip": skip}, {"$limit": limit}],
                }
            },
        ]
    return pipeline


def get_search_pipeline(keyword, store_id, platform, order_type, skip, limit):
    if order_type == 'mall':
        pipe_line = get_boosting_stage(keyword, store_id, platform, order_type, skip, limit)
    else:
        pipe_line = get_pipeline_from_sharded_collection(keyword, store_id, platform, order_type, skip, limit)
    return pipe_line


# def group_autocomplete_stage(keyword="", store_id="", platform="pos", order_type="retail", skip=0, limit=10):
#     PIPELINE = get_search_pipeline(keyword, store_id, platform, order_type, skip, limit)
#     NEW_GROUP_PIPELINE = PIPELINE[:-2] + GROUP_ADDITIONAL_STAGE + [PIPELINE[-1]]
#     # print(NEW_GROUP_PIPELINE)
#     return NEW_GROUP_PIPELINE


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
>>>>>>> 20881d4fec90bad72d702b70b0b31843f2ec0903
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
    if sort_query:
        pipeline[-3]=sort_query
||||||| 0c653b4
=======
    if sort_query:
        pipeline.insert(-3, {'$sort': sort_query})
>>>>>>> 20881d4fec90bad72d702b70b0b31843f2ec0903
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
<<<<<<< HEAD
                "category_id": "$category_id",
                "cl2_id": "$cl2_id"
||||||| 0c653b4
                "category_id": "$category_id"
=======
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
                'data': data_array
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
        pipeline.insert(-3, {'$sort': sort_query})
    return pipeline


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
>>>>>>> 20881d4fec90bad72d702b70b0b31843f2ec0903
            }
        }
    ]
    return pipeline


def get_brand_and_category_ids_for_mall(brand_category_data):
    brand_ids = list(set([str(data.get('brand_id')) for data in brand_category_data if data.get('brand_id')]))
    category_ids = list(set([str(data.get('category_id')) for data in brand_category_data if data.get('category_id')]))
    return brand_ids, category_ids


def group_pipeline_for_mall(keyword="", store_id="", platform="pos", skip=0, limit=10):
    search_pipe = SearchUtils.get_group_pipeline_for_store_with_keyword_length_case_should_for_mall(keyword=keyword)
    match_filter = {"is_mall": "1", "status": 1}
    if platform == "app":
        match_filter["sale_app"] = "1"
    else:
        match_filter["sale_pos"] = "1"

    wh_id = STORE_WH_MAP.get(store_id)
    pipeline = search_pipe + [
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
        {
            '$unwind': '$data'
        },
        {
            '$project': {
                '_id': 0,
                'group_id': 1,
                'stock': '$data.stock',
                "new_score": SearchUtils.get_score_boosting_dict(key="$data.stock")
            }
        }, {
            '$group': {
                '_id': '$group_id',
                'count': {
                    '$sum': '$stock'
                },
                "new_score": {"$first": "$new_score"}
            }
        }, {
            '$match': {
                '_id': {
                    '$ne': None
                },
                'count': {
                    '$gte': 0
                }
            }
        }, {
            '$project': {
                'id': '$_id',
                '_id': 0,
                'count': 1,
                "new_score": 1
            }
        }, {
            '$sort': {
                'new_score': -1
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
                        '$skip': skip
                    }, {
                        '$limit': limit
                    }
                ]
            }
        }
    ]
    # print("mall group pipeline : ", pipeline)
    return pipeline


def group_pipeline_for_retail(keyword="", store_id="", platform="pos", skip=0, limit=10):
    search_pipe = SearchUtils.get_group_pipeline_for_store_with_keyword_length_case_must_with_should_for_retail(
                                                                                                    store_id=store_id,
                                                                                                    keyword=keyword)
    match_filter = {'inv_qty': {'$gte': 0}, "is_mall": "0", "store_id": store_id, "status": "1"}
    if platform == "app":
        match_filter["sale_app"] = "1"
    else:
        match_filter["sale_pos"] = "1"

    pipeline = search_pipe + [
        {"$match": match_filter},
        {
            '$project': {
                '_id': 0,
                'group_id': 1,
                'inv_qty': 1,
                "new_score": SearchUtils.get_score_boosting_dict(key="$inv_qty")
            }
        },
        {
            '$sort': {
                'new_score': -1
            }
        },
        {
            '$group': {
                '_id': '$group_id',
                'count': {
                    '$sum': '$inv_qty'
                },
                "new_score": {"$first": "$new_score"}
            }
        },
        {
            '$match': {
                '_id': {
                    '$ne': None
                },
                'count': {
                    '$gte': 0
                }
            }
        },
        {
            '$project': {
                'id': '$_id',
                '_id': 0,
                'count': 1,
                "new_score": 1
            }
        },
        {
            '$sort': {
                'new_score': -1
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
<<<<<<< HEAD
                        '$skip': offset
                    },
                    {
||||||| 0c653b4
                        '$sort': sort_query
                    },
                    {
                        '$skip': offset
                    },
                    {
=======
                        '$skip': skip
                    }, {
>>>>>>> 20881d4fec90bad72d702b70b0b31843f2ec0903
                        '$limit': limit
                    }
                ]
            }
        }
    ]
<<<<<<< HEAD
    if sort_query:
        pipeline[-3]=sort_query

||||||| 0c653b4

=======
>>>>>>> 20881d4fec90bad72d702b70b0b31843f2ec0903
    return pipeline

