from constants import STORE_WH_MAP


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
                        ],
                    },
                }
            }
        ]
    else:
        keyword = " ".join(
            list(
                filter(lambda x: x not in ["rs", "Rs", "RS", "rS", 'kg', 'ml', 'gm'], keyword.split(" "))
            )
        )
        SEARCH_PIPE = [
            {
                "$search": {
                    "text": {
                        "query": keyword,
                        "path": "name",
                    },
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
            {"$project": {"_id": 0, "id": 1, "stock": {"$first": "$data.stock"}}},
            {"$sort": {"stock": -1}},
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
            {"$project": {"_id": 0, "id": 1, "inv_qty": {"$first": "$store.inv_qty"}}},
            {"$sort": {"inv_qty": -1}},
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
                    lambda x: x not in ["rs", "Rs", "RS", "rS", "gm", "ml", "kg"],
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
                        ]
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
            {"$project": {"id": "$product_id", "inv_qty": 1, "_id": 0}},
            {"$sort": {"inv_qty": -1}},
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
            {"$project": {"_id": 0, "id": 1, "stock": {"$first": "$data.stock"}}},
            {"$sort": {"stock": -1}},
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
        pipe_line = get_boosting_stage(keyword, store_id, platform, order_type, skip, limit)
    else:
        pipe_line = get_pipeline_from_sharded_collection(keyword, store_id, platform, order_type, skip, limit)
    return pipe_line


def group_autocomplete_stage(
    keyword="", store_id="", platform="pos", order_type="retail", skip=0, limit=10
):
    PIPELINE = get_search_pipeline(keyword, store_id, platform, order_type, skip, limit)
    NEW_GROUP_PIPELINE = PIPELINE[:-3] + GROUP_ADDITIONAL_STAGE + [PIPELINE[-1]]
    # print(NEW_GROUP_PIPELINE)
    return NEW_GROUP_PIPELINE


def get_listing_pipeline_for_retail(filter_kwargs, sort_query, offset, limit):
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
                # "score": {"$meta": "textScore"},
                "group_id": "$group_id",
                "brand_id": "$brand_id",
                "category_id": "$category_id"
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
    return pipeline

def get_listing_pipeline_for_mall(filter_kwargs_for_mall, sort_query, offset, limit):
    pipeline = []
    return pipeline
