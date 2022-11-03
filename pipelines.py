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


def listing_pipeline(filters_for, filters_for_id, store_id, type, brand_ids, category_ids, sort_by, skip, limit):
    match_filter = {}
    sort_query = {}
    if type and type == 'mall':
        match_filter['is_mall'] = "1"
    else:
        match_filter['is_mall'] = "0"
    if category_ids:
        match_filter['category_id'] = {'$in': category_ids}
    if brand_ids:
        match_filter['brand_id'] = {'$in': brand_ids}

    if sort_by:
        if sort_by == 'max_price':
            sort_query['price'] = -1
        elif sort_by == 'min_price':
            sort_query['price'] = 1
        elif sort_by == 'new':
            sort_query['updated_at'] = -1
    else:
        sort_query['score'] = {"$meta": "textScore"}

    return [
        {
            '$match': match_filter
        }, {
            '$project': {
                '_id': 0,
                'id': 1,
                'price': {'$toDecimal': "$price"},
                'updated_at': 1,
            }
        },

        {'sort': sort_query},
        {
            '$project': {
                'id': 1,
                '_id': 0
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
    print(PIPELINE)
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
    print(NEW_GROUP_PIPELINE)
    return NEW_GROUP_PIPELINE
