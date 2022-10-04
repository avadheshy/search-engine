from constants import STORE_WH_MAP

def group_autocomplete_stage(
    keyword="", store_id="", platform="pos", order_type="retail", skip=0, limit=10
):
    search_terms_len = len(keyword.split(" "))
    SEARCH_PIPE = []
    if search_terms_len == 1:
        SEARCH_PIPE = {'$search': {
                'compound': {
                    'should': [
                        {
                            'autocomplete': {
                                'query': keyword,
                                'path': 'name',
                            },
                        },
                        {
                            'autocomplete': {
                                'query': keyword,
                                'path': 'barcode',
                            },
                        },
                    ],
                },
            }}
    else:
        SEARCH_PIPE = {'$search': {
                            'text': {
                                'query': keyword,
                                'path': 'name',
                            },
                        }}

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
                '$lookup': {
                    'from': 'product_warehouse_stocks', 
                    'localField': 'id', 
                    'foreignField': 'product_id', 
                    'as': 'data', 
                    'pipeline': [
                        {
                            '$match': {
                                'warehouse_id': wh_id
                            }
                        },
                        {"$project": {"warehouse_id": 1, "stock": 1}}
                    ]
                }},
    
            {"$project": {"_id": 0, "id": 1, "stock": {"$first": "$data.stock"}}},
            # {"$match": {"stock": {"$gt": 0}}},
            {"$sort": {"stock": -1}},
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "data": [{"$skip": skip}, {"$limit": limit}],
                }
            },
        ]

    else:

        PIPELINE = [
    {
        '$search': {
            'autocomplete': {
                'query': keyword, 
                'path': 'name'
            }
        }
    }, {
        '$lookup': {
            'from': 'search_products', 
            'localField': 'id', 
            'foreignField': 'group_id', 
            'as': 'product_data', 
            'pipeline': [
                {
                    '$lookup': {
                        'from': 'product_store', 
                        'localField': 'id', 
                        'foreignField': 'product_id', 
                        'as': 'store_data', 
                        'pipeline': [
                            {
                                '$match': {
                                    'store_id': store_id
                                }
                            }
                        ]
                    }
                }
            ]
        }
    }, {
        '$match': {
            'product_data': {
                '$ne': []
            }, 
            'product_data.is_mall': '0', 
            'product_data.sale_app': '1', 
            'product_data.store_data': {
                '$ne': []
            }
        }
    }, {
        '$sort': {
            'product_data.store_data.inv_qty': -1
        }
    }, {
        '$project': {
            'id': 1, 
            '_id': 0, 
            'name': 1
        }
    },
    {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "data": [{"$skip": skip}, {"$limit": limit}],
                }
            }
]

    print(PIPELINE)
    return PIPELINE


def get_boosting_stage(
    keyword="", store_id="", platform="pos", order_type="retail", skip=0, limit=10
):
    search_terms_len = len(keyword.split(" "))
    SEARCH_PIPE = []

    if search_terms_len == 1:
        SEARCH_PIPE = [{'$search': {
                'compound': {
                    'should': [
                        {
                            'autocomplete': {
                                'query': keyword,
                                'path': 'name',
                            },
                        },
                        {
                            'autocomplete': {
                                'query': keyword,
                                'path': 'barcode',
                            },
                        },
                    ],
                },
            }}]
    else:
        keyword = ' '.join(list(filter(lambda x : x not in ['rs', 'Rs', 'RS', 'rS'], keyword.split(" "))))
        SEARCH_PIPE = [{'$search': {
                            'text': {
                                'query': keyword,
                                'path': 'name',
                            },
                        }}]
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
                '$lookup': {
                    'from': 'product_warehouse_stocks', 
                    'localField': 'id', 
                    'foreignField': 'product_id', 
                    'as': 'data', 
                    'pipeline': [
                        {
                            '$match': {
                                'warehouse_id': wh_id
                            }
                        },
                        {"$project": {"warehouse_id": 1, "stock": 1}}
                    ]
                }},
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
            {"$project": {"_id": 0, "id": 1, 'inv_qty': {"$first": "$store.inv_qty"}}},
            {"$sort": {"inv_qty": -1}},
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "data": [{"$skip": skip}, {"$limit": limit}],
                }
            },
        ]
    print(PIPELINE)
    return PIPELINE
