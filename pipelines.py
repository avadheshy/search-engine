from constants import STORE_WH_MAP

def get_boosting_stage(
    keyword="", store_id="", platform="pos", order_type="retail", skip=0, limit=10
):
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
        PIPELINE = [
                {'$search': {
                'compound': {
                    'should': [
                        {
                            'autocomplete': {
                                'query': keyword,
                                'path': 'name',
                                'fuzzy': {
                                    'maxEdits': 1,
                                    'prefixLength': 0
                                },
                                "score": { "boost": { "value": 3}}
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
            }},
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
            {"$match": {"stock": {"$gt": 0}}},
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
                {'$search': {
                'compound': {
                    'should': [
                        {
                            'autocomplete': {
                                'query': keyword,
                                'path': 'name',
                                'fuzzy': {
                                    'maxEdits': 1,
                                    'prefixLength': 0
                                    },
                                "score": { "boost": { "value": 3}}
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
            }},
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
            {"$match": {"inv_qty": {"$gt": 0}}},
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
