def get_boosting_stage(
    keyword="", store_id="", platform="pos", order_type="retail", skip=0, limit=10
):
    is_mall = "0"
    if order_type == "mall":
        is_mall = "1"

    PIPELINE = [
        {"$search": {"autocomplete": {"query": keyword, "path": "name"}}},
        {"$match": {"is_mall": is_mall}},
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
                                    {"$eq": ["$store_id", store_id]},
                                ]
                            }
                        }
                    },
                    {"$project": {"store_id": 1, "_id": 0}},
                ],
                "as": "store",
            }
        },
        {"$match": {"store.store_id": store_id}},
        {"$project": {"_id": 0, "id": 1}},
        {
            "$facet": {
                "total": [{"$count": "count"}],
                "data": [{"$skip": skip}, {"$limit": limit}],
            }
        },
    ]
    return PIPELINE
