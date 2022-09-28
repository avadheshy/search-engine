from curses import keyname
from unicodedata import name


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


    PIPELINE = [
        {"$search": {"autocomplete": {"query": keyword, "path": "name"}}},
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


def get_listing_stage(
    filters_for,filters_for_id,storeid,sort_by,categories,brandIds,skip,PAGE_SIZE
    ):
    #applying filter for a given brand-id or category-level-id
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
   
    sort_query = {}
    category_ids = []
    brand_ids = []

    # applying filter for a given category-id
    if categories:
        for category in categories:
            category_ids.append(category)
        aggregation_pipeline.append({"$match": {"category.id": {"$in": category_ids}}})
    
    # applying filter for a given brand-id
    if brandIds:
        for brandId in brandIds:
            brand_ids.append(brandId)
        aggregation_pipeline.append({"$match": {"brand.id": {"$in": brand_ids}}})
    
    # matching data with store table for a given store-id
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
        
        
    )
    
    # applying sorting on the basis of a given condition 
    if sort_by:
        if sort_by == "min_price":
            sort_query["price"] = 1
        elif sort_by == "max_price":
            sort_query["price"] = -1
        elif sort_by=='new':
            sort_query['updated_at']=-1
        elif sort_by=='relevance':
            sort_query['score']={ '$meta': "textScore" }
        aggregation_pipeline.append({"$sort": sort_query})
    aggregation_pipeline.append({"$skip": skip})
    aggregation_pipeline.append({"$limit": PAGE_SIZE})
    return aggregation_pipeline


# {
#     "$search": {
#         "compound": {
#             "must": [
#                 {
#                     "autocomplete": {
#                             "query": 'keyword',
#                             "path": 'name',
#                             "fuzzy": {"maxEdits": 1, "maxExpansions": 10},
#                     }
#                 }
#                     ],
#             "should": [
#                 {
#                     "phrase": {
#                         "query": keyword,
#                         "path": 'name',
#                         "slop": 2,
#                     }
#                 }
#                     ],
#                 },
            
#             }
#         }