import unittest
from constants import STORE_WH_MAP
from pymongo import MongoClient
CLIENT = MongoClient("mongodb+srv://sharded-search-service:KC2718oU0Jt9Qt7v@search-service.ynzkd.mongodb.net/test")
DB = CLIENT.product_search

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

    is_mall = "1"
    match_filter = {"is_mall": is_mall}

    if platform == "app":
        match_filter["sale_app"] = "1"
    else:
        match_filter["sale_pos"] = "1"


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
        {"$project": {"_id": 0, "name": 1, "stock": {"$first": "$data.stock"}}},
        {"$sort": {"stock": -1}},
        {"$project": {"_id": 0, "name": 1}}
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
    match_filter["is_mall"] = is_mall

    if platform == "app":
        match_filter["sale_app"] = "1"
    else:
        match_filter["sale_pos"] = "1"
    
    match_filter["store_id"] = store_id
    match_filter["status"] = "1"
    PIPELINE = SEARCH_PIPE + [
        {"$match": match_filter},
        {"$project": {"id": "$product_id", "inv_qty": 1, "_id": 0,"name": 1}},
        {"$sort": {"inv_qty": -1}},
        {"$project": {"_id": 0, "name": 1}}
        
        
    
    ]
    

    return PIPELINE

def get_search_pipeline(keyword, store_id, platform, order_type, skip, limit):
    pipe_line = []
    if order_type == 'mall':
        pipe_line = get_boosting_stage(keyword, store_id, platform, order_type, skip, limit)
    else:
        pipe_line = get_pipeline_from_sharded_collection(keyword, store_id, platform, order_type, skip, limit)
    return pipe_line

def product_search(order_type,store_id,keyword,platform,skip,limit):
    response = []
    skip = int(skip) if skip else 0
    limit = int(limit) if limit else 10
        # DB Query
    pipe_line = get_search_pipeline(keyword, store_id, platform, order_type, skip, limit)
    if order_type == 'mall':
        response = list(DB["search_products"].aggregate(pipe_line))
    else:
        response = list(DB["product_store_sharded"].aggregate(pipe_line))



    return response

class TestSum(unittest.TestCase):
    def test_search_api(self):
        """
        Test that it contains the relevent search products
        """
        result=True
        response=product_search('mall','168','belt','app','0','10')

        if len(response)==0:
            result=False
        else:
            for product in response:
                
                name=product.get('name').split()
                result1=False
                for p_name in ['belt','Belt','belts','Belts']:
                    if p_name in name:
                        result1=True
        result=result1
        
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()
