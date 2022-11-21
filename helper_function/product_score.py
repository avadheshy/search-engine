for i in range(1, 180000, 10000):
    ids = list(map(str, range(i, i + 10000)))

    pipeline = [
        {'$match': {'product_id': {'$in': ids}}},
        {
                '$project': {
                    'product_id': 1,
                    'score': 1,
                    '_id': 0
                }
            }
        ]
    data = list(DB['products_score'].aggregate(pipeline))
    payload = []
    for product in data:
        payload.append(UpdateOne({'id': product.get('product_id')}, {'$set': {'ps': int(product.get('score'))}}))
    if payload:
        DB['search_products'].bulk_write(payload)
    print(i)
    print(len(payload))


# 'response'[
#     #count, total number of products
#     #rows,
#     #currentPage, page (bydefault 1)
#     numFound,
#     #lastPage , count/pagesize(bydefault 15)
#     # productIds,product ids
#     # groupIds , groupids
#     filters
# ]
data = list(DB['products'].aggregate([
    {
        '$match': {}
    }, {
        '$project': {
            '_id': 0,
            'id': 1,
            'brand_id': 1,
            'category_id': 1
        }
    }
]))
payload = []
for i in data:
    dict_data = {}
    dict_data['brand_id'] = int(i.get('brand_id'))
    dict_data['category_id'] = int(i.get('category_id'))
    payload.append(UpdateOne({'id': i.get(id)}, {'$set': dict_data}))
DB['search_products'].bulk_write(payload)


