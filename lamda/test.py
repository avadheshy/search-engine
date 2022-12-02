# from mysql import connector
# import  copy
# from datetime import datetime, timedelta
# from pymongo import MongoClient, UpdateOne
#
#
#
# def sync_product_store():
#     current_time = datetime.now()
#     prev_time = current_time - timedelta(hours=12)

#     cur1 = conn.cursor()
#     Query1 = "SELECT * FROM  pos.product_store WHERE product_store.updated_at >= %s OR product_store.created_at >= %s"
#     cur1.execute(Query1, (prev_time, prev_time,))
#     result = cur1.fetchall()
#
#     keys1 = [
#         "id",
#         "store_id",
#         "product_id",
#         "price",
#         "wholesale_price",
#         "wholesale_moq",
#         "old_price",
#         "fast_sale",
#         "status",
#         "sale_app",
#         "sale_pos",
#         "auto_pricing_at",
#         "created_at",
#         "updated_at",
#     ]
#     data = []
#     for res in result:
#         d = {}
#         for i in range(len(keys1)):
#             d[keys1[i]] = str(res[i]) if res[i] else None
#         data.append(d)
#     payload = []
#     product_ids = []
#     for res in data:
#         query = {}
#         query["product_id"] = str(res["product_id"])
#         product_ids.append(str(res["product_id"]))
#         query["store_id"] = str(res["store_id"])
#         res["product_id"] = str(res["product_id"])
#         res["store_id"] = str(res["store_id"])
#         res["status"] = str(res["status"])
#         res["sale_pos"] = str(res["sale_pos"])
#         res["sale_app"] = str(res["sale_app"])
#         res["panel"] = 'cron'
#     p_data = DB["products"].find({'id': {'$in': product_ids}})
#     p_data_map = {}
#     all_category_ids = set()
#     for i in p_data:
#         is_mall = "1" if i.get("is_mall") in [1, "1"] else "0"
#         p_data_map[i.get('id')] = i.get('name'), i.get('barcode'), is_mall, i.get('group_id'), i.get('brand_id'), i.get(
#             'category_id')
#         all_category_ids.add(i.get('category_id'))
#     all_category_ids = tuple(all_category_ids)
#     category_map = {}
#     if all_category_ids:
#         category_query = str(f"SELECT id, cat_level FROM  pos.all_categories WHERE all_categories.id IN {all_category_ids}")
#         cur1.execute(category_query)
#         category_query_result = [dict((cur1.description[i][0], value) for i, value in enumerate(row)) for row in cur1.fetchall()]
#         for category in category_query_result:
#             category_map[category.get("id")] = str(category.get("cat_level"))
#     for resp in data:
#         query = {"product_id": resp.get("product_id"), "store_id": resp.get("store_id")}
#         resp['name'], resp['barcode'], resp['is_mall'], resp['group_id'], resp['brand_id'], resp[
#             'category_id'] = p_data_map.get(resp.get('product_id'))
#         resp["cat_level"] = category_map.get(resp['category_id']) if resp.get('category_id') else None
#         payload.append(UpdateOne(query, {"$set": resp}, upsert=True))
#
#     conn.close()
#
#     if payload:
#         print(payload)
#         DB["product_store_sharded"].bulk_write(payload)
#     return True, "Syncing was successfull."
#
#
# def lambda_handler(event, context):
#     print("function started!")
#     result, message = sync_product_store()
#     print("function ends!")
#     return {"status": True}
import copy

from mysql import connector
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne
from  settings import  USER,HOST,PASSWORD,SHARDED_SEARCH_DB


# def get_sounds_like(name):
#     items = name.split(' ')
#     sounds_like = []
#     for j in items:
#         a, b = doublemetaphone(j)
#         if a:
#             sounds_like.append(a)
#         if b:
#             sounds_like.append(b)
#     sounds_like = ' '.join(set(sounds_like))
#     return sounds_like


def sync_product_data():

    current_time = datetime.now()
    prev_time = current_time - timedelta(hours=15)

    f = "%Y-%m-%d %H:%M:%S"
    connection = connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD
    )
    cur = connection.cursor()
    Query1 = "SELECT * FROM  pos.products WHERE products.created_at >= %s OR products.updated_at >= %s"
    # Query1 = "SELECT * FROM  pos.products WHERE products.id = 136055"

    cur.execute(Query1, (prev_time, prev_time,))
    result = cur.fetchall()
    keys1 = [
        "id",
        "chain_id",
        "name",
        "image_url",
        "group_id",
        "brand_id",
        "marketer_id",
        "category_id",
        "tax_group_id",
        "description",
        "uom",
        "mrp",
        "old_mrp",
        "price",
        "wholesale_price",
        "wholesale_moq",
        "old_price",
        "hsn_sac_code",
        "sku",
        "barcode",
        "color",
        'localization',
        'org_name',
        "status",
        "sale_app",
        "sale_pos",
        "is_mall",
        "is_returnable",
        "primary_return_window",
        "is_secondary_returnable",
        "secondary_return_window",
        "meta_description",
        "created_at",
        "updated_at",
        "last_price_change"
    ]
    data = []
    for res in result:
        d = {}
        for i in range(len(keys1)):
            if (keys1[i] == "mrp" or keys1[i] == "old_mrp" or keys1[i] == "price" or keys1[i] == "wholesale_price" or
                    keys1[i] == "old_price"):
                d[keys1[i]] = float(res[i]) if res[i] else None
            elif (keys1[i] == "created_at" or keys1[i] == "updated_at" or keys1[i] == "last_price_change"):
                d[keys1[i]] = res[i].strftime(f) if res[i] else None
            else:
                d[keys1[i]] = res[i] if res[i] else None
        data.append(d)
    all_category_ids = set()
    for i in data:
        all_category_ids.add(i.get('category_id'))
    all_category_ids=tuple(all_category_ids)
    category_map = {}
    if all_category_ids:
        category_query = str(
            f"SELECT id, cat_level FROM  pos.all_categories WHERE all_categories.id IN {all_category_ids}")
        cur.execute(category_query)
        category_query_result = [dict((cur.description[i][0], value) for i, value in enumerate(row)) for row in
                                 cur.fetchall()]
        for category in category_query_result:
            category_map[category.get("id")] = str(category.get("cat_level"))

    payload = []
    payload1 = []
    for res in data:
        query = {}
        query["id"] = str(res["id"])
        res["id"] = str(res["id"])
        res["is_mall"] = str(res["is_mall"]) if res.get("is_mall") else "0"
        res["sale_app"] = str(res["sale_app"])
        res["sale_pos"] = str(res["sale_pos"])
        payload.append(UpdateOne(query, {"$set": res}, upsert=True))
        new_res = copy.deepcopy(res)
        new_res['cat_level'] = category_map.get(res.get('category_id'))
        payload1.append(UpdateOne(query, {"$set": new_res}, upsert=True))

    # if payload:
    #     DB["products"].bulk_write(payload)
    # if payload1:
    #     DB['search_products'].bulk_write(payload1)

    connection.close()
    return True, "Syncing was successfull."


def lambda_handler(event, context):
    print("function started!")

    result, message = sync_product_data()
    print("function ends!")
    return {"status": True}
