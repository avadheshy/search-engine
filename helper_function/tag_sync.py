import json
from datetime import datetime, timedelta
from mysql import connector
from pymongo import MongoClient, UpdateOne, UpdateMany

CLIENT = MongoClient("mongodb+srv://sharded-search-service:KC2718oU0Jt9Qt7v@search-service.ynzkd.mongodb.net/test")
DB = CLIENT.product_search


def sync_product_tag():
    prev_time = datetime.now() - timedelta(days=16)
    current_time = datetime.now() - timedelta(days=15)
    connection = connector.connect(
        host="127.0.0.1",
        user="search.engine",
        password="bToswAS8@vAMpwMUk",
        port=3310
    )
    cur = connection.cursor()
    Query1 = "SELECT * FROM  pos.product_tag WHERE (product_tag.updated_at > %s OR product_tag.created_at > %s) AND (product_tag.updated_at <= %s OR product_tag.created_at <= %s) "
    cur.execute(Query1, (prev_time, prev_time,current_time,current_time))
    result1 = cur.fetchall()
    Query2 = "SELECT * FROM  pos.product_tag WHERE product_tag.deleted_at> %s AND product_tag.deleted_at<= %s "
    cur.execute(Query2, (prev_time,current_time))
    result2 = cur.fetchall()
    connection.close()
    keys = [
        "id",
        "product_id",
        "tag_id",
        "created_at",
        "updated_at",
        "deleted_at"
    ]
    data = []
    for res in result1:
        d = {}
        for i in range(len(keys)):
            d[keys[i]] = str(res[i])
        data.append(d)

    payload = []
    payload1 = []
    for res in data:
        payload.append(UpdateOne({"id": res.get("product_id")}, {'$push': {'tag_ids': res.get('tag_id')}}))

        payload1.append(
            UpdateMany({"product_id": res.get("product_id")}, {'$push': {'tag_ids': res.get('tag_id')}}))
    if payload:
        DB["search_products"].bulk_write(payload)
    if payload1:
        DB["product_store_sharded"].bulk_write(payload1)

    data = []
    for res in result2:
        d = {}
        for i in range(len(keys)):
            d[keys[i]] = str(res[i])
        data.append(d)

    payload = []
    payload1 = []
    for res in data:
        payload.append(UpdateOne({"id": res.get("product_id")}, {'$pull': {'tag_ids': res.get('tag_id')}}))

        payload1.append(
            UpdateMany({"product_id": res.get("product_id")}, {'$pull': {'tag_ids': res.get('tag_id')}}))
    if payload:
        DB["search_products"].bulk_write(payload)
    if payload1:
        DB["product_store_sharded"].bulk_write(payload1)

    return True, result1


from mysql import connector
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne

CLIENT = MongoClient("mongodb+srv://sharded-search-service:KC2718oU0Jt9Qt7v@search-service.ynzkd.mongodb.net/test")

DB = CLIENT.product_search


def sync_product_store():
    current_time = datetime.now()
    prev_time = current_time - timedelta(minutes=20)
    conn = connector.connect(host='127.0.0.1',
                             database='pos',
                             user='search.engine',
                             password='bToswAS8@vAMpwMUk',
                             port=3310)
    cur1 = conn.cursor()
    Query1 = "SELECT * FROM  pos.product_store WHERE product_store.product_id='172881'"
    cur1.execute(Query1)
    result = cur1.fetchall()

    keys1 = [
        "id",
        "store_id",
        "product_id",
        "price",
        "wholesale_price",
        "wholesale_moq",
        "old_price",
        "fast_sale",
        "status",
        "sale_app",
        "sale_pos",
        "auto_pricing_at",
        "created_at",
        "updated_at",
    ]
    data = []
    for res in result:
        d = {}
        for i in range(len(keys1)):
            d[keys1[i]] = str(res[i]) if res[i] else None
        data.append(d)
    print("lenn :: ", len(data))
    payload = []
    product_ids = []
    for res in data:
        query = {}
        query["product_id"] = str(res["product_id"])
        product_ids.append(str(res["product_id"]))
        query["store_id"] = str(res["store_id"])
        res["product_id"] = str(res["product_id"])
        res["store_id"] = str(res["store_id"])
        res["status"] = str(res["status"])
        res["sale_pos"] = str(res["sale_pos"])
        res["sale_app"] = str(res["sale_app"])
        res["panel"] = 'cron'
    p_data = DB["products"].find({'id': {'$in': product_ids}})
    p_data_map = {}
    all_category_ids = set()
    for i in p_data:
        is_mall = "1" if i.get("is_mall") in [1, "1"] else "0"
        p_data_map[i.get('id')] = i.get('name'), i.get('barcode'), is_mall, i.get('group_id'), i.get('brand_id'), i.get(
            'category_id')
        all_category_ids.add(i.get('category_id'))
    all_category_ids = tuple(all_category_ids)
    category_map = {}
    if all_category_ids:
        category_query = str(f"SELECT id, cat_level FROM  pos.all_categories WHERE all_categories.id IN {all_category_ids}")
        cur1.execute(category_query)
        category_query_result = [dict((cur1.description[i][0], value) for i, value in enumerate(row)) for row in cur1.fetchall()]
        for category in category_query_result:
            category_map[category.get("id")] = str(category.get("cat_level"))
    for resp in data:
        query = {"product_id": resp.get("product_id"), "store_id": resp.get("store_id")}
        try:
            resp['name'], resp['barcode'], resp['is_mall'], resp['group_id'], resp['brand_id'], resp[
                'category_id'] = p_data_map.get(resp.get('product_id'))
        except:
            print("error: not unpacked data :: ", resp.get('product_id'))
        resp["cat_level"] = category_map.get(resp['category_id']) if resp.get('category_id') else None
        payload.append(UpdateOne(query, {"$set": resp}, upsert=True))

    conn.close()

    if payload:
        DB["product_store_sharded"].bulk_write(payload)
    return True, "Syncing was successfull."


    conn = connector.connect(host='127.0.0.1',
                             database='pos',
                             user='search.engine',
                             password='bToswAS8@vAMpwMUk',
                             port=3310)
    cur1 = conn.cursor()
    Query1 = "SELECT id,fulfil_warehouse_id FROM  pos.stores"
    cur1.execute(Query1)
    result = cur1.fetchall()
    store_map = {}
    for store in result:
        store_map[str(store[0])]= str(store[1])
