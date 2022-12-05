from mysql import connector
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne, UpdateMany
from settings import POS_SQL_USER, POS_SQL_PASSWORD, POS_SQL_HOST, SHARDED_SEARCH_DB

def sync_all_categories():
    current_time = datetime.now()
    prev_time = current_time - timedelta(days=15)
    connection = connector.connect(
        host=POS_SQL_HOST,
        user=POS_SQL_USER,
        password=POS_SQL_PASSWORD

    )
    cur1 = connection.cursor()
    Query1 = "SELECT * FROM  pos.all_categories WHERE all_categories.updated_at > %s OR all_categories.created_at > %s"
    cur1.execute(Query1, (prev_time, prev_time))
    result = cur1.fetchall()
    # connection.close()
    keys = [
        "id",
        "name",
        "icon",
        "cat_level",
        "cl4_id",
        "cl4_name",
        "cl3_id",
        "cl3_name",
        "cl2_id",
        "cl2_name",
        "cl1_id",
        "cl1_name",
        "created_at",
        "updated_at"
    ]
    data = []
    for res in result:
        d = {}
        for i in range(len(keys)):
            d[keys[i]] = str(res[i]) if res[i] else None
        data.append(d)
    payload = []
    for category in data:
        query = {}
        query['id'] = category.get('id')
        payload.append(UpdateOne(query, {'$set': category}, upsert=True))

    # if payload:
    #     DB["all_categories"].bulk_write(payload)
    return True, "Syncing was successfull."


def brands_sync():
    current_time = datetime.now()
    prev_time = current_time - timedelta(days=15)
    connection = connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD
    )
    cur1 = connection.cursor()
    Query1 = "SELECT * FROM  pos.brands WHERE brands.updated_at > %s OR brands.created_at > %s"
    cur1.execute(Query1, (prev_time, prev_time))
    result = cur1.fetchall()
    # connection.close()
    keys = [
        "id",
        "name",
        "parent_id",
        "logo",
        "created_at",
        "updated_at"
    ]
    data = []
    for res in result:
        d = {}
        for i in range(len(keys)):
            d[keys[i]] = str(res[i]) if res[i] else None
        data.append(d)
    payload = []
    for brand in data:
        query = {}
        query['id'] = brand.get('id')
        payload.append(UpdateOne(query, {'$set': brand}, upsert=True))
    #
    # if payload:
    #     DB["brands"].bulk_write(payload)
    return True, "Syncing was successfull."


def score_sync():
    current_time = datetime.now()
    prev_time = current_time - timedelta(minutes=60)
    connection = connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD
    )
    cur1 = connection.cursor()
    Query1 = "SELECT product_id FROM  pos.order_items WHERE updated_at > %s OR created_at > %s"
    cur1.execute(Query1, (prev_time, prev_time))
    result = cur1.fetchall()
    # connection.close()

    data = []
    for res in result:
        data.append({'product_id': res[0]})
    ##
    data = [
        {
            'product_id': 17
        },
        {
            'product_id': 18
        }
    ]

    payload = []
    for product in data:
        payload.append(UpdateOne({'product_id': product.get('product_id')},
                                 {
                                     "$cond": {
                                         "if": {
                                             "$gt": [
                                                 "$ps",
                                                 0
                                             ]
                                         },
                                         "then": {
                                             "$set": {
                                                 '$inc': {'ps': 1}
                                             }

                                         },
                                         "else": {
                                             "$set": {
                                                 "ps": 1
                                             }
                                         }
                                     }
                                 }
                                 ))
    #
    # if payload:
    #     DB["brands"].bulk_write(payload)
    return True, "Syncing was successfull."
