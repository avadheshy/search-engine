# import pandas as pd
#
# from bson import ObjectId
# from pymongo import MongoClient
#
# CLIENT = MongoClient("mongodb+srv://sharded-search-service:KC2718oU0Jt9Qt7v@search-service.ynzkd.mongodb.net/test")
# SHARDED_SEARCH_DB = CLIENT.product_search
#
# # start_dt = datetime.now() - timedelta(days=16)
# # dt_dummy_id = ObjectId.from_datetime(start_dt)
# dummy_id = ObjectId("636061800000000000000000")
#
# # print(dt_dummy_id)
# # print(start_dt)
# print(dummy_id)
#
# data = list(SHARDED_SEARCH_DB["search_log_shard_1"].find({"_id": {"$gte": dummy_id}}))
#
# print(len(data))
# new_list = []
# for da in data:
#     da_dict = {
#         "search_keyword": da.get("request", {}).get("keyword"),
#         "user_id": da.get("request", {}).get("user_id"),
#         "store_id": da.get("request", {}).get("store_id"),
#         "result_count": da.get("response", {}).get("total"),
#         "date_time": str(da.get("_id").generation_time)
#     }
#     new_list.append(da_dict)
#     # break
#
# # print(new_list)
# print("start writing")
#
# filename = "search_log_shard_1_data_from_1st_nov"
# df = pd.DataFrame(list(new_list))
# df.to_csv(f"{filename}.csv", index=False)
# print("Done")
