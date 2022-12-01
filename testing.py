# # TODO don't erase this file
#
# import asyncio
# from bson import ObjectId
# import time
# from settings import ASYNC_SHARDED_SEARCH_DB, loop, SHARDED_SEARCH_DB
#
# dummy_id = ObjectId("636061800000000000000000")
#
#
# async def parallel_db_calls(db):
#     print("async call start")
#     return await asyncio.gather(*[
#         db["search_log_shard_1"].find_one({"_id": {"$gte": dummy_id}}),
#         db["search_log_shard_1"].find_one({"_id": {"$gte": dummy_id}}),
#         db["search_log_shard_1"].find_one({"_id": {"$gte": dummy_id}}),
#         db["search_log_shard_1"].find_one({"_id": {"$gte": dummy_id}}),
#         db["search_log_shard_1"].find_one({"_id": {"$gte": dummy_id}}),
#     ])
#
#
# a = time.time()
# # SHARDED_SEARCH_DB["search_log_shard_1"].find_one({"_id": {"$gte": dummy_id}}),
# # SHARDED_SEARCH_DB["search_log_shard_1"].find_one({"_id": {"$gte": dummy_id}}),
# # SHARDED_SEARCH_DB["search_log_shard_1"].find_one({"_id": {"$gte": dummy_id}}),
# # SHARDED_SEARCH_DB["search_log_shard_1"].find_one({"_id": {"$gte": dummy_id}}),
# # SHARDED_SEARCH_DB["search_log_shard_1"].find_one({"_id": {"$gte": dummy_id}}),
# combined_data = loop.run_until_complete(parallel_db_calls(ASYNC_SHARDED_SEARCH_DB))
# b = time.time()
#
# # transaction_data = combined_data[0]
# # transaction_data1 = combined_data[1]
# # transaction_data2 = combined_data[2]
# # transaction_data3 = combined_data[2]
# # transaction_data4 = combined_data[2]
#
# print((b - a) * 100)
