# import asyncio
#
# from settings import ASYNC_SHARDED_SEARCH_DB
#
#
# async def parallel_db_calls_for_mall_listing_api(pipeline, filter_kwargs_for_brand_and_cat, warehouse_id):
#     brand_category_pipeline = [
#         {'$match': filter_kwargs_for_brand_and_cat},
#         {
#             '$lookup': {
#                 'from': 'product_warehouse_stocks',
#                 'localField': 'id',
#                 'foreignField': 'product_id',
#                 'as': 'data',
#                 'pipeline': [
#                     {
#                         '$match': {
#                             'warehouse_id': warehouse_id
#                         }
#                     }, {
#                         '$project': {
#                             "_id": 0,
#                             'warehouse_id': 1
#                         }
#                     }
#                 ]
#             }
#         },
#         {
#             '$match': {
#                 'data': {
#                     '$ne': []
#                 }
#             }
#         },
#         {
#             '$project': {
#                 "_id": 0,
#                 "brand_id": "$brand_id",
#                 "category_id": "$category_id"
#             }
#         }
#     ]
#     return await asyncio.gather(*[
#         ASYNC_SHARDED_SEARCH_DB["search_products"].aggregate(pipeline).to_list(None),
#         ASYNC_SHARDED_SEARCH_DB["search_products"].aggregate(brand_category_pipeline).to_list(None),
#     ])
