import unittest
from constants import STORE_WH_MAP
from pymongo import  UpdateOne, UpdateMany
from app import DB
def fun():
    data=list(DB['products'].aggregate([{'$project':{'id':1,'_id':0,'name':1,'barcode':1,'status':1,'sale_app':1,'sale_pos':1}}]))
    payload=[]
    for product in data:
        my_data={}
        my_data['name']=product.get('name')
        my_data['barcode']=product.get('barcode')
        my_data['status']=product.get('status')
        my_data['sale_app']=product.get('sale_app')
        my_data['sale_pos']=product.get('sale_pos')
        payload.append(UpdateOne({'id':product.get('id')}, {"$set": my_data}))
    if payload:
        DB["product_warehouse_stocks"].bulk_write(payload)   


# class TestSum(unittest.TestCase):
#     def test_search_api(self):
#         """
#         Test that it contains the relevent search products
#         """
#         result=True
#         response=

#         if len(response)==0:
#             result=False
#         else:
#             for product in response:
                
#                 name=product.get('name').split()
#                 result1=False
#                 for p_name in ['belt','Belt','belts','Belts']:
#                     if p_name in name:
#                         result1=True
#         result=result1
        
#         self.assertTrue(result)


# if __name__ == '__main__':
#     unittest.main()
