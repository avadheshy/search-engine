# from pymongo import MongoClient
# CLIENT = MongoClient(
#     'mongodb+srv://searchengine-appuser:qJSjAhUkcAlyuAwy@search-service.ynzkd.mongodb.net/?retryWrites=true&w=majority')
# DB = CLIENT.search
# ans=0
# for i in range(1,200000,10000):
#     ids = list(map(str, list(range(i, i+10000))))
#     aggregate_pipe = [
#    {'$match' : {'id' : {'$in' : ids}}},
   
#     {
#         '$lookup': {
#             'from': 'all_categories', 
#             'localField': 'parent_id', 
#             'foreignField': 'id', 
#             'as': 'cat_data'
#         }
#     }, {
#         '$unwind': {
#             'path': '$cat_data'
#         }
#     }, {
#         '$project': {
#             'id': 1, 
#             'name': 1, 
#             'image_url': 1, 
#             'group_id': 1, 
#             'uom': 1, 
#             'mrp': 1, 
#             'price': 1, 
#             'hsn_sac_code': 1, 
#             'barcode': 1, 
#             'status': 1, 
#             'sale_app': 1, 
#             'sale_pos': 1, 
#             'is_mall': 1, 
#             'brand': 1, 
#             'group_name': 1, 
#             'category': 1, 
#             'images': 1, 
#             'cat_level': {
#                 'cl1_id': '$cat_data.cl1_id', 
#                 'cl1_name': '$cat_data.cl1_name', 
#                 'cl2_id': '$cat_data.cl2_id', 
#                 'cl2_name': '$cat_data.cl2_name', 
#                 'cl3_id': '$cat_data.cl3_id', 
#                 'cl3_name': '$cat_data.cl3_name', 
#                 'cl4_id': '$cat_data.cl4_id', 
#                 'cl4_name': '$cat_data.cl4_name'
#             }
#         }
#     }, 
#     ]
#     data=list(DB['data'].aggregate(aggregate_pipe))
#     DB['search_products'].insert_many(data)
#     a=len(data)
#     ans+=a
#     print(a,ans)
import unittest
import requests
import mysql.connector

# url='https://uat-discovery.1knetworks.com/docs#/default/product_search_v1_search_post'
# parms={"type":"retail","store_id":"64","keyword":"parle","platform":"pos","skip":"0","limit":"10"}
# resp=requests.post(url,parms)
# print(resp)
# print(resp.status_code)  
# class SimpleTest(unittest.TestCase):
  
#     def test(self):        
#         self.assertTrue(True)
  
# if __name__ == '__main__':
#     unittest.main()
import mysql.connector

try:
    connection = mysql.connector.connect(host='localhost',
                                         database='Electronics',
                                         user='pynative',
                                         password='pynative@#29')

    mySql_Create_Table_Query = """CREATE TABLE Laptop ( 
                             Id int(11) NOT NULL,
                             Name varchar(250) NOT NULL,
                             Price float NOT NULL,
                             Purchase_date Date NOT NULL,
                             PRIMARY KEY (Id)) """

    cursor = connection.cursor()
    result = cursor.execute(mySql_Create_Table_Query)
    print("Laptop Table created successfully ")

except mysql.connector.Error as error:
    print("Failed to create table in MySQL: {}".format(error))
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
   
