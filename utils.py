
import csv

data_map = {
	"Uniq Id": "product_id",
	"Title": "name",
	"Manufacturer": "manufacturer",
	"Sku": "sku_code"
}

from pymongo import MongoClient

CLIENT = MongoClient('mongodb+srv://avadheshy2022:1997Avdy@cluster0.a2ic8ii.mongodb.net/test')
DB = CLIENT.SearchEngine

def insert_file_data_products(collection_name, file_path):

	payload = []
	with open(file_path, mode ='r') as file:
		csvFile = csv.DictReader(file)
		for lines in csvFile:
			data = {
						"product_id": lines.get("Uniq Id"),
						"name": lines.get("Title"),
						"manufacturer": lines.get("Manufacturer"),
						"sku_code": lines.get("Sku")

			}
			payload.append(data)
	DB[collection_name].insert_many(payload)
	return True