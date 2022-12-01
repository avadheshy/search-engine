<<<<<<< HEAD
from constants import S3_BRAND_URL
||||||| 0c653b4
from constants import S3_BRAND_URL


=======
from constants import S3_BRAND_URL, S3_CATEGORY_URL


>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510
class SearchUtils:
    @classmethod
    def remove_duplicates_and_add_count_of_each_item(cls, all_data):
        count_map = {}
        for data in all_data:
            if not count_map.get(data.get("id")):
                count_map[data.get("id")] = 1
            else:
                count_map[data.get("id")] += 1
        removed_duplicates_map = {}
        array_with_count = []
        for key, value in count_map.items():
            for b_data in all_data:
                if b_data.get("id") == key:
                    if not removed_duplicates_map.get(key):
                        removed_duplicates_map[key] = value
                        b_data["count"] = value
                        array_with_count.append(b_data)
        print(array_with_count)
        return array_with_count

    @classmethod
<<<<<<< HEAD
    def make_category_data(cls, category_data,dict_category_id,category_ids_input):
||||||| 0c653b4
    def make_category_data(cls, category_data):
=======
    def make_category_data(cls, category_data, dict_category_id, category_ids_input):
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510
        category_data_to_return = None
        if category_data:
            category_data_to_return = []
            for data in category_data:
<<<<<<< HEAD
                logo_icon = f"category_url/{data.get('id')}/{data.get('icon')}" if data.get('icon') else None
||||||| 0c653b4
                logo_icon = f"category_url/{data.get('id')}/{data.get('logo')}" if data.get('logo') else None
=======
                logo_icon = f"{S3_CATEGORY_URL}{data.get('id')}/{data.get('icon')}" if data.get('icon') else None
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510
                category_data_to_return.append(dict(
                    id=data.get('id'),
                    name=data.get('name'),
                    active=True if int(data.get('id')) in category_ids_input else False,
                    logo=logo_icon,
                    icon=logo_icon,
<<<<<<< HEAD
                    count=dict_category_id.get(data.get('id'))
                    
||||||| 0c653b4
                    type="category"
=======
                    count=dict_category_id.get(data.get('id')) 
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510
                ))
        return category_data_to_return

    @classmethod
<<<<<<< HEAD
    def make_brand_data(cls, brand_data,dict_brand_id,brand_ids_input):
||||||| 0c653b4
    def make_brand_data(cls, brand_data):
=======
    def make_brand_data(cls, brand_data, dict_brand_id, brand_ids_input):
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510
        brand_data_to_return = None
        print(brand_ids_input)
        if brand_data:
            brand_data_to_return = []
            for data in brand_data:
                logo_icon = f"{S3_BRAND_URL}{data.get('id')}/{data.get('logo')}"
                brand_data_to_return.append(dict(
                    id=data.get('id'),
                    name=data.get('name'),
                    active=True if int(data.get('id')) in brand_ids_input else False,
                    logo=logo_icon,
                    icon=logo_icon,
                    count=dict_brand_id.get(data.get('id')),
                    type="brand",
                    filter_key= "brandIds[]",
                ))
        return brand_data_to_return



    
