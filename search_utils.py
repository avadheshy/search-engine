from constants import S3_BRAND_URL, S3_CATEGORY_URL


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
        return array_with_count

    @classmethod
    def make_category_data(cls, category_data, dict_category_id, category_ids_input):
        category_data_to_return = None
        if category_data:
            category_data_to_return = []
            for data in category_data:
                logo_icon = f"{S3_CATEGORY_URL}{data.get('id')}/{data.get('icon')}" if data.get('icon') else None
                category_data_to_return.append(dict(
                    id=data.get('id'),
                    name=data.get('name'),
                    active=True if int(data.get('id')) in category_ids_input else False,
                    logo=logo_icon,
                    icon=logo_icon,
                    count=dict_category_id.get(data.get('id')) 
                ))
        return category_data_to_return

    @classmethod
    def make_brand_data(cls, brand_data, dict_brand_id, brand_ids_input):
        brand_data_to_return = None
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
