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
    def make_category_data(cls, category_data):
        category_data_to_return = None
        if category_data:
            category_data_to_return = []
            for data in category_data:
                category_data_to_return.append(dict(
                    id=data.get('id'),
                    name=data.get('name'),
                    logo=f"category_url/{data.get('id')}/{data.get('logo')}",
                    icon=f"category_url/{data.get('id')}/{data.get('logo')}",
                    type="category"
                ))
        return category_data_to_return

    @classmethod
    def make_brand_data(cls, brand_data):
        brand_data_to_return = None
        if brand_data:
            brand_data_to_return = []
            for data in brand_data:
                brand_data_to_return.append(dict(
                    id=data.get('id'),
                    name=data.get('name'),
                    logo=f"{S3_BRAND_URL}{data.get('id')}/{data.get('logo')}",
                    icon=f"{S3_BRAND_URL}{data.get('id')}/{data.get('logo')}",
                    type="brand"
                ))
        return brand_data_to_return


class SearchUtils:
    
