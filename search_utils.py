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