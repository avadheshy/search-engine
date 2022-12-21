from constants import S3_BRAND_URL, S3_CATEGORY_URL, PRODUCT_BOOST_CONSTANT_VAL, CURRENCY_AND_MEASUREMENTS_KEYWORDS
from settings import IS_PRODUCT_BOOSTING_ON


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
                    filter_key="brandIds[]",
                ))
        return brand_data_to_return

    @classmethod
    def get_filtered_rs_kg_keyword(cls, keyword=""):
        if len(keyword) > 1:
            if keyword[1] == " ":
                keyword = keyword[2:]
        keyword = " ".join(
            list(
                filter(lambda x: x not in CURRENCY_AND_MEASUREMENTS_KEYWORDS, keyword.split(" "))
            )
        )
        return keyword

    @classmethod
    def get_group_pipeline_for_store_with_keyword_length_case_should_for_mall(cls, keyword=""):
        search_terms_len = len(keyword.split(" "))
        if search_terms_len == 1:
            search_pipe = [
                {
                    "$search": {
                        "compound": {
                            "should": [
                                {
                                    "text": {
                                        "query": keyword,
                                        "path": "name",
                                    },
                                },
                                {
                                    "text": {
                                        "query": keyword,
                                        "path": "barcode",
                                    },
                                },
                            ]
                        },
                    }
                }
            ]
        else:
            keyword = cls.get_filtered_rs_kg_keyword(keyword=keyword)
            search_pipe = [
                {
                    "$search": {
                        "compound": {
                            "must": [
                                {
                                    "text": {
                                        "query": keyword,
                                        "path": "name"
                                    }
                                }
                            ]
                        }

                    }
                }
            ]

        return search_pipe

    @classmethod
    def get_group_pipeline_for_store_with_keyword_length_case_must_with_should_for_retail(cls, store_id, keyword=""):
        search_terms_len = len(keyword.split(" "))
        if search_terms_len == 1:
            search_pipe = [
                {
                    "$search": {
                        "compound": {
                            "must": [{"text": {"query": store_id, "path": "store_id"}}],
                            "should": [
                                {"text": {"query": keyword, "path": "name"}},
                                {"text": {"query": keyword, "path": "barcode"}},
                            ],
                            "minimumShouldMatch": 1,
                        }
                    }
                }
            ]
        else:
            keyword = cls.get_filtered_rs_kg_keyword(keyword=keyword)
            search_pipe = [
                {
                    "$search": {
                        "compound": {
                            "must": [
                                {"text": {"query": store_id, "path": "store_id"}},
                                {"text": {"query": keyword, "path": "name"}},
                            ]
                        }
                    }
                }
            ]

        return search_pipe

    @classmethod
    def get_search_pipeline_for_retail(cls, store_id, keyword=""):
        search_terms_len = len(keyword.split(" "))

        if search_terms_len == 1:
            search_pipe = [
                {
                    "$search": {
                        "compound": {
                            "must": [{"text": {"query": store_id, "path": "store_id"}}],
                            "should": [
                                {"autocomplete": {"query": keyword, "path": "name"}},
                                {"autocomplete": {"query": keyword, "path": "barcode"}},
                            ],
                            "minimumShouldMatch": 1,
                        }
                    }
                }
            ]
            if IS_PRODUCT_BOOSTING_ON:
                search_pipe[0]['$search']['compound']['should'] += [
                    {"text": {"query": '1', "path": "is_boosted",
                              "score": {"constant": {"value": PRODUCT_BOOST_CONSTANT_VAL}}}}
                ]
        else:
            keyword = " ".join(
                list(
                    filter(
                        lambda x: x not in CURRENCY_AND_MEASUREMENTS_KEYWORDS, keyword.split(" "),
                    )
                )
            )
            search_pipe = [
                {
                    "$search": {
                        "compound": {
                            "must": [
                                {"text": {"query": store_id, "path": "store_id"}},
                                {"text": {"query": keyword, "path": "name"}},
                            ]
                        }
                    }
                }
            ]
            if IS_PRODUCT_BOOSTING_ON:
                search_pipe[0]['$search']['compound']['should'] = [
                    {"text": {"query": '1', "path": "is_boosted",
                              "score": {"constant": {"value": PRODUCT_BOOST_CONSTANT_VAL}}}}]
        return search_pipe

    @classmethod
    def get_search_pipeline_for_mall(cls, keyword=""):
        search_terms_len = len(keyword.split(" "))
        if search_terms_len == 1:
            search_pipe = [
                {
                    "$search": {
                        "compound": {
                            "should": [
                                {
                                    "autocomplete": {
                                        "query": keyword,
                                        "path": "name",
                                    },
                                },
                                {
                                    "autocomplete": {
                                        "query": keyword,
                                        "path": "barcode",
                                    },
                                },
                            ]
                        },
                    }
                }
            ]
            if IS_PRODUCT_BOOSTING_ON:
                search_pipe[0]['$search']['compound']['should'] += [
                    {
                        "text": {
                            'query': '1',
                            "path": "is_boosted",
                            "score": {"constant": {"value": PRODUCT_BOOST_CONSTANT_VAL}}
                        }
                    }
                ]
        else:
            keyword = " ".join(
                list(
                    filter(lambda x: x not in CURRENCY_AND_MEASUREMENTS_KEYWORDS, keyword.split(" "))
                )
            )
            search_pipe = [
                {
                    "$search": {
                        "compound": {
                            "must": [
                                {
                                    "text": {
                                        "query": keyword,
                                        "path": "name"
                                    }
                                }
                            ]
                        }

                    }
                }
            ]
            if IS_PRODUCT_BOOSTING_ON:
                search_pipe[0]['$search']['compound']['should'] = [{
                    "text": {
                        'query': '1',
                        "path": "is_boosted",
                        "score": {"constant": {"value": PRODUCT_BOOST_CONSTANT_VAL}}
                    }
                }]
        return search_pipe

