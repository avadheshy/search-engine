from fastapi.testclient import TestClient

from app import app
from api_constants import ApiUrlConstants

client = TestClient(app)
expected_response_without_x_source = {
    "count": int,
    "rows": int,
    "currentPage": int,
    "numFound": int,
    "lastPage": int,
    "productIds": list,
    "groupIds": list,
    "filters": list
}
expected_response_with_x_source = {
    "count": int,
    "rows": int,
    "currentPage": int,
    "numFound": int,
    "lastPage": int,
    "productIds": list,
    "groupIds": list,
    "filters": dict
}
expected_response_for_group = {
    "total": int,
    "data": list,
}


def test_read_main():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"msg": "Hello World"}


def test_product_listing_v1_for_mall():
    response = client.post(
        url=ApiUrlConstants.V1_PRODUCT_LISTING,
        json={
            "store_id": "64",
            "page": "1",
            "filters_for": "tag",
            "filter_id": "358",
            "sort_by": "min_price",
            "type": "mall",
            "per_page": 10,
            "brandIds": {
                "0": "1662"
            },
            "categories": None
        },
    )

    response_data = response.json()
    assert response.status_code == 200
    assert response_data.keys() == expected_response_with_x_source.keys()
    for key, value in response_data.items():
        assert type(value) == expected_response_with_x_source.get(key)


def test_product_listing_v1_for_mall_tag():
    response = client.post(
        url=ApiUrlConstants.V1_PRODUCT_LISTING,
        json={
            "store_id": "64",
            "page": "1",
            "filters_for": "tag",
            "filter_id": "345",
            "sort_by": "min_price",
            "type": "mall",
            "x_source": "android_app"

        },
    )

    response_data = response.json()
    assert response.status_code == 200
    assert response_data.keys() == expected_response_without_x_source.keys()
    for key, value in response_data.items():
        assert type(value) == expected_response_without_x_source.get(key)


def test_product_listing_v1_for_mall_x_source():
    response = client.post(
        url=ApiUrlConstants.V1_PRODUCT_LISTING,
        json={
            "store_id": "64",
            "page": "1",
            "filters_for": "tag",
            "filter_id": "345",
            "sort_by": "min_price",
            "type": "mall",
            "x_source": "android_app"

        },
    )

    response_data = response.json()
    assert response.status_code == 200
    assert response_data.keys() == expected_response_without_x_source.keys()
    for key, value in response_data.items():
        assert type(value) == expected_response_without_x_source.get(key)


def test_product_listing_v1_for_mall_cl1():
    response = client.post(
        url=ApiUrlConstants.V1_PRODUCT_LISTING,
        json={
            "store_id": "64",
            "page": "1",
            "filters_for": "cl1",
            "filter_id": "28",
            "sort_by": "max_price",
            "type": "mall",
            "x_source": "android_app"

        },
    )

    response_data = response.json()
    assert response.status_code == 200
    assert response_data.keys() == expected_response_without_x_source.keys()
    for key, value in response_data.items():
        assert type(value) == expected_response_without_x_source.get(key)


def test_product_listing_v1_for_mall_cl2():
    response = client.post(
        url=ApiUrlConstants.V1_PRODUCT_LISTING,
        json={
            "store_id": "64",
            "page": "1",
            "filters_for": "cl2",
            "filter_id": "228",
            "sort_by": "max_price",
            "type": "mall",
            "x_source": "android_app"

        },
    )

    response_data = response.json()
    assert response.status_code == 200
    assert response_data.keys() == expected_response_without_x_source.keys()
    for key, value in response_data.items():
        assert type(value) == expected_response_without_x_source.get(key)


def test_product_listing_v1_for_mall_cl3():
    response = client.post(
        url=ApiUrlConstants.V1_PRODUCT_LISTING,
        json={
            "store_id": "64",
            "page": "1",
            "filters_for": "cl3",
            "filter_id": "970",
            "sort_by": "new",
            "type": "mall",
            "x_source": "android_app"

        },
    )

    response_data = response.json()
    assert response.status_code == 200
    assert response_data.keys() == expected_response_without_x_source.keys()
    for key, value in response_data.items():
        assert type(value) == expected_response_without_x_source.get(key)


def test_product_listing_v1_for_mall_cl4():
    response = client.post(
        url=ApiUrlConstants.V1_PRODUCT_LISTING,
        json={
            "store_id": "64",
            "page": "1",
            "filters_for": "cl4",
            "filter_id": "880",
            "sort_by": "popularity",
            "type": "mall",
            "x_source": "android_app"
        },
    )

    response_data = response.json()
    assert response.status_code == 200
    assert response_data.keys() == expected_response_without_x_source.keys()
    for key, value in response_data.items():
        assert type(value) == expected_response_without_x_source.get(key)


def test_product_listing_v1_for_mall_category():
    response = client.post(
        url=ApiUrlConstants.V1_PRODUCT_LISTING,
        json={
            "store_id": "64",
            "page": "1",
            "filters_for": "category",
            "filter_id": "776",
            "sort_by": "revelence",
            "type": "mall",
            "x_source": "android_app"

        },
    )

    response_data = response.json()
    assert response.status_code == 200
    assert response_data.keys() == expected_response_without_x_source.keys()
    for key, value in response_data.items():
        assert type(value) == expected_response_without_x_source.get(key)


def test_product_listing_v1_for_mall_brand():
    response = client.post(
        url=ApiUrlConstants.V1_PRODUCT_LISTING,
        json={
            "store_id": "64",
            "page": "1",
            "filters_for": "brand",
            "filter_id": "1662",
            "sort_by": "max_price",
            "type": "mall",
            "x_source": "android_app"

        },
    )

    response_data = response.json()
    assert response.status_code == 200
    assert response_data.keys() == expected_response_without_x_source.keys()
    for key, value in response_data.items():
        assert type(value) == expected_response_without_x_source.get(key)


def test_product_listing_v1_for_mall_group():
    response = client.post(
        url=ApiUrlConstants.V1_PRODUCT_LISTING,
        json={
            "store_id": "64",
            "page": "1",
            "filters_for": "group",
            "filter_id": "72770",
            "sort_by": "created_at",
            "type": "mall",
            "x_source": "android_app"

        },
    )

    response_data = response.json()
    assert response.status_code == 200
    assert response_data.keys() == expected_response_without_x_source.keys()
    for key, value in response_data.items():
        assert type(value) == expected_response_without_x_source.get(key)


def test_product_listing_v1_for_mall_and_source_android_app():
    response = client.post(
        url=ApiUrlConstants.V1_PRODUCT_LISTING,
        json={
            "store_id": 1,
            "page": 1,
            "filters_for": "tag",
            "filter_id": 345,
            "type": "mall",
            "sort_by": "min_price",
            "per_page": 15,
            "x_source": "android_app",
            "brandIds": {"0": 1020, "1": 1113, "2": 1140, "3": 1152, "4": 1153},
            "categories": {"0": 936}

        },
    )

    response_data = response.json()
    assert response.status_code == 200
    assert response_data.keys() == expected_response_without_x_source.keys()
    for key, value in response_data.items():
        assert type(value) == expected_response_without_x_source.get(key)


def test_product_search_v2_0():
    response = client.get(url=ApiUrlConstants.V2_PRODUCT_SEARCH_FOR_GROUP,
                          params={
                              "store_id": 10,
                              "keyword": "belt",
                              "type": "mall",
                              "platform": "app",
                              "skip": 0,
                              "limit": 10,
                              'should_group': 'false'
                          }
                          )

    response_data = response.json()

    assert response.status_code == 200
    assert response_data.keys() == expected_response_for_group.keys()
    for key, value in response_data.items():
        assert type(value) == expected_response_for_group.get(key)
    assert len(response_data.get('data')) <= 10


def test_product_search_v2_1():
    response = client.get(url=ApiUrlConstants.V2_PRODUCT_SEARCH_FOR_GROUP,
                          params={
                              "store_id": 10,
                              "keyword": "belt",
                              "type": "mall",
                              "platform": "app",
                              "skip": 0,
                              "limit": 15,
                              'should_group': 'true'
                          }
                          )

    response_data = response.json()
    assert response.status_code == 200
    assert response_data.keys() == expected_response_for_group.keys()
    for key, value in response_data.items():
        assert type(value) == expected_response_for_group.get(key)
    assert len(response_data.get('data')) <= 15


def test_product_search_v1_retail():
    response = client.post(
        url=ApiUrlConstants.V1_PRODUCT_SEARCH,
        json={
            "type": "retail",
            "store_id": "168",
            "keyword": "rice",
            "platform": "pos",
            "skip": "0",
            "limit": "10"
        })

    response_data = response.json()
    assert response.status_code == 200
    assert response_data.keys() == expected_response_for_group.keys()
    for key, value in response_data.items():
        assert type(value) == expected_response_for_group.get(key)


def test_product_search_v1_retail_1():
    response = client.post(
        url=ApiUrlConstants.V1_PRODUCT_SEARCH,
        json={
            "type": "retail",
            "store_id": "168",
            "keyword": "cadbury dairy milk",
            "platform": "pos",
            "skip": "0",
            "limit": "10"
        })

    response_data = response.json()
    assert response.status_code == 200
    assert response_data.keys() == expected_response_for_group.keys()
    for key, value in response_data.items():
        assert type(value) == expected_response_for_group.get(key)


def test_product_search_v1_mall():
    response = client.post(
        url=ApiUrlConstants.V1_PRODUCT_SEARCH,
        json={
            "type": "mall",
            "store_id": "168",
            "keyword": "belt",
            "platform": "pos",
            "skip": "0",
            "limit": "10"
        })

    response_data = response.json()
    assert response.status_code == 200
    assert response_data.keys() == expected_response_for_group.keys()
    for key, value in response_data.items():
        assert type(value) == expected_response_for_group.get(key)


def test_store_map():
    response = client.get(url=ApiUrlConstants.STORE_MAP)
    response_data = response.json()

    assert response.status_code == 200
    assert isinstance(response_data, dict)

