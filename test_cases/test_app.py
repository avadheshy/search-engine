from fastapi.testclient import TestClient

from app import app

client = TestClient(app)


def test_read_main():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"msg": "Hello World"}


def test_product_listing_v1_for_mall():
    response = client.post(
        url="/v1/product-listing/",
        json={
            "store_id": "64",
            "page": "1",
            "filters_for": "tag",
            "filter_id": "358",
            "sort_by": "min_price",
            "type": "mall",
            "per_page": 10,
            "brandIds": [1662],
            "categories": None
        },
    )
    dummy_response = {
        "count": int,
        "rows": int,
        "currentPage": int,
        "numFound": int,
        "lastPage": int,
        "productIds": list,
        "groupIds": list,
        "filters": dict
    }

    response_data = response.json()
    assert response.status_code == 200
    assert response_data.keys() == dummy_response.keys()
    for key, value in response_data.items():
        assert type(value) == dummy_response.get(key)


def test_product_listing_v1_for_mall_and_source_android_app():
    response = client.post(
        url="/v1/product-listing/",
        json={
            "store_id": "64",
            "page": "1",
            "filters_for": "tag",
            "filter_id": "358",
            "sort_by": "min_price",
            "type": "mall",
            "per_page": 10,
            "brandIds": [1662],
            "categories": None,
            "x_source": "android_app"
        },
    )
    dummy_response = {
        "count": int,
        "rows": int,
        "currentPage": int,
        "numFound": int,
        "lastPage": int,
        "productIds": list,
        "groupIds": list,
        "filters": list
    }

    response_data = response.json()
    assert response.status_code == 200
    assert response_data.keys() == dummy_response.keys()
    for key, value in response_data.items():
        assert type(value) == dummy_response.get(key)
