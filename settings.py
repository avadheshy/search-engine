from pymongo import MongoClient
import sentry_sdk

CLIENT = MongoClient("mongodb+srv://sharded-search-service:KC2718oU0Jt9Qt7v@search-service.ynzkd.mongodb.net/test")
SHARDED_SEARCH_DB = CLIENT.product_search

# Sentry

sentry_sdk.init(
    dsn="https://fa760c10052c4da88ba731dc48c9020e@o1194835.ingest.sentry.io/6768729",

    Set traces_sample_rate to 1.0 to capture 100%
    of transactions for performance monitoring.
    We recommend adjusting this value in production,
    traces_sample_rate=1.0,
)
