<<<<<<< HEAD
from pymongo import MongoClient,UpdateMany,UpdateOne
||||||| 0c653b4
from pymongo import MongoClient
=======
import asyncio
import os
from pathlib import Path

from pymongo import MongoClient
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510
import sentry_sdk
import motor.motor_asyncio
from dotenv import load_dotenv

<<<<<<< HEAD
CLIENT = MongoClient("mongodb+srv://sharded-search-service:KC2718oU0Jt9Qt7v@search-service.ynzkd.mongodb.net/test")
DB = CLIENT.product_search
||||||| 0c653b4
CLIENT = MongoClient("mongodb+srv://sharded-search-service:KC2718oU0Jt9Qt7v@search-service.ynzkd.mongodb.net/test")
SHARDED_SEARCH_DB = CLIENT.product_search
=======
dotenv_path = Path('.env')
load_dotenv(dotenv_path=dotenv_path)
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510

ENVIRONMENT = os.getenv('ENVIRONMENT')

<<<<<<< HEAD
sentry_sdk.init(
    # dsn="https://fa760c10052c4da88ba731dc48c9020e@o1194835.ingest.sentry.io/6768729",
||||||| 0c653b4
sentry_sdk.init(
    dsn="https://fa760c10052c4da88ba731dc48c9020e@o1194835.ingest.sentry.io/6768729",
=======
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510

<<<<<<< HEAD
    # # Set traces_sample_rate to 1.0 to capture 100%
    # # of transactions for performance monitoring.
    # # We recommend adjusting this value in production,
    # traces_sample_rate=1.0,
)
||||||| 0c653b4
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production,
    traces_sample_rate=1.0,
)
=======
# Making connection with Mongo using PyMongo
SHARDED_SEARCH_MONGO_CONNECTION = MongoClient(os.getenv('SHARDED_SEARCH_DB_SRV'))
SHARDED_SEARCH_DB = SHARDED_SEARCH_MONGO_CONNECTION[os.getenv('SHARDED_SEARCH_DB_NAME')]


# Making connection with Mongo using Motor for Async DB calls
# import nest_asyncio
loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)
# nest_asyncio.apply()
SHARDED_SEARCH_CLIENT_ASYNC = motor.motor_asyncio.AsyncIOMotorClient(io_loop=loop,
                                                                     host=os.getenv('SHARDED_SEARCH_DB_SRV'),
                                                                     tlsInsecure=True)
ASYNC_SHARDED_SEARCH_DB = SHARDED_SEARCH_CLIENT_ASYNC[os.getenv('SHARDED_SEARCH_DB_NAME')]


# Sentry Connection
SENTRY_BACKEND_PROJECT_DSN = os.getenv('SENTRY_BACKEND_PROJECT_DSN')
if ENVIRONMENT.lower() in ["prod", "uat"]:
    sentry_sdk.init(
        dsn=SENTRY_BACKEND_PROJECT_DSN,
        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        # We recommend adjusting this value in production,
        traces_sample_rate=1.0,
    )
>>>>>>> 4c8e26646d80b2a799ade7fd9dbcb3aa019bf510
