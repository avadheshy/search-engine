import asyncio
import os
from pathlib import Path

from pymongo import MongoClient
import sentry_sdk
import motor.motor_asyncio
from dotenv import load_dotenv

dotenv_path = Path('.env')
load_dotenv(dotenv_path=dotenv_path)

ENVIRONMENT = os.getenv('ENVIRONMENT')

POS_SQL_HOST = os.getenv('POS_SQL_HOST')
POS_SQL_USER = os.getenv('POS_SQL_USER')
POS_SQL_PASSWORD = os.getenv('POS_SQL_PASSWORD')

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
