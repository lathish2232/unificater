import logging

from pymongo import MongoClient

from service.util.json_utils import extract_sub_json

LOGGER = logging.getLogger('unify_service')


def get_mongod_connection():
    try:
        LOGGER.info('Creating MongoDB connection')
        CONN_STR = "mongodb://127.0.0.1:27017"
        DATABASE_NAME = "unificater"
        client = MongoClient(CONN_STR)
        db = client[DATABASE_NAME]
    except Exception:
        LOGGER.error('Exception occurred while connection database.')
        raise Exception('Exception occurred while connection database.')
    finally:
        LOGGER.info('MongoDB connection closed')
        if client: client.close()
    return db


def get_data_from_collection_json(collection, url):
    mongo_db = get_mongod_connection()
    record = mongo_db[collection].find_one({}, {'_id': 0})
    data = extract_sub_json(url, record)[3]
    return data

def get_collections():
    mongo_db=get_mongod_connection()
    return mongo_db.list_collection_names()


class Mongo_DB_Connector:
    try:
        LOGGER.info('Creating MongoDB connection')
        CONN_STR = "mongodb://127.0.0.1:27017"
        DATABASE_NAME = "unificater"
        client = MongoClient(CONN_STR)
        db = client[DATABASE_NAME]
    except Exception:
        LOGGER.error('Exception occurred while connection database.')
        raise Exception('Exception occurred while connection database.')
    finally:
        LOGGER.info('MongoDB connection closed')
        if client: client.close()
        
