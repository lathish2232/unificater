from pymongo import MongoClient

class Mongo_DB_Connector:
    CONN_STR ="mongodb://127.0.0.1:27017"
    DATABASE_NAME ="unificater"
    client =MongoClient(CONN_STR)
    db =client[DATABASE_NAME]