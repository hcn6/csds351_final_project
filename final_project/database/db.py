from pymongo import MongoClient
from ..utils import logging

# MONGO_ENDPOINT = 'mongodb+srv://hieunguyen:Hieu1234@hieubase.r9ivh.gcp.mongodb.net'
MONGO_ENDPOINT = 'localhost'
MONGO_PORT = 27017

DATABASE_NAME = 'raw_data'
DB_COLLECTION = 'reddit'

client = MongoClient(MONGO_ENDPOINT, MONGO_PORT)

def get_collection(collection_name, db_name=DATABASE_NAME):
    current_client = client
    current_db = current_client[db_name]
    collection = current_db[collection_name]
    return collection