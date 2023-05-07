from pymongo import MongoClient
from final_project.utils import logging

# MONGO_ENDPOINT = 'mongodb+srv://hieunguyen:Hieu1234@hieubase.r9ivh.gcp.mongodb.net'
MONGO_ENDPOINT = 'localhost'
MONGO_PORT = 27017

DATABASE_NAME = 'reddit_data'

client = MongoClient(MONGO_ENDPOINT, MONGO_PORT)

def get_collection(collection_name, db_name=DATABASE_NAME):
    current_client = client
    current_db = current_client[db_name]
    collection = current_db[collection_name]
    return collection

if __name__ == '__main__':
    from pymongo import MongoClient

    # Connect to the source and destination databases
    source_client = MongoClient('mongodb://localhost:27017/')
    source_db = source_client['reddit_data']
    source_collection = source_db['reddit_comment_praw']

    dest_client = MongoClient('mongodb+srv://colab:Hieu1234@hieubase.r9ivh.gcp.mongodb.net/?retryWrites=true&w=majority')
    dest_db = dest_client['reddit_data']
    dest_collection = dest_db['reddit_comment_praw']

    # Loop over the documents in the source collection and insert them into the destination collection
    count = 0
    for document in source_collection.find():
        count += 1
        if count % 10000 == 0:
            logging.info(f"Finish upload {count} comments")
        dest_collection.insert_one(document)

    # Drop the source collection (optional)
    # source_collection.drop()

