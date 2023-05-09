from pymongo import MongoClient
from final_project.utils import logging
from tqdm import tqdm

# MONGO_ENDPOINT = 'mongodb+srv://hieunguyen:Hieu1234@hieubase.r9ivh.gcp.mongodb.net'
# MONGO_ENDPOINT = 'localhost'
MONGO_ENDPOINT = 'mongodb+srv://dxn183:NBq4c7oQaFm7kaOD@cluster1.ylkmwu2.mongodb.net/'
MONGO_PORT = 27017

DATABASE_NAME = 'reddit_data'

client = MongoClient(MONGO_ENDPOINT, MONGO_PORT)

def get_collection(collection_name, db_name=DATABASE_NAME):
    current_client = client
    current_db = current_client[db_name]
    collection = current_db[collection_name]
    return collection

def get_db_by_url(url, db_name, port=MONGO_PORT):
    current_client = MongoClient(url, port)
    current_db = current_client[db_name]
    return current_db

def get_collection_by_url(url, db_name, collection_name, port=MONGO_PORT):
    current_client = MongoClient(url, port)
    current_db = current_client[db_name]
    collection = current_db[collection_name]
    return collection

if __name__ == '__main__':
    from pymongo import MongoClient

    # Connect to the source and destination databases
    source_client = MongoClient('localhost:27017')
    source_db = source_client['reddit_data']
    source_collection = source_db['reddit_comment_praw']

    dest_client = MongoClient('mongodb+srv://dxn183:NBq4c7oQaFm7kaOD@cluster1.ylkmwu2.mongodb.net/')
    dest_db = dest_client['reddit_data']
    dest_collection = dest_db['reddit_comment_praw']

    # Loop over the documents in the source collection and insert them into the destination collection
    count = 0
    inserted = set(x['_id'] for x in dest_collection.find())

    batch_docs = []
    # batch_size = 2000
    # query = {"created_utc": {"$lt": 1659312000}, "author": {"$ne": "AutoModerator"}}
    print("Querying...")
    query = {}
    all_data = list(source_collection.find(query))

    all_data = [x for x in all_data if x['_id'] not in inserted]

    for data in all_data:
        data.pop('body_html', None)

    print("Migrating...")

    chunk_size = 10000
    chunks = []
    for i in range(0, len(all_data), chunk_size):
        chunk = all_data[i:i+chunk_size]
        chunks.append(chunk)

    pbar = tqdm(total=len(chunks))
    for chunk in chunks:
        count += len(chunk)
        if count % chunk_size == 0:
            logging.info(f"Finish upload {count} comments")
        
        # if document['_id'] not in inserted:
        #     batch_docs.append(document)

        # if len(batch_docs) == batch_size or count == source_collection.count_documents({}) - 1:
        #     dest_collection.insert_many(batch_docs)
        #     batch_docs.clear()
        
        dest_collection.insert_many(chunk)
        pbar.update(1)
    pbar.close()
    # Drop the source collection (optional)
    # source_collection.drop()

