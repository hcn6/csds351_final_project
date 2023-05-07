from pymongo import MongoClient
from final_project.database import db

def mapper(sentence):
    for word in tokenize(sentence):
        yield {'word': word.lower(), 'count': 1}

# Define the reduce function

def reducer(key, values):
    return {'word': key, 'count': sum(values)}


def tokenize(text):
    return text.split(" ");

# Run the MapReduce job
# result = db.command('mapReduce', 'mycollection',
#                     map=map_function, reduce=reduce_function, out='myoutput')

# # Insert the output into the MongoDB collection
# for doc in db.myoutput.find():
#     collection.insert_one(doc['value'])
