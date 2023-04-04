from pymongo import MongoClient
from final_project.database import db

# Define the map function
def map_function():
    for sentence in sentences:
        for word in sentence.split():
            yield {'word': word.lower(), 'count': 1}

# Define the reduce function
def reduce_function(key, values):
    return {'word': key, 'count': sum(values)}

# Run the MapReduce job
result = db.command('mapReduce', 'mycollection', map=map_function, reduce=reduce_function, out='myoutput')

# Insert the output into the MongoDB collection
for doc in db.myoutput.find():
    collection.insert_one(doc['value'])
