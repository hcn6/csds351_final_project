import pymongo
from datetime import datetime
from kafka import KafkaProducer
import json
import time

# Connect to the MongoDB server
client = pymongo.MongoClient("mongodb+srv://colab:Hieu1234@hieubase.r9ivh.gcp.mongodb.net/?retryWrites=true&w=majority")

# Access a specific database
db = client["reddit_data"]

# Access a specific collection in the database
collection = db["reddit_comment_praw"]

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Replace with your Kafka broker(s) address
    value_serializer=json_serializer
)

def send_message(topic, message):
    producer.send(topic, message)
    producer.flush()  # Ensure the message is sent before exiting the function

# Let cut-off point be 1588291200 (May 1, 2020 12:00:00 AM GMT)
# From the cut-off point, looping through the database and send the data to Kafka
# periodically based on created_utc

cut_off_point = datetime(2022, 12, 21, 0, 0, 0).timestamp()
start_time = datetime(2023, 5, 3, 23, 11, 0).timestamp()

def wait_until(next_time, cut_off_time=None):
    current_time = datetime.now().timestamp()
    if cut_off_time is not None :
        current_time = cut_off_time

    diff = next_time - current_time
    print(diff)
    if diff > 0:
        time.sleep(int(diff // 60))

pipeline = [
    {"$match": {"created_utc": {"$gt": cut_off_point}}},
    {"$sort": {"created_utc": 1}}
]

results = collection.aggregate(pipeline, allowDiskUse=True)

cur_time = cut_off_point

wait_until(start_time)

for doc in results:
    posted_time = doc["created_utc"]
    wait_until(posted_time, cur_time)
    print(posted_time)
    send_message('reddit-posts', {
        "message": doc["body"],
        "created_utc": str(datetime.fromtimestamp(posted_time)),
    })
