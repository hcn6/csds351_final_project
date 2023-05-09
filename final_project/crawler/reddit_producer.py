import praw
import re
import sys
from kafka import KafkaProducer
from stream_reddit import RedditStream
import json
import time
from datetime import datetime
import pymongo

def publish_message(producer_instance, topic_name, partition, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(
            topic_name, partition=partition, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def process_reddit_object(comment):
    return {
        "comment_id": comment.id,
        "raw_text": comment.body,
        "created_at": comment.created_utc,
        "permalink": comment.permalink,
        "author_name": comment.author.name,
        "author_id": comment.author.id,
        "subreddit": comment.subreddit_name_prefixed
    }


def convert_json_object_to_string(json_object):
    return json.dumps(json_object)


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def remove_emoji(comment):
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U0001f900-\U0001f999"
                               u"\U00002702-\U00002f7B"
                               u"\U000024C2-\U0001F251"
                               u"\u00f0-\u99f0"
                               "]+", flags=re.UNICODE)

    cleaned_comment = emoji_pattern.sub(r'', comment)
    return cleaned_comment

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def wait_until(next_time, cut_off_time=None):
    current_time = datetime.now().timestamp()
    if cut_off_time is not None :
        current_time = cut_off_time

    diff = next_time - current_time
    print(diff)
    if diff > 0:
        time.sleep(int(diff // 60))


if __name__ == "__main__":
    # Connect to the MongoDB server
    client = pymongo.MongoClient("mongodb+srv://colab:Hieu1234@hieubase.r9ivh.gcp.mongodb.net/?retryWrites=true&w=majority")

    # Access a specific database
    db = client["reddit_data"]

    # Access a specific collection in the database
    collection = db["reddit_post"]

    # Let cut-off point be 1588291200 (May 1, 2020 12:00:00 AM GMT)
    # From the cut-off point, looping through the database and send the data to Kafka
    # periodically based on created_utc

    cut_off_point = datetime(2022, 12, 21, 0, 0, 0).timestamp()
    start_time = datetime(2023, 5, 3, 23, 11, 0).timestamp()

    pipeline = [
        {"$match": {"created_utc": {"$gt": cut_off_point}}},
        {"$sort": {"created_utc": 1}}
    ]

    results = collection.aggregate(pipeline, allowDiskUse=True)

    cur_time = cut_off_point

    kafka_producer = connect_kafka_producer()

    print(kafka_producer)

    wait_until(start_time)

    for doc in results:
        print(doc)
        posted_time = doc["created_utc"]
        wait_until(posted_time, cur_time)
        print(posted_time)
        data = {
            "message": doc["selftext"],
            "created_utc": str(datetime.fromtimestamp(posted_time)),
        }
        publish_message(kafka_producer, 'reddit_posts',
                            0, 'raw_data', convert_json_object_to_string(data))
