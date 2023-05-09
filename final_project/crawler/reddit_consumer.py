import json

from kafka import KafkaConsumer, TopicPartition
import pymongo
import re
import sys
import pprint
from datetime import datetime
import time


def wait_until(next_time, cut_off_time=None, speed_up=1):
    current_time = datetime.now().timestamp()
    if cut_off_time is not None:
        current_time = cut_off_time

    diff = next_time - current_time
    print(diff)
    if diff > 0:
        time.sleep(int(diff // speed_up))


def connect_kafka_consumer(topic, partition):
    _consumer = None
    try:
        _consumer = KafkaConsumer(
            auto_offset_reset='earliest',
            bootstrap_servers=['localhost:9092'],
            consumer_timeout_ms=1000,
            api_version=(0, 10),
            group_id="reddit_stream",
            # enable_auto_commit=True,
            # key_deserializer=lambda x: x.decode("utf-8"),
            value_deserializer=lambda x: x.decode("utf-8"),
        )
        topic = TopicPartition(topic, partition)
        _consumer.assign([topic])
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _consumer


def polling_and_process():
    data = kafka_consumer.poll()
    for tp, comments in data.items():
        pprint.pprint(len(comments))
        for comment in comments:
            pprint.pprint(type(comment.value))
            json_obj = json.loads(comment.value)
            pprint.pprint(json_obj)
        print()


if __name__ == "__main__":
    kafka_consumer = None
    start_time = datetime(2023, 5, 8, 18, 20, 0).timestamp()
    time_interval = 60 * 60 * 24  # 1 day

    try:
        kafka_consumer = connect_kafka_consumer('reddit-posts-2', 0)
        # pprint.pprint(vars(kafka_consumer))
        wait_until(start_time, speed_up=60)
        cur_time = start_time
        while True:
            polling_and_process()
            wait_until(cur_time + time_interval, cut_off_time=cur_time, speed_up=15 * 60 * 24)
            cur_time = cur_time + time_interval
    except Exception as e:
        print("Exception occurs")
        print(str(e))
        print(vars(e.__traceback__))
    finally:
        if kafka_consumer:
            kafka_consumer.close()
            print("Closed consumer")
        print("Goodbye")

# try:
#     arg_length = len(sys.argv)
#     myclient = pymongo.MongoClient("mongodb+srv://hieu:Hieu1234@cluster0.uuizv.mongodb.net/"
#                                    + "myFirstDatabase?retryWrites=true&w=majority")
#     mydb = myclient["CSDS393"]
#     mycol = mydb[sys.argv[arg_length - 1]]
#     print(sys.argv[arg_length - 1])
#     kafka_consumer = connect_kafka_consumer(int(sys.argv[arg_length - 2]))
#     while True:
#         data = kafka_consumer.poll(100.0)
#         for tp, comments in data.items():
#             for comment in comments:
#                 # print(comment.value)
#                 filter(comment.value, mycol)
#
# except Exception as e:
#     print("Exception occurs")
#     print(str(e))
# finally:
#     if kafka_consumer != None:
#         kafka_consumer.close()
#         print("Closed consumer")
#     print("Goodbye")
