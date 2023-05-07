from kafka import KafkaConsumer, TopicPartition
import pymongo
import re
import sys
import pprint


def connect_kafka_consumer(topic, partition):
    _consumer = None
    try:
        _consumer = KafkaConsumer(
            auto_offset_reset='latest',
            bootstrap_servers=['localhost:9092'], 
            consumer_timeout_ms=1000, 
            api_version=(0, 10),
            group_id = "reddit_stream",
            # enable_auto_commit=True,
            key_deserializer=lambda x: x.decode("utf-8"), 
            value_deserializer=lambda x: x.decode("utf-8"))
        tp = TopicPartition(topic, partition)
        _consumer.assign([tp])
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _consumer


if __name__ == "__main__":
    try:
        kafka_consumer = connect_kafka_consumer('reddit_posts', 0)
        # pprint.pprint(vars(kafka_consumer))
        while True:
            data = kafka_consumer.poll()
            for tp, comments in data.items():
                pprint.pprint(comments[0].value)
                print()
                # for comment in comments:
                #     print(comment.value)
                #     filter(comment.value, mycol)
    except Exception as e:
        print("Exception occurs")
        print(str(e))
    finally:
        if kafka_consumer != None:
            kafka_consumer.close()
            print("Closed consumer")
        print("Goodbye")
# try:
#     arg_length = len(sys.argv)
#     myclient = pymongo.MongoClient("mongodb+srv://hieu:Hieu1234@cluster0.uuizv.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
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

# except Exception as e:
#     print("Exception occurs")
#     print(str(e))
# finally:
#     if kafka_consumer != None:
#         kafka_consumer.close()
#         print("Closed consumer")
#     print("Goodbye")
