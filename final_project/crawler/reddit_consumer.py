import json
import pymongo
from kafka import KafkaConsumer, TopicPartition
import spacy
from collections import defaultdict
import pprint
from datetime import datetime
import time

from final_project.analysis.analysis import Analysis

SENTIMENT = 'cardiffnlp/twitter-roberta-base-sentiment-latest'
EMOTION = 'cardiffnlp/twitter-roberta-base-emotion'
SPAM = "mrm8488/bert-tiny-finetuned-sms-spam-detection"

analysis_model = Analysis.init_sentiment_analysis()

transformer_nlp = spacy.load('en_core_web_trf')
nlp = spacy.load('en_core_web_sm')

client = pymongo.MongoClient('localhost', 27017)

# Access a specific database
db = client["reddit_data"]

# Access a specific collection in the database
collection = db["reddit_analysis_interval"]


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


def average_sentiment_score(list_of_dicts):
    averages_dict = defaultdict(float)
    count_dict = defaultdict(int)

    for dict_ in list_of_dicts:
        for key, value in dict_.items():
            averages_dict[key] += value
            count_dict[key] += 1

    for key, value in averages_dict.items():
        averages_dict[key] = value / count_dict[key]

    return averages_dict


def merge_dict_keys(sets):
    merged_set = sets[0]

    for d in sets:
        merged_set = merged_set.union(d)
    return merged_set


def lower_case(text):
    return text.lower()


def ner_company_from_text(text, transformer=True):
    text = lower_case(text)
    doc = None
    if transformer:
        doc = transformer_nlp(text)
    else:
        doc = nlp(text)

    if transformer and len(doc) > nlp.max_length:
        doc = doc[:nlp.max_length]

    orgs = {}
    for ent in doc.ents:
        if ent.label_ == 'ORG':
            orgs[ent.text] = 1 if ent.text not in orgs else orgs[ent.text] + 1
    return set(orgs.keys())


def kafka_batch_analysis(texts, sentiment, batch_date):
    results = sentiment.tweet_sentiment(texts)
    ners = [ner_company_from_text(text) for text in texts]

    merged_company_set = merge_dict_keys(ners)

    merged_company_dict = {}
    for key in merged_company_set:
        merged_company_dict[key] = []

    for result, companies in zip(results, ners):
        for company in companies:
            score = {
                "positive": result["positive"],
                "negative": result["negative"],
                "neutral": result["neutral"],
                "normalize_score": normalize_score(result["positive"], result["neutral"], result["negative"])
            }
            merged_company_dict[company].append(score)

    merged_company_score = {}
    for key, value in merged_company_dict.items():
        merged_company_score[key] = {
            **average_sentiment_score(value),
            "count": len(value),
            "utc_interval_date": batch_date,
            "utc_interval_timestamp": batch_date.timestamp(),
        }

    return merged_company_score


def normalize_score(pos, neu, neg):
    return ((pos - neg) + 1) / 2


def update_date(ner_dict):
    company_dict = collection.find_one(
        {"company": ner_dict["company"],
         "utc_interval_timestamp": ner_dict["utc_interval_timestamp"]},
    )

    if company_dict is None:

        collection.insert_one(ner_dict)

    else:
        collection.update_one(
            {"company": ner_dict["company"],
             "utc_interval_timestamp": ner_dict["utc_interval_timestamp"]},
            {"$set": {
                "positive": (ner_dict["positive"] * ner_dict["count"]
                             + company_dict["positive"] * company_dict["count"])
                            / (ner_dict["count"] + company_dict["count"]),
                "negative": (ner_dict["negative"] * ner_dict["count"]
                             + company_dict["negative"] * company_dict["count"])
                            / (ner_dict["count"] + company_dict["count"]),
                "neutral": (ner_dict["neutral"] * ner_dict["count"]
                            + company_dict["neutral"] * company_dict["count"])
                           / (ner_dict["count"] + company_dict["count"]),
                "normalize_score": (ner_dict["normalize_score"] * ner_dict["count"]
                                    + company_dict["normalize_score"] * company_dict["count"])
                                   / (ner_dict["count"] + company_dict["count"]),
                "count": ner_dict["count"] + company_dict["count"],
            }}
        )


def polling_and_process():
    data = kafka_consumer.poll(2000)
    messages = []
    while data:
        for tp, comments in data.items():
            pprint.pprint(len(comments))
            messages.extend(json.loads(comment.value) for comment in comments)
        data = kafka_consumer.poll()
    print("Finished polling every messages. Start processing...")
    messages_content = [message.get('body') for message in messages]

    pprint.pprint(messages_content)
    if len(messages_content) == 0:
        print("No messages to process")
        return

    # This is for very edge cases when the previous batch left out some messages
    # from previous day
    message_dates = [datetime.combine(datetime.fromtimestamp(message.get('created_utc')).date(),
                                      datetime.min.time())
                     for message in messages]
    unique_dates = list(set(message_dates))

    for unique_date in unique_dates:
        message_content_by_date = [message.get('body') for message in messages
                                   if datetime.combine(datetime.fromtimestamp(message.get('created_utc')).date(),
                                                       datetime.min.time()) == unique_date]
        analysis_result = kafka_batch_analysis(message_content_by_date, analysis_model, unique_date)
        pprint.pprint(analysis_result)
        for key, value in analysis_result.items():
            update_date({
                "company": key,
                **value
            })


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
            wait_until(cur_time + time_interval, cut_off_time=cur_time, speed_up=60 * 24)
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
