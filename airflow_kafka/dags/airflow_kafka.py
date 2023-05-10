from collections import defaultdict

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pprint
import json
import spacy
import pymongo
from final_project.analysis.analysis import Analysis

import logging

from kafka import KafkaConsumer, TopicPartition

dag = DAG('kafka_reddit_analysis',
          start_date=datetime(2021, 5, 10),
          schedule_interval='*/10 * * * *',
          catchup=False)

KAFKA_URL = 'localhost:9092'
REDDIT_TOPIC = 'reddit-posts-2'
REDDIT_PARTITION = 0

logger = logging.getLogger(__name__)

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


def update_date(ner_dict):

    logging.info("Updating Date to mongodb!")

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


def normalize_score(pos, neu, neg):
    return ((pos - neg) + 1) / 2


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


def polling_and_process():
    kafka_consumer = KafkaConsumer(
        auto_offset_reset='earliest',
        bootstrap_servers=['localhost:9092'],
        consumer_timeout_ms=1000,
        api_version=(0, 10),
        group_id="reddit_stream",
        # enable_auto_commit=True,
        # key_deserializer=lambda x: x.decode("utf-8"),
        value_deserializer=lambda x: x.decode("utf-8"),
    )
    topic = TopicPartition(REDDIT_TOPIC, REDDIT_PARTITION)
    kafka_consumer.assign([topic])
    data = kafka_consumer.poll(2000)
    messages = []
    while data:
        for tp, comments in data.items():
            messages.extend(json.loads(comment.value) for comment in comments)
        data = kafka_consumer.poll()

    messages_content = [message.get('body') for message in messages]

    pprint.pprint(messages_content)
    if len(messages_content) == 0:
        logging.info("No messages to process")
        return

    logging.info(f"Received {len(messages)} messages. Start processing...")

    # This is for very edge cases when the previous batch left out some messages
    # from previous day
    message_dates = [datetime.combine(datetime.fromtimestamp(message.get('created_utc')).date(),
                                      datetime.min.time())
                     for message in messages]

    unique_dates = list(set(message_dates))

    for unique_date in unique_dates:
        logging.info(f"Processing messages for {unique_date}")
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

    kafka_consumer.close()
    logging.info(f"Finished processing!")


def check_kafka_online():
    try:
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_URL)
        consumer.close()
        return 'read_kafka'
    except Exception as e:
        raise Exception('Kafka is not online') from e


def batch_process():
    polling_and_process()


check_kafka_task = PythonOperator(
    task_id='check_kafka',
    python_callable=check_kafka_online,
    dag=dag,
)

batch_process_task = PythonOperator(
    task_id='batch_process',
    python_callable=batch_process,
    dag=dag,
)

# Define task dependencies
check_kafka_task >> batch_process_task
