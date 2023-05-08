from analysis.analysis import Analysis
import json
import logging
import re
from nltk.corpus import stopwords
from kafka import TopicPartition
import time
from nltk.tokenize import sent_tokenize, word_tokenize
from crawler.reddit_consumer import connect_kafka_consumer
import pprint
from clean_text import twitter_clean_text as preprocess
from database import db
import nltk
import ner
from collections import defaultdict
nltk.download('punkt')
nltk.download('stopwords')


def convert_json_string_to_dict(json_string):
    return json.loads(json_string)


def tokenize_word(sentence):
    words = word_tokenize(sentence)
    return words


def tokenize_sentence(text):
    sentences = sent_tokenize(text)
    return sentences


def remove_non_characters(words):
    pattern = re.compile('[^a-zA-Z]')
    words = [word for word in words if not pattern.match(word)]
    return words


def remove_stopwords(words):
    words = [word for word in words if word not in stopwords.words('english')]
    return words


def process_sentence(sentence):
    words = tokenize_word(sentence)
    words = remove_non_characters(words)
    words = remove_stopwords(words)
    return words


def init_sentiment_analysis():
    print("Initiating model")
    SENTIMENT = 'cardiffnlp/twitter-roberta-base-sentiment-latest'
    EMOTION = 'cardiffnlp/twitter-roberta-base-emotion'
    SPAM = "mrm8488/bert-tiny-finetuned-sms-spam-detection"

    sentiment_tokenizer = AutoTokenizer.from_pretrained(SENTIMENT)
    emotion_tokenizer = AutoTokenizer.from_pretrained(EMOTION)
    spam_tokenizer = AutoTokenizer.from_pretrained(SPAM)

    model_sentiment = AutoModelForSequenceClassification.from_pretrained(
        SENTIMENT)
    model_emotion = AutoModelForSequenceClassification.from_pretrained(EMOTION)
    model_spam = AutoModelForSequenceClassification.from_pretrained(SPAM)
    analysis = Analysis(sentiment_tokenizer=sentiment_tokenizer,
                        emotion_tokenizer=emotion_tokenizer,
                        spam_tokenizer=spam_tokenizer,
                        model_sentiment=model_sentiment,
                        model_emotion=model_emotion,
                        model_spam=model_spam)

    return analysis


def map_reduce_and_update_db(data, current_time):
    output_db = db.get_collection('reddit_word_frequency')
    time_series_db = db.get_collection('reddit_time_series')
    # Preprocess the data
    data = data.lower()
    data = data.replace('\n', ' ')

    # Split the data into sentences
    sentences = tokenize_sentence(data)

    # Map the words
    mapped_words = []
    for sentence in sentences:
        words = process_sentence(sentence)
        for word in words:
            mapped_words.append((word, 1))

    # Reduce the word counts
    word_frequencies = {}
    for key, value in mapped_words:
        if key in word_frequencies:
            word_frequencies[key] += value
        else:
            word_frequencies[key] = value

    # Insert the result into MongoDB
    current_time = time.time()
    for key, value in word_frequencies.items():
        try:
            output_db.update_one(
                {"word": key},
                {"$inc": {"count": value},
                 "$set": {"timestamp": current_time}},
                upsert=True
            )
            time_series_db.insert_one(
                {"word": key, "count": value, "timestamp": current_time}
            )
        except Exception as e:
            print(e)


"""
Put read_messages as a method of RedditStream object
Read_messages will be a generator.
"""

logger = logging.getLogger(__name__)


def initialize_consumer(topic_name, partition):
    kafka_consumer = connect_kafka_consumer(topic_name, partition)
    topic_partition = TopicPartition(topic_name, partition)
    kafka_consumer.assign([topic_partition])
    return kafka_consumer, topic_partition


def read_messages(kafka_consumer, topic_partition):
    data = None
    while True:
        data = kafka_consumer.poll()
        if len(data) > 0:
            yield data


def map_reduce_layer(comments, current_time):
    """Process a batch of comments and update the word frequency collection."""
    for comment in comments:
        comment = convert_json_string_to_dict(comment.value)
        comment['raw_text'] = preprocess.preprocess_string(comment['raw_text'])
        map_reduce_and_update_db(comment['raw_text'], current_time)


def store_raw_data_layer(data, current_time):
    output_db = db.get_collection('reddit_raw_data')
    for comment in data:
        comment = convert_json_string_to_dict(comment.value)
        output_db.insert_one({**comment,
                              "insert_time": current_time})


def sentiment_layer(sentiment, comments):
    """Process a batch of comments and update the sentiment collection."""
    comments = [convert_json_string_to_dict(
        comment.value) for comment in comments]
    sentiment.tweet_sentiment_and_insert_db(comments)
# def mapping_sentiment_to_company(sentiments, companies):

def merge_dict_keys(sets):
    merged_set = sets[0]
    
    for d in sets:
        merged_set = merged_set.union(d)
    return merged_set

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

def kafka_batch_analysis(texts, sentiment):
    results = sentiment.tweet_sentiment(texts)
    ners = [ner.ner_company_from_text(text) for text in texts]
    merged_company_set = merge_dict_keys(ners)

    merged_company_dict = {}
    for key in merged_company_set:
        merged_company_dict[key] = []

    for result, companies in zip(results, ners):
        for company in companies:
            score = {
                "positive": result["positive"],
                "negative": result["negative"],
                "neutral": result["neutral"]
            }
            merged_company_dict[company].append(score)
    
    merged_company_score = {}
    for key, value in merged_company_dict.items():
        merged_company_score[key] = average_sentiment_score(value)

    return merged_company_score

def mongodb_batch_sentiment(analysis, texts):
    scores = analysis.sentimental_anal(texts)
    # print(scores)
    labels = analysis.get_label_for_task('sentiment')
    # Print tweet along with its sentiment score
    all_scores = []

    for i in range(len(scores)):
      score_dict = {}
      # print(f"Tweet: {scores[i]}")
      for j, score in enumerate(scores[i]):
        # print(f"{labels[j]}: {score}")
        score_dict[labels[j]] = score
      all_scores.append(score_dict)
    
    return all_scores
 
if __name__ == "__main__":
    from transformers import pipeline
    comment_db_url = "mongodb+srv://dxn183:NBq4c7oQaFm7kaOD@cluster1.ylkmwu2.mongodb.net/"
    post_db_url = "mongodb+srv://colab:Hieu1234@hieubase.r9ivh.gcp.mongodb.net/?retryWrites=true&w=majority"
    comment_output_url = "mongodb+srv://dxn183:P4TnUn0wuNZqztQx@cluster0.7tqovhs.mongodb.net/"

    print("Loading sentiment analysis pipeline...")
    # Connect to MongoDB
    data_collection = db.get_collection_by_url(url=comment_db_url, db_name="reddit_data", collection_name="reddit_comment_praw")
    analysis_log = db.get_collection_by_url(url=comment_output_url , db_name="reddit_data", collection_name="reddit_comment_sentiment_analysis_log")
    output_collection = db.get_collection_by_url(url=comment_output_url, db_name="reddit_data", collection_name="reddit_comment_sentiment_score")

    # Define the batch size
    db_batch_size = 10000
    # model_batch_size = 16

    # Get the IDs of the documents that have already been analyzed
    analyzed_ids = set(x["document_id"] for x in analysis_log.find())
    analysis = Analysis.init_sentiment_analysis()
    # batch_docs = []

    print("Starting sentiment analysis...")

    time_out_duration = 7200000
    data_cursor = list(data_collection.find(max_time_ms=time_out_duration))
    
    print("Accumulate texts...")
    data_cursor = [d for d in data_cursor if d.get('_id') not in analyzed_ids]
    print("Total {} documents".format(len(data_cursor)))
    
    
    chunk_size = db_batch_size
    chunks = []
    for i in range(0, len(data_cursor), chunk_size):
        chunk = data_cursor[i:i+chunk_size]
        chunks.append(chunk)
    # Iterate over the collection using a cursor
    # for i, document in enumerate(data_cursor):
    for i, batch_docs in enumerate(chunks):
        # Check if the document has already been analyzed
        # if document["_id"] in analyzed_ids \
        # or document["author"] == "AutoModerator":
        #     continue
        print(f"{i}. Getting chunk...")
        # batch_docs.append(document)
        start = time.time()
        # Perform sentiment analysis on the text
        if len(batch_docs) == db_batch_size or i == data_collection.count_documents({}) - 1:
            print("Analyzing sentiment...")
            batch_texts = [doc["body"] for doc in batch_docs]
            results = mongodb_batch_sentiment(analysis, batch_texts)
            print(f"Finish analysis on {i} in {time.time() - start}")
            outputs = []
            logs = []
            for result, document in zip(results, batch_docs):
                # Add the sentiment analysis result to the document
                outputs.append({"_id": document["_id"], "sentiment": result, "text": document["body"]})
                # Log the analysis in the analysis log collection
                logs.append({"document_id": document["_id"], "sentiment": result})
            
            output_collection.insert_many(outputs)
            analysis_log.insert_many(logs)
            # batch_docs.clear()

        # If the batch size has been reached, print a status update and sleep for a bit
        # if i % db_batch_size == 0 and i > 0:
        print(f"Processed {i * db_batch_size} documents.")
        print(f"Batch {i} took {time.time() - start} seconds.")
        time.sleep(5)

# 01-08-2022 -> past
# read by hours -> output to json, with timestamp as key

# try:
#     kafka_consumer = connect_kafka_consumer('reddit_posts', 0)
#     kafka_consumer, topic_partition = initialize_consumer('reddit_posts', 0)
#     batch = 1
#     # kafka_consumer.seek_to_end(topic_partition)
#     for data in read_messages(kafka_consumer, topic_partition):
#         count = 0
#         pos = kafka_consumer.position(topic_partition)
#         logger.info("Most recent offset: %s", pos)

#         current_time = time.time()
#         for topic_partition, comments in data.items():
#             logger.info("Processing %d comments...", len(comments))
#             map_reduce_layer(comments, current_time)
#             store_raw_data_layer(comments, current_time)
#             sentiment_layer(comments)
#             count += len(comments)

#         logger.info(f"Finished processing batch {batch}")
#         batch += 1
#         time.sleep(60)

# except Exception as e:
#     logger.exception("An exception occurred: %s", str(e))

# logger.info("Goodbye")
