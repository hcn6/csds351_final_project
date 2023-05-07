import time
import datasets
import numpy as np
import torch
import csv
from final_project.database import db
from final_project.clean_text.twitter_clean_text import preprocess_string
from transformers import AutoModelForSequenceClassification
from transformers import AutoTokenizer
import pprint

class Analysis:
  def __init__(self, 
               sentiment_tokenizer, 
               emotion_tokenizer, 
               spam_tokenizer, 
               model_sentiment, 
               model_emotion, 
               model_spam,):
      self.sentiment_tokenizer = sentiment_tokenizer
      self.emotion_tokenizer = emotion_tokenizer
      self.spam_tokenizer = spam_tokenizer

      self.model_sentiment = model_sentiment
      self.model_emotion = model_emotion
      self.model_spam = model_spam

      self.MAX_LENGTH = 64
      self.BATCH_SIZE = 64

      self.device = self.init_device()
  
  @classmethod
  def init_sentiment_analysis(cls):
    SENTIMENT = 'cardiffnlp/twitter-roberta-base-sentiment-latest'
    EMOTION = 'cardiffnlp/twitter-roberta-base-emotion'
    SPAM = "mrm8488/bert-tiny-finetuned-sms-spam-detection"

    sentiment_tokenizer = AutoTokenizer.from_pretrained(SENTIMENT)
    emotion_tokenizer = AutoTokenizer.from_pretrained(EMOTION)
    spam_tokenizer = AutoTokenizer.from_pretrained(SPAM)

    model_sentiment = AutoModelForSequenceClassification.from_pretrained(SENTIMENT)
    model_emotion = AutoModelForSequenceClassification.from_pretrained(EMOTION)
    model_spam = AutoModelForSequenceClassification.from_pretrained(SPAM)
    analysis = Analysis(sentiment_tokenizer=sentiment_tokenizer,
                        emotion_tokenizer=emotion_tokenizer,
                        spam_tokenizer=spam_tokenizer,
                        model_sentiment=model_sentiment,
                        model_emotion=model_emotion,
                        model_spam=model_spam)
    return analysis

  def init_device(self):
    if torch.cuda.is_available():
      device = torch.device("cuda")
    else:
      device = torch.device("cpu")
    return device

  def sentimental_anal(self, tweets):
    tokenizer = self.sentiment_tokenizer
    model = self.model_sentiment.to(self.device)
    labels = self.get_label_for_task('sentiment')
    return self.cal_stat_for_collection_with_labels(tokenizer, tweets, model)

  def get_label_for_task(self, task):
    labels = []
    mapping_link = f"analysis/mapping/{task}/mapping.txt"
    with open (mapping_link, 'r', encoding='utf-8') as f:
        html = f.read().split("\n")
        csvreader = csv.reader(html, delimiter='\t')
        
    labels = [row[1] for row in csvreader if len(row) > 1]
    return labels

  def preprocess_tweets(self, tweets):
    tweets = [preprocess_string(tweet) for tweet in tweets]
    return tweets

  def convert_tweets_for_training(self, tokenizer, tweets):
    clean_tweets = self.preprocess_tweets(tweets)
    encoded_input = tokenizer(clean_tweets, return_tensors='pt', padding="max_length", truncation=True, max_length=self.MAX_LENGTH)
    return encoded_input

  def generate_softmax_score(self, batch, model):
    input_ids = torch.tensor(batch['input_ids']).to(self.device)
    attention_mask = torch.tensor(batch['attention_mask']).to(self.device)
    output = model(input_ids=input_ids, attention_mask=attention_mask)
    
    softmax_layer = torch.nn.Softmax()
    scores = softmax_layer(output[0]).cpu()
    scores = scores.detach().numpy()
    batch['scores'] = scores
    return batch

  def cal_stat_for_collection_with_labels(self, tokenizer, tweets, model):
    processed_tweets = self.convert_tweets_for_training(tokenizer, tweets)

    dataset_tweets = datasets.Dataset.from_dict(processed_tweets)
    result = dataset_tweets.map(lambda batch: self.generate_softmax_score(batch, model), batched=True, batch_size=self.BATCH_SIZE)
    result_scores = np.array(result['scores'])
  
    return result_scores

  def tweet_sentiment_and_insert_db(self, tweet_json):
    tweets = [tweet['raw_text'] for tweet in tweet_json]

    scores = self.sentimental_anal(tweets)
    labels = self.get_label_for_task('sentiment')

    data_to_insert = []
    current_time = time.time()

    for score, tweet_dict in zip(scores, tweet_json):
      obj = {
        **tweet_dict,
        'inserted_at': current_time
      }
      for j, sub in enumerate(score):
        obj[labels[j]] = sub
      data_to_insert.append(obj)
    
    try:
      db.get_collection('reddit_sentiment').insert_many(data_to_insert)
      print(f"Successfully insert {len(data_to_insert)} tweet sentiment into db")
    except Exception as e:
      print(e)
      print("Failed to insert tweet sentiment into db")

  def tweet_sentiment(self, tweets):
    scores = self.sentimental_anal(tweets)
    labels = self.get_label_for_task('sentiment')

    data_to_insert = []
    current_time = time.time()

    for score, tweet in zip(scores, tweets):
      obj = {
        "tweet": tweet,
        'inserted_at': current_time
      }
      for j, sub in enumerate(score):
        obj[labels[j]] = sub
      data_to_insert.append(obj)
    return data_to_insert

if __name__ == "__main__":
  analysis = Analysis.init_sentiment_analysis()
  analysis.get_label_for_task('sentiment')
#   print("Initiating model")
#   collection = db.get_collection('reddit_raw_data')
#   tweets = [obj['raw_text'] for obj in list(collection.find({}).limit(10))]
#   SENTIMENT = 'cardiffnlp/twitter-roberta-base-sentiment-latest'
#   EMOTION = 'cardiffnlp/twitter-roberta-base-emotion'
#   SPAM = "mrm8488/bert-tiny-finetuned-sms-spam-detection"

#   sentiment_tokenizer = AutoTokenizer.from_pretrained(SENTIMENT)
#   emotion_tokenizer = AutoTokenizer.from_pretrained(EMOTION)
#   spam_tokenizer = AutoTokenizer.from_pretrained(SPAM)

#   model_sentiment = AutoModelForSequenceClassification.from_pretrained(SENTIMENT)
#   model_emotion = AutoModelForSequenceClassification.from_pretrained(EMOTION)
#   model_spam = AutoModelForSequenceClassification.from_pretrained(SPAM)
#   analysis = Analysis(sentiment_tokenizer=sentiment_tokenizer,
#                       emotion_tokenizer=emotion_tokenizer,
#                       spam_tokenizer=spam_tokenizer,
#                       model_sentiment=model_sentiment,
#                       model_emotion=model_emotion,
#                       model_spam=model_spam)

#   print("Start analyzing")
#   scores = analysis.sentimental_anal(tweets)
#   # Use get_label_for_task to get the labels for each score
#   labels = analysis.get_label_for_task('sentiment')
#   print(labels)
#   # Print tweet along with its sentiment score
#   for i in range(len(tweets)):
#     print(f"Tweet: {tweets[i]}")
#     for j, score in enumerate(scores[i]):
#       print(f"{labels[j]}: {score}")
#     print("=====================================")




  