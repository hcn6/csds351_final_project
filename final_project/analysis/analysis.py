import time
import datasets
import numpy as np
import torch
import csv
import database.db as db
import filter_spam
from utils import logging

class Anal:
  def __init__(self, sentiment_tokenizer, emotion_tokenizer, spam_tokenizer, model_sentiment, model_emotion, model_spam):
      self.sentiment_tokenizer = sentiment_tokenizer
      self.emotion_tokenizer = emotion_tokenizer
      self.spam_tokenizer = spam_tokenizer

      self.model_sentiment = model_sentiment
      self.model_emotion = model_emotion
      self.model_spam = model_spam

      self.MAX_LENGTH = 64
      self.BATCH_SIZE = 16

  def filter_spam_tweet(self, tweets_dict):
    return filter_spam.filter_spam(tweets_dict, self.spam_tokenizer, self.model_spam)

  def analyze_tweets(self, tweets_dict, twitter_data):
      q1 = time.time()
      username = twitter_data['name']
      
      tweets_list = list(set(tweets_dict.keys()))
      tweets_list = self.filter_spam_tweet({
          'name': username,
          'tweets': tweets_list
      })

      url = f"https://twitter.com/{twitter_data['username']}"
      logging.info(f'Start analyzing sentiment for {username}')

      scores = self.sentimental_anal({
          'name': username,
          'tweets': tweets_list
      }, self.sentiment_tokenizer, self.model_sentiment)
      q2 = time.time()
      logging.info(f'Finish analyzing sentiment for {username} in {q2 - q1}')

      
      avg_negative = np.mean(scores[:, 0])
      std_negative = np.std(scores[:, 0])

      avg_neutral = np.mean(scores[:, 1])
      std_neutral = np.std(scores[:, 1])

      avg_positive = np.mean(scores[:, 2])
      std_positive = np.std(scores[:, 2])

      norm_scores = (scores.dot(np.array([-1, 0, 1])) + 1) / 2

      avg_norm = np.mean(norm_scores)
      std_norm = np.std(norm_scores)

      logging.info(f'Start analyzing emotion for {username}')

      emotion_scores = self.emotion_anal({
          'name': username,
          'tweets': tweets_list
      }, self.emotion_tokenizer, self.model_emotion)
      q3 = time.time()
      logging.info(f'Finish analyzing emotion for {username} in {q3 - q2}')
      
      data = []
      
      for tweet, sentiment, emotion in zip(tweets_list, scores, emotion_scores):
        json = {
          "tweet": tweet,
          "sentiment": list(sentiment),
          "emotion": list(emotion),
          "collection": username,
          "inserted_at": time.time() * 1000,
          "url": url,
          "created_at": tweets_dict[tweet]['created_at'],
          "_id": tweets_dict[tweet]['id']
        }
        data.append(json)
      
      # data = [{k:v} for k, v in zip(tweets_list, scores) ]
      try:
        db.get_collection("tweets_score").insert_many(data, ordered=False)
      except:
        logging.warn("Just Duplicates")
      
      # scam_tweets = get_scam_tweet(tweets_list, scam_label)

      avg_anger = np.mean(emotion_scores[:, 0])
      std_anger = np.std(emotion_scores[:, 0])

      avg_joy = np.mean(emotion_scores[:, 1])
      std_joy = np.std(emotion_scores[:, 1])

      avg_optimism = np.mean(emotion_scores[:, 2])
      std_optimism = np.std(emotion_scores[:, 2])

      avg_sadness = np.mean(emotion_scores[:, 3])
      std_sadness = np.std(emotion_scores[:, 3])
      
      q4 = time.time()
      logging.info(f'Finish analyzing all in {q4 - q1}')
      return {
          
          'sent_avg_negative': avg_negative,
          'sent_std_negative': std_negative,

          'sent_avg_neutral': avg_neutral,
          'sent_std_neutral': std_neutral,

          'sent_avg_positive': avg_positive,
          'sent_std_positive': std_positive,

          'sent_avg_norm': avg_norm,
          'sent_std_norm': std_norm,

          'emo_avg_anger' : avg_anger,
          'emo_std_anger' : std_anger,

          'emo_avg_joy' : avg_joy ,
          'emo_std_joy' : std_joy,

          'emo_avg_optimism' : avg_optimism ,
          'emo_std_optimism' : std_optimism,

          'emo_avg_sadness' : avg_sadness ,
          'emo_std_sadness' : std_sadness,
      }

  def emotion_anal(self, collection_dict, emotion_tokenizer, model_emotion):
    tokenizer = emotion_tokenizer
    model = model_emotion.to("cuda")
    labels = self.get_label_for_task('emotion')
    return self.cal_stat_for_collection_with_labels(tokenizer, collection_dict, labels, model)

  def sentimental_anal(self, collection_dict, sentiment_tokenizer, model_sentiment):
    tokenizer = sentiment_tokenizer
    model = model_sentiment.to("cuda")
    labels = self.get_label_for_task('sentiment')
    return self.cal_stat_for_collection_with_labels(tokenizer, collection_dict, labels, model)

  def get_label_for_task(self, task):
    labels = []
    mapping_link = f"mapping/{task}/mapping.txt"
    with open (mapping_link, 'r', encoding='utf-8') as f:
        html = f.read().split("\n")
        csvreader = csv.reader(html, delimiter='\t')
        
    labels = [row[1] for row in csvreader if len(row) > 1]
    return labels

  def preprocess_tweets(self, tweets):
    tweets = [self.preprocess(tweet) for tweet in tweets]
    return tweets

  def convert_tweets_for_training(self, tokenizer, tweets):
    clean_tweets = self.preprocess_tweets(tweets)
    encoded_input = tokenizer(clean_tweets, return_tensors='pt', padding="max_length", truncation=True, max_length=self.MAX_LENGTH)
    return encoded_input

  def generate_softmax_score(self, batch, model):
    input_ids = torch.tensor(batch['input_ids']).to("cuda")
    attention_mask = torch.tensor(batch['attention_mask']).to("cuda")
    output = model(input_ids=input_ids, attention_mask=attention_mask)
    
    softmax_layer = torch.nn.Softmax()
    scores = softmax_layer(output[0]).cpu()
    scores = scores.detach().numpy()
    batch['scores'] = scores
    return batch

  def show_score_with_label(self, name, scores, labels):
    output_str = name
    for i in range(scores.shape[1]):
      score_list = scores[:, i]
      avg_score = np.mean(score_list)
      std_score = np.std(score_list)

      output_str += f"\n {labels[i]}: {avg_score} Std: {std_score}"
    return scores


  def cal_stat_for_collection_with_labels(self, tokenizer, collection, labels, model):
    tweets = collection['tweets']
    if not tweets:
      return f"{collection['name']} has empty list of tweets"
    processed_tweets = self.convert_tweets_for_training(tokenizer, tweets)

    dataset_tweets = datasets.Dataset.from_dict(processed_tweets)
    result = dataset_tweets.map(lambda batch: self.generate_softmax_score(batch, model), batched=True, batch_size=self.BATCH_SIZE)
    result_scores = np.array(result['scores'])
  
    return self.show_score_with_label(collection['name'], result_scores, labels)
