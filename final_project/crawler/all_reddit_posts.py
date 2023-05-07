import praw
import re
import sys
from kafka import KafkaProducer
from stream_reddit import RedditStream
import json
import time
from psaw import PushshiftAPI
import requests
import pprint
from final_project.database import db
from final_project.utils import logging

url = "https://api.pushshift.io/reddit/search/submission"
params = {"subreddit": "csMajors", 
        "limit": 1000, 
        "order": "desc", 
        "sort": "created_utc",
        "before": int(time.time())}
# print(time.time())
data = requests.get(url, params=params).json()

count = 0
sub_id = set()
max_count = 100000

for sub in data['data']:
    db.get_collection('reddit_post').insert_one(sub)
    sub_id.add(sub['id'])
    logging.info(f"{count}. Title: {sub['title']}")
    count += 1

while len(data['data']) > 0:
    # print(len(data['data']))
    params['before'] = data['data'][-2]['created_utc']
    response = requests.get(url, params=params)
    status = response.status_code
    if status != 200:
        logging.warn(status)
        time.sleep(60)
    else:
        data = response.json()
        for sub in data['data']:
            db.get_collection('reddit_post').insert_one(sub)
            sub_id.add(sub['id'])
            print(f"{count}. Title: {sub['title']}")
            count += 1
        logging.info(params['before'])
        time.sleep(1)

    if count > max_count:
        logging.info(len(sub_id))
        break

# def get_reddit_posts(limit=1000, subreddit='csMajors', before=int(time.time())):
#     url = "https://api.pushshift.io/reddit/search/submission"
#     params = {"subreddit": subreddit, 
#             "limit": limit, 
#             "order": "desc", 
#             "sort": "created_utc",
#             "before": before}
    
#     response = requests.get(url, params=params)
#     if response.status_code == 200:
#         data = response.json()
#     else:
#         data = None
#     return data

