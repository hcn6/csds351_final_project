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

def base36_to_10(in_str: str) -> int:
    return int(in_str, 36)

def get_response_data(params):
    url = "https://api.pushshift.io/reddit/search/comment"

    # print(time.time())
    response = requests.get(url, params=params)
    status = response.status_code

    while status != 200:
        time.sleep(60)
        response = requests.get(url, params=params)
        status = response.status_code
        
    return response.json()['data']
    
def crawl_comment_one_post(post_id: str, total_comment: int):
    params = {"subreddit": "csMajors", 
            "limit": 100, 
            "order": "desc", 
            "sort": "created_utc",
            "link_id": base36_to_10(post_id),
            # "q": "*",
            "before": int(time.time())
            }
    
    data = get_response_data(params=params)
    
    # logging.info(data)
    logging.info("Finish first request!")

    count = 0
    sub_id = set()
    # logging.info(data['data'][0])

    for sub in data:
        db.get_collection('reddit_comment').insert_one(sub)
        sub_id.add(sub['id'])
        # logging.info(f"{count}. Timestamp: {sub['utc_datetime_str']}")
        count += 1

    logging.info("Enter while loop!")
    while len(data) > 0 and len(sub_id) < total_comment:
        try:
            params['before'] = data[-2]['created_utc'] if len(data) > 1  else data[-1]['created_utc']
            # logging.info("Got new before!")
        except Exception as e:
            logging.warning(e)
            break
        else:   
            data = get_response_data(params=params)
            for sub in data:
                if sub['id'] not in sub_id:
                    db.get_collection('reddit_comment').insert_one(sub)
                    sub_id.add(sub['id'])
                # print(f"{count}. Timestamp: {sub['utc_datetime_str']}")
                count += 1
            logging.info(params['before'])
            time.sleep(1)
    logging.info(f"Total comments {len(sub_id)}")

if __name__ == '__main__':
    count = 13361
    print(f"COUNT {count}")
    collection = db.get_collection('reddit_post')
    logging.info(collection.count_documents({}))
    logging.info('Start crawling comment')

    all_posts = list(collection.find({}))[count:]
    for post in all_posts:
        count += 1
        if post['num_comments'] <= 1 or post['author'] == '[deleted]':
            continue
        logging.info(f"{count}. Post ID: {post['id']} Total comments: {post['num_comments']} reddit_url: {post['url']}'")
        crawl_comment_one_post(post['id'], post['num_comments'])
        logging.info(f"{count}. Finish crawling comment for post {count}. Title: {post['title']}")
