import time
import json
from utils import logging
import requests
import re
from selenium import webdriver
from fake_useragent import UserAgent
from selenium.webdriver.common.by import By
import pytz
from datetime import datetime, timedelta
from dateutil import parser

MAX_TWEETS = 50
TOKEN = "AAAAAAAAAAAAAAAAAAAAAARDZgEAAAAAsCsxn64ZC1wL2It2K4QZ%2Fiw7YiA%3D0IXIFaYm9aRYdPgjNudkVK2OifOe9Jxh0oraO4ZCC9hHMwsjL7"
class Crawler:
    def api_call(self, endpoint, headers, params):
        MAX_TIME = 150
        current_time = 1
        while True:
            try:
                response = requests.get(endpoint, headers=headers, params=params)

                if response.status_code == 200:
                    data = response.json()
                    return data
                else:
                    raise ValueError(response)

            except Exception as e:
                logging.error(f'Error found: {e}, {endpoint}')
                if current_time >= MAX_TIME:
                    raise ValueError(f"""
                    Cannot get data from {endpoint}
                    Headers: {headers}
                    Params: {params}
                    """)
                logging.info(f'Retry after {current_time}s.')
                time.sleep(current_time)
                current_time *= 2

    def crawl_twitter(self, url):
        start = time.time()
        pattern =  '^https?:\/\/(?:www\.)?twitter\.com\/(?:#!\/)?@?([^/?#]*)(?:[?#].*)?$'
        url = url.strip()
        username = re.match(pattern, url).group(1)

        username_endpoint = f'https://api.twitter.com/2/users/by/username/{username}'
        
        headers = {"Authorization": f"Bearer {TOKEN}"}
        
        logging.info(f'Collection name: {username}')

        user_params = {
            'user.fields': 'public_metrics,description'
        }

        api_call_res = self.api_call(endpoint=username_endpoint, headers=headers, params=user_params)
        
        user_data = api_call_res['data']
        user_id = user_data['id']

        # timeline_endpoint = f'https://api.twitter.com/2/users/{user_id}/tweets'
        search_endpoint = f'https://api.twitter.com/2/tweets/search/recent'
        
        retweets_data = []
        pagination_token = None

        while True:
            retweets_params = {
                'query' : f'@{username}',
                'tweet.fields' : 'public_metrics,created_at'
            }
            if pagination_token:
                retweets_params['pagination_token'] = pagination_token
            
            response_data = self.api_call(search_endpoint, headers = headers, params=retweets_params)
            if response_data == None:
                TOKEN = self.TOKEN_LIST[self.token_idx]
                headers = {"Authorization": f"Bearer {TOKEN}"}
                continue

            retweet_split_data = response_data.get('data', [])
            meta_data = response_data['meta']
            

            retweets_data = retweets_data + retweet_split_data

            global MAX_TWEETS
            if 'next_token' in meta_data and len(retweets_data) < MAX_TWEETS:
                pagination_token = meta_data['next_token']
            else:
                break

        end = time.time()
        logging.info(f'Finished {username} retweets in {end - start}!')
        
        return {
            'collection_data': user_data,
            'retweets_data': retweets_data,
        }
    
crawler = Crawler()
crawler.crawl_twitter('https://twitter.com/FamousFoxFed')