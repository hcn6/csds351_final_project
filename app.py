from flask import Flask, jsonify
import pprint
from pymongo import MongoClient
import json
from bson import ObjectId, json_util
from datetime import datetime, timedelta

app = Flask(__name__)

# MongoDB connection config
mongodb_uri = 'mongodb+srv://dxn183:P4TnUn0wuNZqztQx@cluster0.7tqovhs.mongodb.net/'
client = MongoClient(mongodb_uri)

# fetch company with multiple alias
def get_single_company_api(company):
    db = client.reddit_data.company_sentiment_day_interval
    cutoff = datetime(2022, 8, 1, 0, 0, 0)
    intervals = [
        ('day', cutoff - timedelta(days=1)),
        ('week', cutoff - timedelta(weeks=1)),
        ('month', cutoff - timedelta(days=30)),
        ('year', cutoff - timedelta(days=365)),
        ('allTime', None),
    ]

    rating = {}
    for interval_name, interval_start in intervals:
        if interval_start is None:
            match = {"company": company}
        else:
            match = {
                "company": company, 
                "utc_timestamp": {"$gte": interval_start.timestamp()}
            }

        pipeline = [
            {"$match": match},
            {"$group": {
                "_id": None,
                "positive_avg": {"$avg": "$positive"},
                "neutral_avg": {"$avg": "$neutral"},
                "negative_avg": {"$avg": "$negative"},
                "normalize_avg": {"$avg": "$normalize_score"},
            }},
        ]

        result = db.aggregate(pipeline)
        result = list(result)
        if result:
            result = result[0]
            rating[interval_name] = [
                {"name": "Positive", "percent": result["positive_avg"]},
                {"name": "Neutral", "percent": result["neutral_avg"]},
                {"name": "Negative", "percent": result["negative_avg"]},
            ]

    company_sentiment = {
        "companyName": company,
        "rating": rating,
        "normalize": result["normalize_avg"] if result else None,
    }

    return json.loads(json_util.dumps(company_sentiment))
        

# searching a company
@app.route('/api/<company>/', methods=['GET'])
def fetch_single_company(company):
    return get_single_company_api(company=company)

# retrieving all companies
@app.route('/api/', methods=['GET'])
def get_all_companies():
    # comp_list = [
    #     ["dropbox"],
    #     ["snap"],
    #     ["aws", "amazon"],
    #     ["roku"],
    #     ["mongodb"],
    #     ["oci", "oracle"],
    #     ["servicenow"],
    #     ["snowflake"],
    #     ["paypal"],
    #     ["msft", "microsoft"]
    # ]

    comp_list = [
        "dropbox", "snap", "aws", "roku", "mongodb",
        "oci", "servicenow", "snowflake", "paypal", "microsoft"
    ]

    result = []
    for company in comp_list:
        result.append(get_single_company_api(company=company))
    
    return result

if __name__ == '__main__':
    app.run()