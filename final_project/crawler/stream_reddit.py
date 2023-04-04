import praw
import pprint
from final_project.database import db
from final_project.clean_text import twitter_clean_text as clean_text
from pymongo import MongoClient


class RedditStream():
    def __init__(self, subreddit):
        self.reddit = praw.Reddit('bot1')
        self.db_name = 'raw_data'
        self.collection = 'reddit'
        self.subreddit = subreddit
        self.stream_instance = self.reddit.subreddit(self.subreddit)

    def get_stream_instance(self):
        return self.stream_instance
    
    def sample(self):
        num_comments_collected = 0
        # comment_stream = r.subreddit(subreddit)
        for comment in self.stream_instance.stream.comments():
            num_comments_collected += 1
            pprint.pprint(vars(comment))
            self.insert_comment(self.process_before_insert_db(comment))
            print(
                f"Inserted comment {num_comments_collected}: {comment.id} to db!")

    def insert_comment(self, comment):
        collection = db.get_collection(
            db_name=self.db_name, collection_name=self.collection)
        try:
            collection.insert_one(comment)
        except Exception as e:
            print(e)

    def process_before_insert_db(self, comment):
        return {
            "_id": comment.id,
            "raw_text": comment.body,
            "created_at": comment.created,
            "permalink": comment.permalink,
            "author_name": comment.author.name,
            "author_id": comment.author.id,
            "subreddit": comment.subreddit_name_prefixed
        }

# # Connect to your MongoDB database
# client = MongoClient('mongodb://localhost:27017/')
# db = client['mydatabase']
# collection = db['mycollection']

# # Define your list of sentences
# sentences = [
#     'The quick brown fox jumps over the lazy dog.',
#     'The quick brown fox jumps over the quick dog.',
#     'A quick brown dog jumps over the lazy fox.'
# ]

# Define the map function


# def map_function():
#     for sentence in sentences:
#         for word in sentence.split():
#             yield {'word': word.lower(), 'count': 1}

# # Define the reduce function


# def reduce_function(key, values):
#     return {'word': key, 'count': sum(values)}


# # Run the MapReduce job
# result = db.command('mapReduce', 'mycollection',
#                     map=map_function, reduce=reduce_function, out='myoutput')

# # Insert the output into the MongoDB collection
# for doc in db.myoutput.find():
#     collection.insert_one(doc['value'])

#     def clean_reddit_text(self, comment):
#         comment = clean_text.preprocess_string(comment)
#         return comment

# stream = RedditStream('csMajors')

# stream.sample()
