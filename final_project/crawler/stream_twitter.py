import tweepy

# Set up Twitter API v2 credentials
bearer_token = "AAAAAAAAAAAAAAAAAAAAAARDZgEAAAAAsCsxn64ZC1wL2It2K4QZ%2Fiw7YiA%3D0IXIFaYm9aRYdPgjNudkVK2OifOe9Jxh0oraO4ZCC9hHMwsjL7"
# TOKEN = "AAAAAAAAAAAAAAAAAAAAAARDZgEAAAAAsCsxn64ZC1wL2It2K4QZ%2Fiw7YiA%3D0IXIFaYm9aRYdPgjNudkVK2OifOe9Jxh0oraO4ZCC9hHMwsjL7"

class PrintListener(tweepy.StreamingClient):
    def __init__(self, bearer_token, company):
        super().__init__(bearer_token)
        self.company = company
        self.samples = []

    def on_tweets(self, tweet):
        print("aaa")

    def on_status(self, status):
        print(status.text)

    def on_data(self, data):
        self.samples.append(data)
        print(data.decode('utf-8'))

    def on_error(self, status):
        print(f"Error with status code {status}")

    def on_connect(self):
        print("Connected!")


# Set up the stream
listener = PrintListener(bearer_token, "dropbox")

for rule in listener.get_rules().data:
    listener.delete_rules(rule.id)

stream_rules = tweepy.StreamRule("Amazon (interview OR layoff OR engineer) lang:en -is:retweet -has:mentions")

listener.add_rules(stream_rules)

print(listener.get_rules())
# while len(listener.samples) <= 10:
#     print(len(listener.samples))
#     listener.filter()
# print(listener.samples)
# Start the stream and filter for tweets containing the word "Python"
listener.filter(tweet_fields=["created_at", "lang"],
                expansions=["author_id"],
                user_fields=["username", "name"],)
