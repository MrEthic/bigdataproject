from src.twitter import TweetProducer
from src import Config
import os

#python -m src.scripts.test_producer

if __name__ == '__main__':
    TWITTER_BEARER_TOKEN = Config.twitter_bearer
    stream = TweetProducer(bearer_token=TWITTER_BEARER_TOKEN, wait_on_rate_limit=True)
    try:
        stream.start()
    except KeyboardInterrupt:
        print("ByeBye")
        stream.disconnect()
