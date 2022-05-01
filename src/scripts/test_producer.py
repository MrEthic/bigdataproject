from src.twitter import TweetProducer
import os


stream = TweetProducer(os.environ['TWITTER_BEARER'], wait_on_rate_limit=True)
