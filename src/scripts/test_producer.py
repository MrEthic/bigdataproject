from src.twitter import TweetProducer
import os

#python -m src.scripts.test_producer

if __name__ == '__main__':
    stream = TweetProducer(os.environ['TWITTER_BEARER'], wait_on_rate_limit=True)
    try:
        stream.start()
    except KeyboardInterrupt:
        stream.disconnect()
