from src.twitter import TweetProducer
import os


if __name__ == '__main__':
    stream = TweetProducer(os.environ['TWITTER_BEARER'], wait_on_rate_limit=True)
    try:
        stream.start()
    except KeyboardInterrupt:
        stream.disconnect()
