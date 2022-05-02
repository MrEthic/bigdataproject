from src.twitter import TweetProducer
import os

#python -m src.scripts.test_producer

if __name__ == '__main__':
    TWITTER_BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAJjibwEAAAAA5TOqrJE7UTnitH0sy8hn4lbqCR4%3D9K9iMe8P8Dgcvok16IrL20VWr9BW3lbnJGiN4IBUsn1iQlzuzA'
    stream = TweetProducer(bearer_token=TWITTER_BEARER_TOKEN, wait_on_rate_limit=True)
    try:
        stream.start()
    except KeyboardInterrupt:
        print("ByeBye")
        stream.disconnect()
