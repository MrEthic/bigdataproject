from tweepy import StreamingClient, StreamRule
from kafka import KafkaConsumer, KafkaProducer
import base64
import logging
from src.utils.logger import config_logger


logger = config_logger(logging.getLogger(__name__))

class TweetProducer(StreamingClient):

    RULE = StreamRule(
        value="-is:retweet legislatives2022",
        tag="legislatives2022 no retweets"
    )

    def __init__(self, bootstrap_server: str = 'localhost:9092', topic_name: str = 'twitter.election.raw', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kafka_producer = KafkaProducer(bootstrap_servers=bootstrap_server)
        self.topic_name = topic_name

    def send(self, data):
        try:
            self.kafka_producer.send(self.topic_name, data.decode('utf-8').encode('utf-8')).get(timeout=10)
        except Exception:
            b64_data = base64.b64encode(str(data))
            logger.exception(f"Error while sending data to topic {self.topic_name}\n\t{b64_data}")

    def on_data(self, data):
        self.send(data)

    def on_error(self, status):
        logger.error(f"Error receiving data from Twitter Stream, status: {status}")

    def start(self):
        self.add_rules(TweetProducer.RULE)
        self.filter(expansions="author_id",
              tweet_fields="attachments,author_id,created_at,entities,geo,lang,possibly_sensitive,referenced_tweets,source",
              user_fields="created_at,profile_image_url,description,entities,verified,url")
