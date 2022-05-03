import pymongo
from pymongo import MongoClient
from src import Config
from src.utils.datawarehouse import BaseWarehouseClient


class AtlasClient(BaseWarehouseClient):

    def __init__(self, conn_str: str, db_name: str):
        self.client = MongoClient(conn_str)
        self.db = self.client[db_name]
        self.tweets = self.db[Config.mongo_collection['tweet']]
        self.users = self.db[Config.mongo_collection['users']]

    def _validate_tweet_data(self, tweet_data: dict) -> dict:
        assert tweet_data.id is not None, "Tweet must have an id"

        id_ = tweet_data.id
        del tweet_data['id']
        tweet_data['_id'] = id_

        return tweet_data

    def insert_tweet(self, tweet_data: dict):
        tweet_data = self._validate_tweet_data(tweet_data)
        self.tweets.update_one({"_id": tweet_data['_id']}, tweet_data, upsert=False)


    def insert_tweets(self, tweets: list):
        tweets_clean = [self._validate_tweet_data(t) for t in tweets]
        self.tweets.update_m


    def insert_user(self, user: dict): pass

    def insert_users(self, users: list): pass