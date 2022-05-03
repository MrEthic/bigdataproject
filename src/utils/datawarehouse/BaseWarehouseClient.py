import abc


class BaseWarehouseClient(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def insert_tweet(self, tweet: dict) -> bool: pass

    @abc.abstractmethod
    def insert_tweets(self, tweets: list) -> bool: pass

    @abc.abstractmethod
    def insert_user(self, user: dict) -> bool: pass

    @abc.abstractmethod
    def insert_users(self, users: list) -> bool: pass