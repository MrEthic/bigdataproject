
class Config:

    @classmethod
    @property
    def log_dir(cls):
        return '/home/bigdata/logs/bigdataproject/'

    @classmethod
    @property
    def twitter_bearer(cls):
        return 'AAAAAAAAAAAAAAAAAAAAAJjibwEAAAAA5TOqrJE7UTnitH0sy8hn4lbqCR4%3D9K9iMe8P8Dgcvok16IrL20VWr9BW3lbnJGiN4IBUsn1iQlzuzA'

    @classmethod
    @property
    def mongo_user(cls): return 'remote_worker'

    @classmethod
    @property
    def mongo_pwd(cls): return 'remote_worker'

    @classmethod
    @property
    def mongo_db(cls): return 'bigdataproject'

    @classmethod
    @property
    def mongo_collection(cls): return {'tweet': 'twitter.tweet', 'user': 'twitter.user'}

    @classmethod
    @property
    def mongo_conn(cls):
        return f'mongodb+srv://{cls.mongo_user}:{cls.mongo_pwd}@bddbd.ptwl0.mongodb.net/{cls.mongo_db}?retryWrites=true&w=majority'
