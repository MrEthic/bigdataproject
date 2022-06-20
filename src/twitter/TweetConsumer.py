import faust
import logging
from src.utils.datalake import LocalDatalakeClient, LocalDatalakeConfig
from src.utils.logger import config_logger
from src import Config
from src.twitter.model import TweetText

log = config_logger(logging.getLogger(__name__))


# python -m src.twitter.TweetConsumer
# faust -A src.twitter.TweetConsumer worker -l info

app = faust.App('src.twitter.TweetConsumer', broker='kafka://localhost:9092')
election_raw = app.topic(Config.election_topic, value_serializer='json')
election_text = app.topic(Config.tweet_text_topic, value_type=TweetText)

datalake = LocalDatalakeClient(LocalDatalakeConfig())

@app.agent(election_raw)
async def election_raw_process(messages):
    async for data in messages:
        if data is not None:
            id = data['data']['id']
            filename = f"{id}.json"
            datalake.put_json(data, filename, 'raw', 'twitter')
            log.info(f'Tweet {id} consumed')
            #yield TweetText(text=data['data']['text'], id=id)
        else:
            log.info('No message received')

@app.agent(election_text)
async def election_raw_process(messages):
    async for data in messages:pass


if __name__ == '__main__':
    app.main()