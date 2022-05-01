import faust
import logging
from asyncio import sleep
from datetime import datetime
from src.utils.datalake import LocalDatalakeClient, LocalDatalakeConfig

log = logging.getLogger(__name__)


# python -m src.twitter.TweetConsumer

app = faust.App('twitterElection', broker='kafka://localhost:9092')
election_raw = app.topic('twitter.election.raw', value_type=bytes)

datalake = LocalDatalakeClient(LocalDatalakeConfig())

@app.agent(election_raw)
async def hello(messages):
    async for message in messages:
        if message is not None:
            data = dict(message)
            id = data.data.id
            filename = f"{id}.json"
            datalake.put_json(dict(message), filename, 'raw', 'twitter')
        else:
            log.info('No message received')


if __name__ == '__main__':
    app.main()