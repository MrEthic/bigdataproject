import faust
import logging
from src.utils.datalake import LocalDatalakeClient, LocalDatalakeConfig

log = logging.getLogger(__name__)


# python -m src.twitter.TweetConsumer
# faust -A src.twitter.TweetConsumer worker -l info

app = faust.App('src.twitter.TweetConsumer', broker='kafka://localhost:9092')
election_raw = app.topic('twitter.election.raw', value_serializer='json')

datalake = LocalDatalakeClient(LocalDatalakeConfig())

@app.agent(election_raw)
async def hello(messages):
    async for data in messages:
        if data is not None:
            print(data)
            log.debug(data)
            id = data.id
            filename = f"{id}.json"
            datalake.put_json(data, filename, 'raw', 'twitter')
        else:
            log.info('No message received')


if __name__ == '__main__':
    app.main()