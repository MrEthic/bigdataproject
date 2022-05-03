from src.utils.datawarehouse import AtlasClient
from src.utils.datalake import LocalDatalakeClient, LocalDatalakeConfig
from src import Config
from datetime import date


client = AtlasClient(Config.mongo_conn, Config.mongo_db)
lake = LocalDatalakeClient(LocalDatalakeConfig())

today = date.today()


files = lake.list_day('raw', 'group', today)

print(files)

