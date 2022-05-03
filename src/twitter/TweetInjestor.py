from src.utils.datalake import BaseDatalakeClient
from src.utils.datawarehouse import BaseWarehouseClient


class TweetInjestor:

    def __init__(self, datalake: BaseDatalakeClient, warehouse: BaseWarehouseClient):
        self.lake = datalake
        self.wh = warehouse
