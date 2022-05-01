from typing import Type
from src.utils.datalake import BaseDatalakeConfig


class BaseDatalakeClient:

    def __init__(self, config: BaseDatalakeConfig):
        self.config = config
        self.root: str = str(config.DataLakeRoot)
        self.date_format: str = str(config.DateFormat)
        self.sep: str = str(config.OSSep)
        self.layers: list = list(config.Layer)
