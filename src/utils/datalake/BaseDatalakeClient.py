import abc
from typing import Union
from datetime import date
from src.utils.datalake import BaseDatalakeConfig


class BaseDatalakeClient(metaclass=abc.ABCMeta):


    def __init__(self, config: BaseDatalakeConfig):
        self.config = config
        self.root: str = str(config.DataLakeRoot)
        self.date_format: str = str(config.DateFormat)
        self.sep: str = str(config.OSSep)
        self.layers: list = list(config.Layer)


    @abc.abstractmethod
    def put_json(self, object_: dict, filename: str, layer: str, group: str, explicit_date: Union[str, date]=None): pass

    @abc.abstractmethod
    def list_day(self, layer: str, group: str, datedir: Union[str, date], extension: str = 'json') -> list: pass

    @abc.abstractmethod
    def get_json_as_dict(self, layer: str, group: str, datedir: Union[str, date], tid: str) -> Union[None, dict]: pass
