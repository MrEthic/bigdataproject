import os
from abc import ABC, abstractmethod


class BaseDatalakeConfig(ABC):

    @classmethod
    @property
    @abstractmethod
    def Layer(cls): raise NotImplementedError

    @classmethod
    @property
    @abstractmethod
    def DateFormat(cls): raise NotImplementedError

    @classmethod
    @property
    @abstractmethod
    def DataLakeRoot(cls): raise NotImplementedError

    @classmethod
    @property
    @abstractmethod
    def OSSep(cls): raise NotImplementedError


class LocalDatalakeConfig(BaseDatalakeConfig):
    Layer = ['raw', 'formatted', 'usage']
    DateFormat = "%Y%m%d"
    DataLakeRoot = os.sep + os.sep.join(['root', 'BIGDATAPROJECT', 'datalake'])
    OSSep = os.sep
