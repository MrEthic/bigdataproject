from typing import Literal, Union, Type
import json
from datetime import date, datetime
from pathlib import Path
import logging
import re
import json
import os, os.path
from src.utils.datalake import LocalDatalakeConfig, BaseDatalakeClient


class LocalDatalakeClient(BaseDatalakeClient):

    def __init__(self, config: LocalDatalakeConfig):
        super().__init__(config)
        self.ensure_path(self.root)

    def _validate_date(self, str_date: str) -> date:
        """Validate date string against expected fromat from conifg. Return a date object or raise ValueError"""
        try:
            date_ = datetime.strptime(str_date, self.date_format)
        except ValueError:
            raise ValueError(f"Explicit date must match the format {self.date_format}")

        return date_

    def ensure_path(self, path: str) -> str:
        """Ensure if path exists in local fs and creates it if not."""
        assert self.sep.join(path.split(self.sep)) == path, "Separator must be the same as defined in config"
        Path(path).mkdir(parents=True, exist_ok=True)
        return path

    def put_json(self, object_: dict, filename: str, layer: str, group: str, explicit_date: Union[str, date]=None):
        assert layer in self.layers, f"Layer must be one of the following {self.layers}"

        #TODO: check group value ?
        group = group.lower()
        today = date.today()

        if explicit_date is not None:
            if type(explicit_date) == str:
                explicit_date = self._validate_date(explicit_date)

            if explicit_date > today:
                raise ValueError("Cannot put object with greater date than today")

        date_ = today if explicit_date is None else explicit_date
        date_ = date_.strftime(self.date_format)

        path = self.sep.join([
            self.config.DataLakeRoot,
            layer,
            group,
            date_
        ])
        path = self.ensure_path(path)
        file_path = self.sep.join([path, filename])

        with open(file_path, 'w') as f:
            json.dump(object_, f, ensure_ascii=False)

    def get_filename(self, layer: str, group: str, datedir: Union[str, date], tid: str, extension: str = 'json') -> str:
        assert layer in self.layers, f"Layer must be one of the following {self.layers}"

        if type(datedir) == str:
            datedir = self._validate_date(datedir)

        date_ = datedir.strftime(self.date_format)
        filename = self.sep.join([
            self.root,
            layer,
            group,
            date_,
            tid + extension
        ])
        return filename

    def list_day(self, layer: str, group: str, datedir: Union[str, date], extension: str = 'json') -> list:
        assert layer in self.layers, f"Layer must be one of the following {self.layers}"

        if type(datedir) == str:
            datedir = self._validate_date(datedir)

        date_ = datedir.strftime(self.date_format)

        dir_ = self.sep.join([
            self.root,
            layer,
            group,
            date_
        ])
        files = [
            f for f in os.listdir(dir_)
            if os.path.isfile(self.sep.join([dir_, f]))
            and f.endswith(extension)
        ]
        return files

    def get_json_as_dict(self, layer: str, group: str, datedir: Union[str, date], tid: str) -> Union[None, dict]:
        filename = self.get_filename(layer, group, datedir, tid)
        if not os.path.isfile(filename):
            return None

        with open(filename, 'r') as f:
            data = json.load(f)

        return data

