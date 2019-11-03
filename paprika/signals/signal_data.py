import os
import sys
import json
import jsonpickle
import numpy as np
import pandas as pd
from typing import List, Tuple
from collections import defaultdict

from paprika.core.base_signal import Signal


class SignalData:
    RESULTS_STRING = "RESULTS"

    def __init__(self, name: str, data: List[Tuple[str, pd.DataFrame]]):
        _data = defaultdict(pd.DataFrame)
        self._name = name
        if isinstance(data, dict):
            _data = data
        else:
            for key, value_frame in data:
                self.add(key, value_frame, ignore_index=False)

    def add(self, key_str, data_frame, ignore_index=False):
        self._data[key_str] = self._data[key_str].append(data_frame, ignore_index=ignore_index, sort=True)
        # pd.merge(self._data[key], value_frame, how='outer', left_index=True, right_index=True)
        self._data[SignalData.RESULTS_STRING] = self._data[SignalData.RESULTS_STRING].append(self._data[key_str],
                                                                                             sort=True)

    def __add__(self, other):
        if isinstance(other, SignalData):
            for key, value_frame in other._data:
                self.add(key, value_frame, ignore_index=False)
            return self
        else:
            raise ValueError(
                f'"+" operator expected instance of {str(self.__class__.__name__)} but received instance of {str(other.__name__)}')

    def get_indices(self):
        return self._data[SignalData.RESULTS_STRING].index

    def get_frame(self, key_str):
        return self._data[key_str]

    @property
    def name(self):
        return self._name

    @property
    def json_string(self):
        return json.dumps(self._data, indent=4)

    @classmethod
    def create_from_json_file(cls, json_file_name_to_use):
        if json_file_name_to_use.strip()[-4:].lower() == 'json' or os.path.isfile(json_file_name_to_use):
            with open(json_file_name_to_use, 'r') as json_file:
                created_object = jsonpickle.decode(str(json_file.read()))
                if isinstance(created_object, SignalData):
                    return created_object
                else:
                    return SignalData(created_object)
        else:
            raise ValueError(f'Not a valid json file name: {json_file_name_to_use}')

    @classmethod
    def create_from_json_string(cls, json_string):
        dict_string_to_use = json_string.strip()
        created_object = jsonpickle.decode(dict_string_to_use)
        if isinstance(created_object, SignalData):
            return created_object
        else:
            temp = SignalData()
            temp._data = created_object
            return temp

    @staticmethod
    def create_indexed_frame(orig_frame, dt_col="DateTime"):
        ts1 = orig_frame.copy()
        ts1 = ts1.sort_values(by=[dt_col])
        ts1 = ts1.reset_index(drop=True)
        ts1 = ts1.rename(columns={dt_col: 'date'})
        ts1.set_index('date', inplace=True)
        return ts1
