from abc import ABC, abstractmethod
from bisect import bisect
import pandas as pd


class Timeseries(object):
    def __init__(self):
        self.timestamps = []
        self.values = []
    
    def append(self, timestamp, value):
        if self.timestamps:
            if timestamp < self.timestamps[-1]:
                raise Exception(
                    'The timestamp appended ({}) is earlier than the latest timestamp ({}) in the'
                    ' time series.'.format(timestamp, self.timestamps[-1]))
            elif value != self.values[-1] and timestamp == self.timestamps[-1]:
                raise Exception(
                    'The timestamp appended ({}) is equal to the latest timestamp ({}), but'
                    ' with a different value - new value: {}, old value: {}'.
                        format(timestamp, self.timestamps[-1], value,
                               self.values[-1]))
        
        self.timestamps.append(timestamp)
        self.values.append(value)
    
    def get_value(self, timestamp):
        pos = bisect(self.timestamps, timestamp) - 1
        return self.values[pos]
    
    def items(self):
        return zip(self.timestamps, self.values)
    
    def to_dataframe(self, column_name='value'):
        df = pd.DataFrame({'date': pd.to_datetime(self.timestamps), column_name: self.values})
        df.set_index('date', inplace=True)
        return df


class Recorder(ABC):
    @abstractmethod
    def record(self, key, value):
        pass


class RecorderOffline(Recorder):
    def __init__(self, key_column=None, timestamp_column=None, value_column=None):
        self._key_to_time_series = {}
        self.key_column = key_column
        self.value_column = value_column
        self.timestamp_column = timestamp_column  # make it the index if it is None
    
    def record(self, *args):  # key, timestamp, value):
        key = None
        timestamp = None
        value = None
    
        if not isinstance(args[0], pd.DataFrame):
            key = args[0]
            timestamp = args[1]
            value = args[2]
        else:
            
            pass
        
        if key not in self._key_to_time_series:
            self._key_to_time_series[key] = Timeseries()

        time_series = self._key_to_time_series[key]
        time_series.append(timestamp, value)

    def get_dict(self):
        return self._key_to_time_series.copy()


class RecorderOnline(Recorder):
    pass
