from abc import ABC, abstractmethod
from bisect import bisect


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


class Recorder(ABC):
    @abstractmethod
    def record(self, key, value):
        pass


class RecorderOffline(Recorder):
    def __init__(self):
        self._key_to_timeseries = {}

    def record(self, key, timestamp, value):
        if key not in self._key_to_timeseries:
            self._key_to_timeseries[key] = Timeseries()

        timeseries = self._key_to_timeseries[key]
        timeseries.append(timestamp, value)

    def get_dict(self):
        return self._key_to_timeseries.copy()


class RecorderOnline(Recorder):
    pass
