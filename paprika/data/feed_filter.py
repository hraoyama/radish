import os
import sys
from datetime import datetime
import logging
from abc import ABC

print(os.getenv("RADISH_PATH"))
print(os.getenv("PAPRIKA_PATH"))

from enum import Enum


# from fetcher import HistoricalDataFetcher
# from fetcher import DataUploader


class FilterType(Enum):
    TIME = 'Time'
    VOLUME = 'Volume'


class FilterInterface(ABC):
    pass


class Filtration(object):
    def __init__(self):
        self.filters = []
    
    def add_filter(self, filter: FilterInterface):
        self.filters.append(filter)
    
    def __str__(self):
        return '\n'.join([str(f) for f in self.filters])


class FreqFilter(FilterInterface):
    def __init__(self, period, length):
        super().__init__()
        self.length = length
        self.period = period
    
    def __str__(self):
        type_of_length = "NoneType" if self.length is None else str(type(self.length))
        return " ".join([str(type(self.period)), str(self.period), type_of_length, str(self.length)])


class TimePeriod(Enum):
    DAY = 'D'
    WEEK = 'W'
    WEEKLY = 0
    HOUR = 'H'
    MINUTE = 'M'
    SECOND = 'S'
    MS = 'MS'
    CONTINUOUS = ''


class TimeFreqFilter(FreqFilter):
    def __init__(self, period, length=None):
        assert isinstance(period, TimePeriod)
        if length is None:
            assert period == TimePeriod.CONTINUOUS
        super().__init__(period, length)


if __name__ == "__main__":
    a = TimeFreqFilter(TimePeriod.SECOND, 30)
    b = TimeFreqFilter(TimePeriod.WEEK, 2)
    c = TimeFreqFilter(TimePeriod.CONTINUOUS)
    filt = Filtration()
    filt.add_filter(a)
    filt.add_filter(c)
    filt.add_filter(b)
    print(filt)