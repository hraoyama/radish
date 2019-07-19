import os
import sys
from datetime import datetime
import logging
from abc import ABC

print(os.getenv("RADISH_PATH"))
print(os.getenv("PAPRIKA_PATH"))


from enum import Enum
from fetcher import HistoricalDataFetcher
from fetcher import DataUploader


class FreqFilter(ABC):
    def __init__(self, period, length):
        self.length = length
        self.period = period

    def __str__(self):
        return str(type(self.period)), str(self.period), str(type(self.length)), str(self.length)
    
class TimePeriod(Enum):
    DAY = 'D'
    WEEK = 'W'
    HOUR = 'H'
    MINUTE = 'M'
    SECOND = 'S'
    MS = 'MS'
    CONTINUOUS = ''
    
class TimeFreqFilter(FreqFilter):
    def __init__(self, period, length = 1):
        assert isinstance(period, TimePeriod)
        super(period, length)


class Filter:
    
    def __init__(self):
        self.filter_dict = {}
        
        
if __name__ == "__main__":
    a = TimeFreqFilter(TimePeriod.SECOND, 30)
    print(a)
    