from abc import ABC
import pandas as pd
from enum import Enum


class FilterType(Enum):
    TIME = 'Time'
    VOLUME = 'Volume'


class FilterInterface(ABC):
    def apply(self, *args, **kwargs):
        pass


class Filtration(object):
    def __init__(self):
        self.filters = []
    
    def add_filter(self, filter: FilterInterface):
        self.filters.append(filter)
    
    def __str__(self):
        return '\n'.join([str(f) for f in self.filters])
    
    def apply(self, *args, **kwargs):
        return [f.apply(args, kwargs) for f in self.filters]


# class TimeSpanFilter(FilterInterface):
#     def __init__(self, ):

class FreqFilter(FilterInterface):
    def __init__(self, period, length=1, starting=None):
        super(FreqFilter, self).__init__()
        self.length = length
        self.period = period
        self.starting = starting
    
    def __str__(self):
        type_of_length = "NoneType" if self.length is None else str(type(self.length))
        return " ".join([str(type(self.period)), str(self.period), type_of_length, str(self.length)])
    
    def apply(self, *args, **kwargs):
        pass


class TimePeriod(Enum):
    # https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-offset-aliases
    DAY = 'D'
    BUSINESS_DAY = 'B'
    WEEK = 'W'
    MONTH_END = 'M'
    BUSINESS_MONTH_END = 'BM'
    SEMI_MONTH_END = 'SM'
    QUARTER = 'Q'
    HOUR = 'H'
    BUSINESS_HOUR = 'BH'
    MINUTE = 'T'
    SECOND = 'S'
    MILLISECOND = 'L'
    MICROSECOND = 'U'
    CONTINUOUS = ''


class TimeFreqFilter(FreqFilter):
    def __init__(self, period, length=None, starting=None):
        assert isinstance(period, TimePeriod)
        if length is None:
            assert period == TimePeriod.CONTINUOUS
        super(TimeFreqFilter, self).__init__(period, length, starting)
    
    def apply(self, *args, **kwargs):
        dfi = args[0][0].index
        # it is possible that we get duplicated indices in
        # (that is OK, multiple data points at the same instance)
        # but we need unique data points when filtering
        dfi = dfi[~dfi.duplicated(keep='first')]
        # TODO: check that the latter did not unnecessarily clean indices in the original data
        if self.period == TimePeriod.CONTINUOUS:
            return dfi
        
        used_starting = self.starting if self.starting is not None else dfi[0]
        used_range = pd.date_range(used_starting,
                                   dfi[-1].to_pydatetime(),
                                   freq=f'{self.length}{self.period.value}')
        return sorted(list(set([dfi.asof(x) for x in used_range])))
        pass



