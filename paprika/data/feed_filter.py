import pandas as pd
import functools
import operator
from abc import ABC
from enum import Enum
from typing import Union, List


class FilterType(Enum):
    TIME = 'Time'
    VOLUME = 'Volume'


class FilterInterface(ABC):
    def apply(self, *args, **kwargs):
        pass


class Filtration(object):
    def __init__(self, filters: Union[FilterInterface, List[FilterInterface]] = None):
        self.filters = []
        if filters is not None:
            self.filters = [x for x in filters if x is not None] if isinstance(filter, list) else [filters]
    
    def add_filter(self, filter: FilterInterface):
        self.filters.append(filter)
    
    def __str__(self):
        return '\n'.join([str(f) for f in self.filters])
    
    def apply(self, *args, **kwargs):
        return [f.apply(*args, **kwargs) for f in self.filters]


class FreqFilter(FilterInterface):
    def __init__(self, period, length=1, starting=None, return_fixed_indices=False):
        super(FreqFilter, self).__init__()
        self.length = length
        self.period = period
        self.starting = starting
        self.return_fixed_indices = return_fixed_indices
    
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


class TimeIndexing(Enum):
    BEFORE = 1
    AFTER = 2
    BEFORE_AND_AFTER = 3


class TimeFreqFilter(FreqFilter):
    def __init__(self, period, length=None, starting=None, indexing=TimeIndexing.BEFORE, return_fixed_indices=False):
        assert isinstance(period, TimePeriod)
        if length is None:
            if not period == TimePeriod.CONTINUOUS:
                length = 1
        elif period == TimePeriod.CONTINUOUS:
            period = None
        super(TimeFreqFilter, self).__init__(period, length, starting, return_fixed_indices)
        self.time_indexing = indexing
    
    def apply(self, *args, **kwargs):
        dfi = args[0][0].index if not isinstance(args[0], pd.DataFrame) else args[0].index
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
        
        return_fixed_range = self.return_fixed_indices
        # comparing interval to retrieved time index:
        #  [used_range[2],
        #   dfi.asof(used_range[2]),
        #   dfi.to_series().truncate(before=used_range[2])[0],
        #   dfi[dfi.get_loc(used_range[2], method='bfill')]]
        if self.time_indexing == TimeIndexing.BEFORE:
            indices_to_return = sorted(list(set([dfi.asof(x) for x in used_range])))
        elif self.time_indexing == TimeIndexing.AFTER:
            indices_to_return = sorted(list(set([dfi[dfi.get_loc(x, method='bfill')] for x in used_range])))
        elif self.time_indexing == TimeIndexing.BEFORE_AND_AFTER:
            # https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-list-of-lists
            indices_to_return = sorted(list(set(functools.reduce(operator.iconcat,
                                                                 [[dfi.asof(x), dfi[dfi.get_loc(x, method='bfill')]]
                                                                  for x in used_range], []))))
        if return_fixed_range:
            indices_to_return = (indices_to_return, used_range)
        
        return indices_to_return
        
        pass
