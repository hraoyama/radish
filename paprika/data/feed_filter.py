import os
from datetime import datetime
from abc import ABC
import pandas as pd
import numpy as np

print(os.getenv("RADISH_PATH"))
print(os.getenv("PAPRIKA_PATH"))

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
        super().__init__()
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
        super().__init__(period, length, starting)
    
    def apply(self, *args, **kwargs):
        df = args[0][0]
        assert isinstance(df, pd.DataFrame)
        if self.period == TimePeriod.CONTINUOUS:
            return df.index
        used_starting = self.starting if self.starting is not None else df.index[0]
        used_range = pd.date_range(used_starting, df.index[-1].to_pydatetime(),
                                   freq=f'{self.length}{self.period.value}')
        return sorted(list(set([df.index.asof(x) for x in used_range])))
        pass


if __name__ == "__main__":
    now = datetime.now()
    a = TimeFreqFilter(TimePeriod.SECOND, 30)
    b = TimeFreqFilter(TimePeriod.WEEK, 2)
    c = TimeFreqFilter(TimePeriod.CONTINUOUS)
    filt = Filtration()
    filt.add_filter(a)
    filt.add_filter(b)
    print(filt)
    i = pd.date_range('2018-04-09 08:12:13.156895', periods=100, freq='5h')
    df = pd.DataFrame(np.random.randn(len(i)), index=i, columns=list('A'))
    i2 = pd.date_range('2018-04-12 16:15:00.798643', periods=20, freq='1h')
    a = [df.index.asof(x) for x in i2]
    print(df.loc[a].shape)
    d = TimeFreqFilter(TimePeriod.WEEK, 2)
    filt2 = Filtration()
    filt2.add_filter(d)
    filt_match = filt2.apply(df)
    print(filt_match)


#
# i = pd.date_range('2018-04-09 08:12:13.156895', periods=4, freq='1D')
# ts = pd.DataFrame({'A': [1, 2, 3, 4]}, index=i)
# ts.first('2d')
# ts.first('20s')
# ts.first(relativedelta(day=11))
# import numpy as np
# dates = pd.date_range('20130101',periods=20, freq='1D')
# df = pd.DataFrame(np.random.randn(len(dates)),index=dates,columns=list('A'))
# dates2  = pd.date_range('20130104', periods=60, freq='6h')
# df.first(dates2[0])
# df.index.asof(dates2[0])
# df = pd.DataFrame(np.random.randn(6),index=dates,columns=list('A'))
# df.first(pd.DateOffset(days=3))
# df.last(pd.DateOffset(days=3))
# ts.at_time('12:00')
# ts.between_time()

# idx[idx.duplicated()].unique()
# df.loc[df.index[df.index.duplicated()].unique()]  # .to_csv('C:/Temp/test.csv')

# df.shape
# df.loc[df.index[df.index.duplicated()].unique()].shape
# df.loc[df.index[~df.index.duplicated()]].shape
