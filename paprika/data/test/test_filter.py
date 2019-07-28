from paprika.data.feed_filter import *
from datetime import datetime
import numpy as np

def test_filter():
    now = datetime.now()
    a = TimeFreqFilter(TimePeriod.SECOND, 30)
    b = TimeFreqFilter(TimePeriod.WEEK, 2)
    filt = Filtration()
    filt.add_filter(a)
    filt.add_filter(b)
    assert filt.filters[0].period == TimePeriod.SECOND
    assert filt.filters[0].length == 30
    assert filt.filters[1].period == TimePeriod.WEEK
    assert filt.filters[1].length == 2
    # print(filt)
    i = pd.date_range('2018-04-09 08:12:13.156895', periods=100, freq='5h')
    df = pd.DataFrame(np.random.randn(len(i)), index=i, columns=list('A'))
    i2 = pd.date_range('2018-04-12 16:15:00.798643', periods=20, freq='1h')
    a = [df.index.asof(x) for x in i2]
    assert df.shape == (100,1)
    # print(df.loc[a].shape)
    d = TimeFreqFilter(TimePeriod.WEEK, 2)
    filt2 = Filtration()
    filt2.add_filter(d)
    filt_match = filt2.apply(df)
    assert len(filt_match) == 1
    assert len(filt_match[0]) == 2
    assert isinstance(filt_match[0][1], pd.Timestamp)
    # print(filt_match)
    