from paprika.data.data_channel import DataChannel
from paprika.data.feed_filter import TimeFreqFilter, TimePeriod
from paprika.exchange.data_processor import DataProcessor
from haidata.fix_colnames import fix_colnames
from haidata.extract_returns import extract_returns

from pprint import pprint as pp
from functools import partial


def custom_function(df):
    df[["L1_LOG_RET"]] = df[["Price"]].shift(1)
    return df


def duplicate_col(source_col_name, target_col_name, df):
    df[[target_col_name]] = df[[source_col_name]]
    return df


def test_data_processor():
    # to see what is available
    # print(DataChannel.table_names(arctic_source_name='mdb'))
    
    data = DataChannel.download("EUX.FDAX201709.Trade", arctic_source_name='mdb', string_format=False)
    pp(data.Price['2017-09-15 12:55:49.743080':'2017-09-15 13:00:00.866140'])
    
    z = DataProcessor(data) \
        (TimeFreqFilter(TimePeriod.MINUTE, 15)) \
        (extract_returns, {"COLS": "Price", "RETURN_TYPE": "LOG_RETURN"}).data
    pp(z.Price['2017-09-13 23:18:47.488475':'2017-09-15 00:18:44.655347'])
    
    # make source data available from the feeds
    DataProcessor.AVAILABLE_IN_FEEDS = True
    
    z2 = DataProcessor(data, table_name="I_WILL_ACCESS_THIS_LATER") \
        (TimeFreqFilter(TimePeriod.HOUR, 1)) \
        ("between_time", '08:30', '16:30') \
        (extract_returns, {"COLS": "Price", "RETURN_TYPE": "LOG_RETURN"}) \
        (fix_colnames, {"CASE": "upper"}).data
    pp(z2.PRICE['2017-09-13 23:55':'2017-09-14 11:00'])
    
    DataProcessor.AVAILABLE_IN_FEEDS = False
    
    z3 = DataProcessor("I_WILL_ACCESS_THIS_LATER") \
        ("between_time", '11:30', '14:00') \
        (custom_function).data
    pp(z3.L1_LOG_RET['2017-09-13 11:00':'2017-09-14 14:00'])
    pp(z3.Price['2017-09-13 11:00':'2017-09-14 14:00'])
    
    data3 = DataProcessor("EUX.FDAX201709.Trade") \
        ("between_time", '15:59', '16:30') \
        (TimeFreqFilter(TimePeriod.BUSINESS_DAY)) \
        (lambda x: x[x.Price > 0.0]) \
        (partial(duplicate_col, "Price", "LogReturn_Px")) \
        (extract_returns, {"COLS": "LogReturn_Px", "RETURN_TYPE": "LOG_RETURN"}).data
    
    pp(data3['2017-08-16':'2017-09-11'][["Price", "LogReturn_Px"]])
