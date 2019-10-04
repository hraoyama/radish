import pandas as pd
import numpy as np
import os
import sys
import re

from paprika.data.fetcher import HistoricalDataFetcher

from paprika.exchange.data_processor import DataProcessor
from paprika.data.data_channel import DataChannel, DataType

# DataProcessor("EUX.FDAX201709.Trade").between_time('15:59', '16:30'). \
#     time_freq(TimePeriod.BUSINESS_DAY).positive_price().extract_returns("Price", "LOG_RETURN", "LogReturn_Px").data
#
# pp(data3['2017-08-16':'2017-09-11'][["Price", "LogReturn_Px"]])


def get_return_series(ticker, data_type, between_times = None, time_freq = None, extract_returns_args=None):
    
    permanent_names = DataChannel.table_names(arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME)
    DataChannel.download()
    
    
    fetcher = HistoricalDataFetcher()
    pattern_list_str = HistoricalDataFetcher().generate_simple_pattern_list([ticker], data_type)
    
    