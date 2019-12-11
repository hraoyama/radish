from paprika.data.data_processor import DataProcessor
from paprika.data.feed import TimePeriod, TimeFreqFilter

from haidata.fix_colnames import fix_colnames

from pprint import pprint as pp
from datetime import datetime
import matplotlib.pyplot as plt

import numpy as np


def test_data_processor_convenience_interface():
    
    data = DataProcessor("EUX.FDAX201709.Trade").time_freq(TimePeriod.MINUTE, 15). \
        extract_returns("Price").index('2017-09-13 23:18:47.488475', '2017-09-15 00:18:44.655347').data
    pp(data.head(5))
    
    z2 = DataProcessor(data, table_name="I_WILL_ACCESS_THIS_LATER").time_freq(TimePeriod.HOUR, 1). \
        between_time('08:30', '16:30').extract_returns()(fix_colnames, {"CASE": "upper"}).data
    pp(z2.PRICE['2017-09-13 23:55':'2017-09-14 11:00'])
    
    z3 = DataProcessor("I_WILL_ACCESS_THIS_LATER").between_time('11:30', '14:00'). \
        shift_to_new_column("L1_LOG_RET", "PRICE", 1).data
    
    pp(z3.L1_LOG_RET['2017-09-13 11:00':'2017-09-14 14:00'])
    pp(z3.PRICE['2017-09-13 11:00':'2017-09-14 14:00'])
    
    data3 = DataProcessor("EUX.FDAX201709.Trade").between_time('15:59', '16:30'). \
        time_freq(TimePeriod.BUSINESS_DAY).positive_price().extract_returns("Price", "LOG_RETURN", "LogReturn_Px").data
    
    pp(data3['2017-08-16':'2017-09-11'][["Price", "LogReturn_Px"]])
    
    data = DataProcessor("EUX.FDAX201709.Trade").index('2017-06-01 08:00', '2017-06-10 08:00'). \
        between_time('08:15', '16:30').positive_price(). \
        summarize_intervals(TimeFreqFilter(TimePeriod.MINUTE, 5, starting=datetime(2017, 6, 1, 8, 15, 0)),
                            [DataProcessor.first, np.max, np.min, DataProcessor.last, np.median, np.mean, np.std],
                            "Price"). \
        rename_columns(['amax', 'amin', 'mean', 'median', 'first', 'last', 'std'],
                       ['HIGH', 'LOW', 'MEAN', 'MEDIAN', 'OPEN', 'CLOSE', 'STD']).data
    
    pp(data['2017-06-09 12:00':'2017-06-09 13:00'])
    pp(data.HIGH - data.LOW)
    pp(data.columns.values)
    
    data2 = DataProcessor("EUX.FDAX201709.Trade").index('2017-06-01 08:00', '2017-06-10 08:00'). \
        between_time('08:15', '16:30').positive_price(). \
        summarize_intervals(TimeFreqFilter(TimePeriod.MINUTE, 15, starting=datetime(2017, 6, 1, 8, 15, 0)),
                            [DataProcessor.first, np.max, np.min, DataProcessor.last, np.median, np.mean, np.std],
                            "Price"). \
        rename_columns(['amax', 'amin', 'mean', 'median', 'first', 'last', 'std'],
                       ['HIGH', 'LOW', 'MEAN', 'MEDIAN', 'OPEN', 'CLOSE', 'STD']). \
        extract_returns(column_name="MEAN", return_type="LOG_RETURN", new_column_name="LogReturn_MEAN"). \
        extract_returns(column_name="STD", return_type="LOG_RETURN", new_column_name="LogReturn_STD"). \
        shift_to_new_column('F1_LogReturn_MEAN', 'LogReturn_MEAN', -1). \
        shift_to_new_column('F1_LogReturn_STD', 'LogReturn_STD', -1) \
        (lambda x: x[~np.isnan(x.LogReturn_STD) & ~np.isnan(x.STD) & ~np.isnan(x.F1_LogReturn_STD)]).data
    
    pp(data2.columns.values)
    pp(data2.head(10))
    
    plt.scatter(data2.STD.values[data2.STD > 0.0], data2.F1_LogReturn_MEAN.values[data2.STD > 0.0], alpha=0.5)
    plt.title('Scatter plot of derived data at intervals')
    plt.xlabel('data2.STD')
    plt.ylabel('data2.L1_LogReturn_MEAN')
    plt.show()
