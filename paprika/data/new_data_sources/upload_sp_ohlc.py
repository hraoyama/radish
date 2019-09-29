import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt

from datetime import datetime
from paprika.data.data_type import DataType
from paprika.data.data_channel import DataChannel
from paprika.data.feed import Feed
from paprika.signals.buy_on_gap_signal import BuyOnGap
from paprika.utils import utils

PATH = r'../../../resources/data/'


def main():
    entry_zscore = 1
    lookback = 20  # for MA
    
    op = pd.read_csv(os.path.join(PATH, 'inputDataOHLCDaily_20120424_op.csv'))
    lo = pd.read_csv(os.path.join(PATH, 'inputDataOHLCDaily_20120424_lo.csv'))
    cl = pd.read_csv(os.path.join(PATH, 'inputDataOHLCDaily_20120424_cl.csv'))
    stocks = pd.read_csv(os.path.join(PATH, 'inputDataOHLCDaily_20120424_stocks.csv'))
    
    op['Var1'] = pd.to_datetime(op['Var1'], format='%Y%m%d')
    op.columns = np.insert(stocks.values, 0, 'date')
    op.set_index('date', inplace=True)
    
    lo['Var1'] = pd.to_datetime(lo['Var1'], format='%Y%m%d')
    lo.columns = np.insert(stocks.values, 0, 'date')
    lo.set_index('date', inplace=True)
    
    cl['Var1'] = pd.to_datetime(cl['Var1'], format='%Y%m%d')
    cl.columns = np.insert(stocks.values, 0, 'date')
    cl.set_index('date', inplace=True)
    
    # standard deviation is computed using close returns for the last 90 days
    vol_estimate = utils.returns_calculator(cl, 1).rolling(90).std().shift(1)
    # select stocks near the market open whose returns from their previous day's low to today's open are lower
    # than one standard deviation
    lo_shift1 = lo.shift(1).values
    
    buy_price = lo_shift1 * (1 + entry_zscore * vol_estimate.values)
    buy_price = pd.DataFrame(buy_price, index=lo.index)
    
    ret_gap = (op - lo_shift1) / lo_shift1
    ma = cl.shift(1).rolling(lookback).mean()
    
    DataType.extend("CLOSE_PRICE")
    table_namex = DataChannel.name_to_data_type("SP_CLOSE", DataType.CLOSE_PRICE)
    
    # even though uploading to redis only would seem quicker,
    # it is much slower in putting it from Redis to the permanent store because Redis reads are element-wise!
    # DataChannel.upload_to_redis(cl, table_namex)
    # DataChannel.upload_to_permanent(table_namex)
    DataChannel.upload(cl, table_namex,
                       is_overwrite=True,
                       arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME,
                       put_in_redis=False)
    print(f'Uploaded {table_namex}')
    
    DataType.extend("OPEN_PRICE")
    table_name1 = DataChannel.name_to_data_type("SP_OPEN", DataType.OPEN_PRICE)
    DataChannel.upload(op, table_name1,
                       is_overwrite=True,
                       arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME,
                       put_in_redis=False)
    print(f'Uploaded {table_name1}')

    DataType.extend("BUY_PRICE")
    table_name2 = DataChannel.name_to_data_type("SP_BUY", DataType.BUY_PRICE)
    DataChannel.upload(buy_price, table_name2,
                       is_overwrite=True,
                       arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME,
                       put_in_redis=False)
    print(f'Uploaded {table_name2}')

    DataType.extend("RETURN_GAP")
    table_name3 = DataChannel.name_to_data_type("SP_RETURN_GAP", DataType.RETURN_GAP)
    DataChannel.upload(ret_gap, table_name3,
                       is_overwrite=True,
                       arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME,
                       put_in_redis=False)
    print(f'Uploaded {table_name3}')

    DataType.extend("CLOSE_MA")
    table_name4 = DataChannel.name_to_data_type("SP_CLOSE_MA", DataType.CLOSE_MA)
    DataChannel.upload(ma, table_name4,
                       is_overwrite=True,
                       arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME,
                       put_in_redis=False)
    print(f'Uploaded {table_name4}')

if __name__ == "__main__":
    main()
