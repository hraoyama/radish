import pandas as pd
import os

from paprika.data.data_type import DataType
from paprika.data.data_channel import DataChannel

PATH = r'../../../resources/data/'


def main():
    tckr1 = 'GLD'
    tckr2 = 'GDX'
    ts1 = pd.read_excel(os.path.join(PATH, '{}.xls'.format(tckr1)), usecols=[0, 6])
    ts1 = ts1.sort_values(by=['Date'])
    ts1 = ts1.reset_index(drop=True)

    ts2 = pd.read_excel(os.path.join(PATH, '{}.xls'.format(tckr2)), usecols=[0, 6])
    ts2 = ts2.sort_values(by=['Date'])
    ts2 = ts2.reset_index(drop=True)

    ts = pd.merge(ts1, ts2, how='inner', on=['Date'])
    ts.columns = ['date', 'GLD', 'GDX']
    ts.set_index('date', inplace=True)
    
    DataType.extend("EOD_PRICE")
    table_name = DataChannel.name_to_data_type("GOLD_GDX", DataType.EOD_PRICE)
    DataChannel.upload(ts, table_name)
    
    # following stores it in DB permanently - only do this if you are sure you need to keep this data
    # usage of this data happens in test_signal_close_px_cointegration.py
    DataChannel.upload_to_permanent(table_name)

    # clear out any feeds in the feeds cache AND in Redis!
    DataChannel.clear_all_feeds()


if __name__ == "__main__":
    main()
