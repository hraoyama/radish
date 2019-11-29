import pandas as pd
import numpy as np
import os

from paprika.data.data_type import DataType
from paprika.data.data_channel import DataChannel
from paprika.utils import utils

PATH = r'/Users/fangxia/Google Drive/stocks/eod'


def main():
    stocks = pd.read_csv(os.path.join(PATH, 'HSI50_20070102_20190212.csv'))
    stocks['Index'] = pd.to_datetime(stocks.Index)
    stocks.set_index('Index', inplace=True)

    symbols = set([symbol.split('.')[0].strip() for symbol in stocks.columns])
    symbols_ohlcv = [[f'{symbol}.HK.Open',
                      f'{symbol}.HK.High',
                      f'{symbol}.HK.Low',
                      f'{symbol}.HK.Close',
                      f'{symbol}.HK.Volume']
                     for symbol in symbols]
    for symbol_ohlcv in symbols_ohlcv:
        symbol = symbol_ohlcv[0].split('.')[0].strip()
        try:
            df = stocks[symbol_ohlcv]
        except KeyError:
            print(f'Can not find {symbol}')
            continue
        df.loc[:, 'Symbol'] = symbol
        df.rename(columns={f'{symbol}.HK.Open': 'Open',
                           f'{symbol}.HK.High': 'High',
                           f'{symbol}.HK.Low': 'Low',
                           f'{symbol}.HK.Close': 'Close',
                           f'{symbol}.HK.Volume': 'Volume'}, inplace=True)
        df.index.name = 'date'
        DataType.extend("EOD")
        table_name = DataChannel.name_to_data_type(f"HK.{symbol}", DataType.EOD)
        DataChannel.upload(df, table_name,
                           is_overwrite=True,
                           arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME,
                           put_in_redis=False)
        print(f'Uploaded {table_name}')


if __name__ == "__main__":
    main()
