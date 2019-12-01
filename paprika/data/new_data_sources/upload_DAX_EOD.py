import pandas as pd
import numpy as np
import os

from paprika.data.data_type import DataType
from paprika.data.data_channel import DataChannel
from paprika.utils import utils

PATH = r'/Users/fangxia/Google Drive/stocks/eod'


def main():
    stocks = pd.read_csv(os.path.join(PATH, 'DAX30_20070102_20190211.csv'))
    stocks['Index'] = pd.to_datetime(stocks.Index)
    stocks.set_index('Index', inplace=True)

    symbols = set([symbol.split('.')[0].strip() for symbol in stocks.columns])
    symbols_ohlcv = [[f'{symbol}.DE.Open',
                      f'{symbol}.DE.High',
                      f'{symbol}.DE.Low',
                      f'{symbol}.DE.Close',
                      f'{symbol}.DE.Volume']
                     for symbol in symbols]
    for symbol_ohlcv in symbols_ohlcv:
        symbol = symbol_ohlcv[0].split('.')[0].strip()
        try:
            df = stocks[symbol_ohlcv]
        except KeyError:
            print(f'Can not find {symbol}')
            continue
        df.loc[:, 'Symbol'] = symbol
        df.rename(columns={f'{symbol}.DE.Open': 'Open',
                           f'{symbol}.DE.High': 'High',
                           f'{symbol}.DE.Low': 'Low',
                           f'{symbol}.DE.Close': 'Close',
                           f'{symbol}.DE.Volume': 'Volume'}, inplace=True)
        df.index.name = 'date'
        DataType.extend("EOD")
        table_name = DataChannel.name_to_data_type(f"DAX.{symbol}", DataType.EOD)
        DataChannel.upload(df, table_name,
                           is_overwrite=True,
                           arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME,
                           put_in_redis=False)
        print(f'Uploaded {table_name}')


if __name__ == "__main__":
    main()
