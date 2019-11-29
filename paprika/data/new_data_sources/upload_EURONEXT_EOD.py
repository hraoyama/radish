import pandas as pd
import numpy as np
import os

from paprika.data.data_type import DataType
from paprika.data.data_channel import DataChannel
from paprika.utils import utils

PATH = r'/Users/fangxia/Google Drive/stocks/eod'


def main():
    stocks = pd.read_csv(os.path.join(PATH, 'EURONEXT_2018_data.csv'))
    stocks['Date'] = pd.to_datetime(stocks.Date)
    stocks.set_index('Date', inplace=True)

    symbols = set([symbol.split('-')[0].split('/')[1].strip() for symbol in stocks.columns])
    symbols_ohlcv = [[f'EURONEXT/{symbol} - Open',
                      f'EURONEXT/{symbol} - High',
                      f'EURONEXT/{symbol} - Low',
                      f'EURONEXT/{symbol} - Last',
                      f'EURONEXT/{symbol} - Volume',
                      f'EURONEXT/{symbol} - Turnover']
                     for symbol in symbols]
    for symbol_ohlcv in symbols_ohlcv:
        symbol = symbol_ohlcv[0].split('-')[0].split('/')[1].strip()
        try:
            df = stocks[symbol_ohlcv]
        except KeyError:
            print(f'Can not find {symbol}')
            continue
        df.loc[:, 'Symbol'] = symbol
        df.rename(columns={f'EURONEXT/{symbol} - Open': 'Open',
                           f'EURONEXT/{symbol} - High': 'High',
                           f'EURONEXT/{symbol} - Low': 'Low',
                           f'EURONEXT/{symbol} - Last': 'Close',
                           f'EURONEXT/{symbol} - Volume': 'Volume',
                           f'EURONEXT/{symbol} - Turnover': 'Turnover'}, inplace=True)
        df.index.name = 'date'
        DataType.extend("EOD")
        table_name = DataChannel.name_to_data_type(f"EURONEXT.{symbol}", DataType.EOD)
        DataChannel.upload(df, table_name,
                           is_overwrite=True,
                           arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME,
                           put_in_redis=False)
        print(f'Uploaded {table_name}')


if __name__ == "__main__":
    main()
