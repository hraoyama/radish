from multiprocessing import Process

import pandas as pd
import os

from paprika.data.data_utils import EOD_upload_given_columns
from paprika.data.data_channel import DataChannel

PATH = r'../../../resources/data/'


def main():
    stocks = pd.read_csv(os.path.join(PATH, 'SP500_20070103_20190211.csv'))
    stocks['Index'] = pd.to_datetime(stocks.Index)
    stocks.set_index('Index', inplace=True)

    symbols = set([symbol.split('.')[0] for symbol in stocks.columns])
    symbols_ohlcv = [[f'{symbol}.Open',
                      f'{symbol}.High',
                      f'{symbol}.Low',
                      f'{symbol}.Close',
                      f'{symbol}.Volume']
                     for symbol in symbols]
    for symbol_ohlcv in symbols_ohlcv:
        symbol = symbol_ohlcv[0].split('.')[0]
        df = stocks[symbol_ohlcv]
        df.loc[:, 'Symbol'] = symbol
        df.rename(columns={f'{symbol}.Open': 'Open',
                           f'{symbol}.High': 'High',
                           f'{symbol}.Low': 'Low',
                           f'{symbol}.Close': 'Close',
                           f'{symbol}.Volume': 'Volume'}, inplace=True)
        df.index.name = 'date'
        # DataType.extend("EOD")
        # table_name = DataChannel.name_to_data_type(f"SP500.{symbol}", DataType.EOD)
        table_name = f"SP500.{symbol}.1D.Candle"
        DataChannel.upload(df, table_name,
                           is_overwrite=True,
                           arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME,
                           put_in_redis=False,
                           string_format=False)
        print(f'Uploaded {table_name}')

    # index_name = 'SP500'
    # city_id = None
    # ps = [Process(target=EOD_upload_given_columns, args=(index_name, symbol_ohlcv, stocks, city_id)) for symbol_ohlcv in
    #       symbols_ohlcv]
    # for p in ps:
    #     p.start()
    # for p in ps:
    #     p.join()


if __name__ == "__main__":
    main()
