import pandas as pd
import os

from paprika.data.data_channel import DataChannel
from paprika.data.data_utils import EOD_upload_given_columns
from multiprocessing import Process

PATH = r'../../../resources/data/'


def main():
    to_remove_list = DataChannel.check_register(['CAC.*EOD'], feeds_db=False)
    for to_remove in to_remove_list:
        DataChannel.delete_table(to_remove, arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME)
    
    stocks = pd.read_csv(os.path.join(PATH, 'CAC40_20070102_20190211.csv'))
    stocks['Index'] = pd.to_datetime(stocks.Index)
    stocks.set_index('Index', inplace=True)
    symbols = set([symbol.split('.')[0].strip() for symbol in stocks.columns])
    symbols_ohlcv = [[f'{symbol}.PA.Open',
                      f'{symbol}.PA.High',
                      f'{symbol}.PA.Low',
                      f'{symbol}.PA.Close',
                      f'{symbol}.PA.Volume']
                     for symbol in symbols]
    index_name = 'CAC'
    city_id = 'PA'
    ps = [Process(target=EOD_upload_given_columns, args=(index_name, symbol_ohlcv, stocks, city_id)) for symbol_ohlcv in
          symbols_ohlcv]
    for p in ps:
        p.start()
    for p in ps:
        p.join()


if __name__ == "__main__":
    main()
