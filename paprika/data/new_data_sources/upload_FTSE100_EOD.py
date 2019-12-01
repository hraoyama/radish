from multiprocessing import Process

import pandas as pd
import os

from paprika.data.data_utils import EOD_upload_given_columns

PATH = r'../../../resources/data/'


def main():
    stocks = pd.read_csv(os.path.join(PATH, 'FTSE100_20070102_20190211.csv'))
    stocks['Index'] = pd.to_datetime(stocks.Index)
    stocks.set_index('Index', inplace=True)
    
    symbols = set([symbol.split('.')[0] for symbol in stocks.columns])
    symbols_ohlcv = [[f'{symbol}.L.Open',
                      f'{symbol}.L.High',
                      f'{symbol}.L.Low',
                      f'{symbol}.L.Close',
                      f'{symbol}.L.Volume']
                     for symbol in symbols]
    index_name = 'FTSE100'
    city_id = 'L'
    ps = [Process(target=EOD_upload_given_columns, args=(index_name, symbol_ohlcv, stocks, city_id)) for symbol_ohlcv in
          symbols_ohlcv]
    for p in ps:
        p.start()
    for p in ps:
        p.join()


if __name__ == "__main__":
    main()
