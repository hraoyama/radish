from multiprocessing import Process

import pandas as pd
import os

from paprika.data.data_utils import EOD_upload_given_columns

PATH = r'../../../resources/data/'


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
    index_name = 'HK'
    city_id = 'HK'
    ps = [Process(target=EOD_upload_given_columns, args=(index_name, symbol_ohlcv, stocks, city_id)) for symbol_ohlcv in
          symbols_ohlcv]
    for p in ps:
        p.start()
    for p in ps:
        p.join()


if __name__ == "__main__":
    main()
