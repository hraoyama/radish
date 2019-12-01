from multiprocessing import Process

import pandas as pd
import os

from paprika.data.data_utils import EOD_upload_given_columns

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
    index_name = 'SP500'
    city_id = None
    ps = [Process(target=EOD_upload_given_columns, args=(index_name, symbol_ohlcv, stocks, city_id)) for symbol_ohlcv in
          symbols_ohlcv]
    for p in ps:
        p.start()
    for p in ps:
        p.join()


if __name__ == "__main__":
    main()
