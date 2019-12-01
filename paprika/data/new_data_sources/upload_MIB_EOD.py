from multiprocessing import Process
import pandas as pd
import os

from paprika.data.data_utils import EOD_upload_given_columns

PATH = r'../../../resources/data/'


def main():
    stocks = pd.read_csv(os.path.join(PATH, 'MIB40_20070102_20190211.csv'))
    stocks['Index'] = pd.to_datetime(stocks.Index)
    stocks.set_index('Index', inplace=True)
    
    symbols = set([symbol.split('.')[0].strip() for symbol in stocks.columns])
    symbols_ohlcv = [[f'{symbol}.MI.Open',
                      f'{symbol}.MI.High',
                      f'{symbol}.MI.Low',
                      f'{symbol}.MI.Close',
                      f'{symbol}.MI.Volume']
                     for symbol in symbols]
    index_name = 'MIB'
    city_id = 'MI'
    ps = [Process(target=EOD_upload_given_columns, args=(index_name, symbol_ohlcv, stocks, city_id)) for symbol_ohlcv in
          symbols_ohlcv]
    for p in ps:
        p.start()
    for p in ps:
        p.join()


if __name__ == "__main__":
    main()
