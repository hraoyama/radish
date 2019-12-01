from multiprocessing import Process

import pandas as pd
import os

from paprika.data.data_utils import EOD_EURONEXT_upload_given_columns

PATH = r'../../../resources/data/'


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
    ps = [Process(target=EOD_EURONEXT_upload_given_columns, args=(symbol_ohlcv, stocks)) for symbol_ohlcv in
          symbols_ohlcv]
    for p in ps:
        p.start()
    for p in ps:
        p.join()


if __name__ == "__main__":
    main()
