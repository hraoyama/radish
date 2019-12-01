import pandas as pd
import os
from paprika.data.data_type import DataType
from paprika.data.data_channel import DataChannel


def get_data_frame_from_excel(ticker: str, input_path):
    ts1 = pd.read_excel(os.path.join(input_path, '{}.xls'.format(ticker)))
    if ts1 is not None:
        ts1 = ts1.sort_values(by=['Date'])
        ts1 = ts1.reset_index(drop=True)
        ts1 = ts1.rename(columns={'Date': 'date'})
        ts1.set_index('date', inplace=True)
    return ts1


def get_data_frame_from_csv(ticker: str, input_path):
    ts1 = pd.read_csv(os.path.join(input_path, '{}.csv'.format(ticker)))
    if ts1 is not None:
        ts1 = ts1.sort_values(by=['Date'])
        ts1 = ts1.reset_index(drop=True)
        ts1 = ts1.rename(columns={'Date': 'date'})
        ts1.set_index('date', inplace=True)
    return ts1


def new_data_upload(df, ticker, data_type_str, to_feeds=False, to_redis=False, to_permanent=True):
    if df is not None:
        if data_type_str not in [str(x) for x in DataType]:
            DataType.extend(data_type_str.upper().strip())
        table_name = DataChannel.name_to_data_type(ticker.upper().strip(), DataType(len(DataType) - 1))
        print(f'Attempting to upload {table_name} as DataType.{data_type_str.upper().strip()}')
        if to_feeds:
            return DataChannel.upload(df, table_name, put_in_redis=to_redis)
        if to_permanent:
            return DataChannel.upload(df, table_name,
                                      arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME,
                                      put_in_redis=to_redis)
    return None


def OHLCVAC_upload(ticker: str, input_path, to_feeds=False, to_redis=False, to_permanent=True):
    df = get_data_frame_from_excel(ticker, input_path)
    if df is not None:
        if "OHLCVAC_PRICE" not in [str(x) for x in DataType]:
            DataType.extend("OHLCVAC_PRICE")
        table_name = DataChannel.name_to_data_type(ticker.upper().strip(), DataType.OHLCVAC_PRICE)
        print(f'Uploading {table_name}')
        if to_feeds:
            return DataChannel.upload(df, table_name, put_in_redis=to_redis)
        if to_permanent:
            return DataChannel.upload(df, table_name,
                                      arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME,
                                      put_in_redis=to_redis)
    return None


def EOD_EURONEXT_upload_given_columns(symbol_ohlcv, stocks):
    symbol = symbol_ohlcv[0].split('-')[0].split('/')[1].strip()
    try:
        df = stocks[symbol_ohlcv][:]
    except KeyError:
        print(f'Can not find {symbol}')
        return None, None, None
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
    print(f'Uploading {table_name}')
    return DataChannel.upload(df, table_name,
                              is_overwrite=True,
                              arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME,
                              put_in_redis=False)


def EOD_upload_given_columns(index_name, symbol_ohlcv, stocks, city_id):
    # symbol_ohlcv = the list of column names
    # stocks = the data frame with everything
    symbol = symbol_ohlcv[0].split('.')[0].strip()
    try:
        df = stocks[symbol_ohlcv][:]
    except KeyError:
        print(f'Can not find {symbol}')
        return None, None, None
    
    df.loc[:, 'Symbol'] = symbol
    if city_id is not None:
        df.rename(columns={f'{symbol}.{city_id}.Open': 'Open',
                           f'{symbol}.{city_id}.High': 'High',
                           f'{symbol}.{city_id}.Low': 'Low',
                           f'{symbol}.{city_id}.Close': 'Close',
                           f'{symbol}.{city_id}.Volume': 'Volume'}, inplace=True)
    else:
        df.rename(columns={f'{symbol}.Open': 'Open',
                           f'{symbol}.High': 'High',
                           f'{symbol}.Low': 'Low',
                           f'{symbol}.Close': 'Close',
                           f'{symbol}.Volume': 'Volume'}, inplace=True)
    df.index.name = 'date'
    DataType.extend("EOD")
    table_name = DataChannel.name_to_data_type(f"{index_name}.{symbol}", DataType.EOD)
    print(f'Uploading {table_name}')
    return DataChannel.upload(df, table_name,
                              is_overwrite=True,
                              arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME,
                              put_in_redis=False)
