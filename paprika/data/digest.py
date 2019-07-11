import numpy as np
import pandas as pd
from arctic import CHUNK_STORE, Arctic


def digest_ohlcv(source,
                 symbol,
                 frequency,
                 file_path,
                 mongodb_host='localhost'):
    df = pd.read_csv(file_path)

    df.set_index('timestamp', inplace=True)
    timestamps = df.index.to_series()
    start = timestamps.iloc[0]
    end = timestamps.iloc[-1]
    step = timestamps.diff().value_counts().index[0]
    filled_index = np.arange(start, end + 1, step, dtype=np.int64)
    df = df.reindex(filled_index)
    df['close'].fillna(method='ffill', inplace=True)
    df['open'].fillna(df['close'], inplace=True)
    df['high'].fillna(df['close'], inplace=True)
    df['low'].fillna(df['close'], inplace=True)
    df['volume'].fillna(0, inplace=True)
    df.index = pd.to_datetime(df.index, unit='ms')
    df.index.names = ['date']
    df = df[['open', 'high', 'low', 'close', 'volume']]

    lib_name = source

    store = Arctic(mongodb_host)
    store.initialize_library(lib_name, lib_type=CHUNK_STORE)
    library = store[lib_name]
    library.write(
        symbol + '_' + frequency,
        df,
        chunk_size='M',
        metadata={
            'start': df.index[0],
            'end': df.index[-1]
        })

    return df


if __name__ == '__main__':
    # pass
    df = digest_ohlcv(
        'Bitfinex', 'BTC/USD', '1m',
        '/Users/SuperBeario/projects/chives/data/bitfinex/candle/bitfinex_ohlcv_tBTCUSD_1m.csv'
    )
    print(df)
