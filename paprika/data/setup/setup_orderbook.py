from enum import Enum
from arctic import Arctic, CHUNK_STORE
from functools import partial
import os
import pandas as pd
import numpy as np
import re
import multiprocessing


class ArcticAction(Enum):
    OVERWRITE = 1
    APPEND = 2
    UPDATE = 3


def read_and_persist(file_name, data_path, qty_pattern, px_pattern, tag, lib, action):

    data = pd.read_csv(os.path.join(data_path, file_name), sep="|")
    cols_to_int64 = [x for x in data.columns.values if x in ('Date', 'TimeSec', 'TimeMM') or qty_pattern.match(x)]
    data = data[data[cols_to_int64].notnull().all(axis=1)]
    data[cols_to_int64] = data[cols_to_int64].astype(np.int64)
    data['date'] = pd.to_datetime(data.Date.map(str) + " " + data.TimeSec.map(str) + " " + data.TimeMM.map(str),
                                  format='%Y%m%d %H%M%S %f')

    cols_to_float64 = [x for x in data.columns.values if px_pattern.match(x)]
    data[cols_to_float64] = data[cols_to_float64].astype(np.float64)
    print(f'{file_name} is read')
    all_products = data['ISIN'].unique()

    for product in all_products:
        symbol = tag + '.' + product + '.OrderBook'
        df = data[data['ISIN'] == product]
        df.sort_values(by=['date'], inplace=True)
        df.set_index('date', inplace=True)

        if symbol not in lib.list_symbols():
            lib.write(symbol, df, chunk_size='D')
            print(f'NEW Write: {symbol}')
        elif (action == ArcticAction.OVERWRITE) or (action == ArcticAction.UPDATE):
            lib.update(symbol, df, chunk_size='D')
            print(f'UPDATE Write: {symbol}')
        elif action == ArcticAction.APPEND:
            lib.append(symbol, df, chunk_size='D')
            print(f'APPEND Write: {symbol}')

    print(f'Persist {file_name} finish')
    return


def main():

    file_patterns = [re.compile("^[0-9]{8}_\S+_Book.csv.gz$")]
    qty_pattern = re.compile("^.*_Qty_Lev_[0-9]$")
    px_pattern = re.compile("^.*_Px_Lev_[0-9]$")

    root = os.path.join(os.getenv("DATA_DIR_STOCKS"), 'm_data')
    dirs_to_load = ['EUX', 'ETF', 'MTA']
    lib_name = 'mdb'
    store = Arctic('localhost')
    arctic_action = ArcticAction.OVERWRITE

    try:
        assert lib_name in store.list_libraries()
    except AssertionError as error:
        store.initialize_library(lib_name, lib_type=CHUNK_STORE)
    lib = store[lib_name]
    lib._arctic_lib.set_quota(350 * 1024 * 1024 * 1024)
    n_processes = multiprocessing.cpu_count()

    for dir_to_load in dirs_to_load:
        data_path = os.path.join(root, dir_to_load)
        if not os.path.exists(data_path):
            continue
        else:
            # make sure the file patterns are mutually exclusive, else you will be loading multiple times the same data
            gz_csv_files = [x for x in os.listdir(data_path) for y in file_patterns if y.match(x) is not None]
        if len(gz_csv_files) < 1:
            continue
        gz_csv_files.sort()

        tag = dir_to_load.strip().upper()
        func = partial(read_and_persist, data_path=data_path, qty_pattern=qty_pattern, px_pattern=px_pattern,
                       tag=tag, lib=lib, action=arctic_action)

        # with multiprocessing.Pool(processes=n_processes) as pool:
        #    pool.map(func, gz_csv_files[:10])
        for gz_csv_file in gz_csv_files:
            func(gz_csv_file)


if __name__ == '__main__':
    main()
