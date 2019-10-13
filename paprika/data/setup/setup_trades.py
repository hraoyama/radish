from arctic import Arctic, CHUNK_STORE
from functools import partial
import multiprocessing
import os
import pandas as pd
import re


def csv_read(filename, data_path):
    """
    Wrapper function for individual csv read that can be mapped
    :param data_path:
    :param filename:
    :return:
    """
    print(f'Read {filename}')
    data = pd.read_csv(os.path.join(data_path, filename), sep="|")
    data['date'] = pd.to_datetime(data.Date.map(str) + " " + data.TimeSec.map(str) + " " + data.TimeMM.map(str),
                                  format='%Y%m%d %H%M%S %f')
    return data


def persist_in_db(product, tag, df, lib, lib_name):
    """
    Wrapper function for individual csv read that can be mapped
    :param product:
    :param tag:
    :param df:
    :param lib:
    :param lib_name:
    :return:
    """
    symbol = tag + '.' + product + '.Trade'
    tmp = df[df['ISIN'] == product]
    tmp.set_index('date', inplace=True)
    lib.write(symbol, tmp, chunk_size='D')
    print(f'Persist {lib_name}: {symbol} finish')


def main():

    pattern = re.compile("^[0-9]{8}_\S+_MKtrade.csv.gz$")
    root = os.path.join(os.getenv("DATA_DIR_STOCKS"), 'm_data')
    dirs_to_load = ['EUX', 'ETF', 'MTA']  #
    lib_name = 'mdb'

    store = Arctic('localhost')
    try:
        assert (lib_name in store.list_libraries())
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
            gz_csv_files = [x for x in os.listdir(data_path) if pattern.match(x) is not None]
        if len(gz_csv_files) < 1:
            continue

        func = partial(csv_read, data_path=data_path)
        with multiprocessing.Pool(processes=n_processes) as pool:
            all_data = pool.map(func, gz_csv_files)

        df = pd.concat(all_data, axis=0)
        df.sort_values(by=['date', 'ISIN'], inplace=True)  # probably not necessary
        print(df.shape)
        all_products = df['ISIN'].unique()
        tag = dir_to_load.strip().upper()

        func2 = partial(persist_in_db, tag=tag, df=df, lib=lib, lib_name=lib_name)
        with multiprocessing.Pool(processes=n_processes) as pool:
            pool.imap(func2, all_products)


if __name__ == '__main__':
    main()
