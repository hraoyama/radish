from multiprocessing import Process

import pandas as pd
import os

from paprika.data.data_utils import EOD_upload_given_columns
from paprika.data.data_channel import DataChannel

PATH = r'../../../resources/data/'


def main():
    df = pd.read_csv(os.path.join(PATH, '^GSPC.csv'))
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    df['date'] = pd.to_datetime(df.date)
    df.set_index('date', inplace=True)
    table_name = f"GSPC.1D.Candle"
    DataChannel.upload(df, table_name,
                       is_overwrite=True,
                       arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME,
                       put_in_redis=False,
                       string_format=False)
    print(f'Uploaded {table_name}')


if __name__ == "__main__":
    main()
