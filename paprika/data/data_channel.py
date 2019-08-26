import pandas as pd
import logging

from arctic import Arctic
from arctic import CHUNK_STORE
from datetime import datetime, timedelta
import redis
import time

from paprika.data.data_type import DataType



class DataChannel:
    # this class deals with temporary data feeds
    
    redis = redis.Redis('localhost')
    DEFAULT_ARCTIC_SOURCE_NAME = 'feeds'
    DEFAULT_ARCTIC_HOST = 'localhost'
    
    @staticmethod
    def name_to_data_type(name: str, data_type: DataType):
        name = name.upper().strip()
        return f'{name}.{str(data_type)}'
    
    @staticmethod
    def upload_to_permanent(table_name: str,
                            is_overwrite: bool = True
                            ):
        table_name = table_name.upper().strip()
        df = DataChannel.download(table_name, use_redis=False)  # need to make sure it was uploaded to temporary 'feeds'
        if df is None:
            raise KeyError(
                f'{table_name} does not exist in {DataChannel.DEFAULT_ARCTIC_SOURCE_NAME} on ' +
                f'{DataChannel.DEFAULT_ARCTIC_HOST}')
        else:
            DataChannel.upload(df, table_name, is_overwrite, arctic_source_name='mdb',
                               arctic_host=DataChannel.DEFAULT_ARCTIC_HOST, put_in_redis=False)
    
    @staticmethod
    def upload(data_frame: pd.DataFrame,
               table_name: str,
               is_overwrite: bool = True,
               arctic_source_name: str = DEFAULT_ARCTIC_SOURCE_NAME,
               arctic_host: str = DEFAULT_ARCTIC_HOST,
               put_in_redis=True,
               string_format=True):
        
        arctic = Arctic(arctic_host)
        if arctic_source_name not in arctic.list_libraries():
            arctic.initialize_library(arctic_source_name, lib_type=CHUNK_STORE)
        
        library = arctic[arctic_source_name]
        
        table_name = table_name.upper().strip() if string_format else table_name
        if not (table_name in library.list_symbols()):
            library.write(table_name, data_frame)
        elif is_overwrite:
            library.update(table_name, data_frame)
        else:
            library.append(table_name, data_frame)
        
        logging.info(f"Uploaded Arctic {table_name} to {arctic_source_name} on {arctic_host}")
        
        if put_in_redis:
            DataChannel.redis.set(table_name, data_frame.to_msgpack(compress='blosc'))
            logging.info(f"Uploaded Redis {table_name} on {DataChannel.DEFAULT_ARCTIC_HOST}")
        
        return arctic_host, arctic_source_name, table_name
    
    @staticmethod
    def download(table_name: str,
                 arctic_source_name: str = DEFAULT_ARCTIC_SOURCE_NAME,
                 arctic_host: str = DEFAULT_ARCTIC_HOST,
                 use_redis=True,
                 string_format=True,
                 cascade = True):
        
        if string_format:
            table_name = table_name.upper().strip()
        msg = DataChannel.redis.get(table_name) if use_redis else None
        if msg:
            logging.debug(f'Load Redis cache for {table_name}')
            return pd.read_msgpack(msg)
        else:
            logging.debug(f'Load Arctic cache for {table_name}')
            arctic = Arctic(arctic_host)
            assert arctic_source_name in arctic.list_libraries()

            library = arctic[arctic_source_name]
            
            if table_name in library.list_symbols() or not cascade:
                logging.info(f"Read {table_name} inside {arctic_source_name} on {arctic_host}.")
            else:
                library = arctic['mdb']
                logging.info(f"Read {table_name} inside {arctic_source_name} on {arctic_host}.")

            return library.read(table_name)
    
    @staticmethod
    def delete_table(table_name: str, arctic_source_name: str = DEFAULT_ARCTIC_SOURCE_NAME,
                     arctic_host: str = DEFAULT_ARCTIC_HOST,
                     string_format=True):
        table_name = table_name.upper().strip() if string_format else table_name
        arctic = Arctic(arctic_host)
        assert arctic_source_name in arctic.list_libraries()
        library = arctic[arctic_source_name]
        DataChannel.redis.delete(table_name)
        return library.delete(table_name)
    
    @staticmethod
    def table_names(arctic_source_name: str = DEFAULT_ARCTIC_SOURCE_NAME, arctic_host: str = DEFAULT_ARCTIC_HOST):
        arctic = Arctic(arctic_host)
        assert arctic_source_name in arctic.list_libraries()
        library = arctic[arctic_source_name]
        return library.list_symbols()
    
    @staticmethod
    def clear_all_feeds(arctic_source_name: str = DEFAULT_ARCTIC_SOURCE_NAME, arctic_host: str = DEFAULT_ARCTIC_HOST):
        # just to be safe...
        assert arctic_source_name == 'feeds'
        assert arctic_host == 'localhost'
        arctic = Arctic(arctic_host)
        if arctic_source_name not in arctic.list_libraries():
            arctic.initialize_library(arctic_source_name, lib_type=CHUNK_STORE)
        library = arctic[arctic_source_name]
        for table_name in library.list_symbols():
            DataChannel.redis.delete(table_name)
            library.delete(table_name)
    
    @staticmethod
    def extract_time_span(symbol, time_start, time_delta=timedelta(seconds=300),
                          arctic_source_name: str = DEFAULT_ARCTIC_SOURCE_NAME, arctic_host: str = DEFAULT_ARCTIC_HOST):
        data = DataChannel.download(symbol, arctic_source_name, arctic_host, use_redis=True, string_format=False)
        return_data = data.loc[time_start:(time_start + time_delta)]
        if return_data.shape[0] <= 0:
            return_data = data
        return return_data
      