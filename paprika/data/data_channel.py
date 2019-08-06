import pandas as pd
import logging

from arctic import Arctic
from arctic import CHUNK_STORE
import redis


class DataChannel:
    redis = redis.Redis('localhost')
    
    @staticmethod
    def upload(data_frame: pd.DataFrame,
               table_name: str,
               is_overwrite: bool = True,
               arctic_source_name: str = 'feeds',
               arctic_host: str = 'localhost',
               put_in_redis=True):
        
        arctic = Arctic(arctic_host)
        if arctic_source_name not in arctic.list_libraries():
            arctic.initialize_library(arctic_source_name, lib_type=CHUNK_STORE)
        
        library = arctic[arctic_source_name]
        
        if not (table_name in library.list_symbols()):
            library.write(table_name, data_frame)
        elif is_overwrite:
            library.update(table_name, data_frame)
        else:
            library.append(table_name, data_frame)
        
        logging.info(f"Uploaded Arctic {table_name} to {arctic_source_name} on {arctic_host}")
        
        if put_in_redis:
            DataChannel.redis.set(table_name, data_frame.to_msgpack(compress='blosc'))
            logging.info(f"Uploaded Redis {table_name} on localhost")
        
        return arctic_host, arctic_source_name, table_name
    
    @staticmethod
    def download(table_name: str,
                 arctic_source_name: str = 'feeds',
                 arctic_host: str = 'localhost',
                 use_redis=True):
        
        msg = DataChannel.redis.get(table_name) if use_redis else None
        if msg:
            logging.debug(f'Load Redis cache for {table_name}')
            return pd.read_msgpack(msg)
        else:
            logging.debug(f'Load Arctic cache for {table_name}')
            arctic = Arctic(arctic_host)
            assert arctic_source_name in arctic.list_libraries()
            
            library = arctic[arctic_source_name]
            logging.info(f"Read {table_name} inside {arctic_source_name} on {arctic_host}.")
            return library.read(table_name)
    
    @staticmethod
    def delete_table(table_name: str, arctic_source_name: str = 'feeds', arctic_host: str = 'localhost'):
        arctic = Arctic(arctic_host)
        assert arctic_source_name in arctic.list_libraries()
        library = arctic[arctic_source_name]
        DataChannel.redis.delete(table_name)
        return library.delete(table_name)
    
    @staticmethod
    def table_names(arctic_source_name: str = 'feeds', arctic_host: str = 'localhost'):
        arctic = Arctic(arctic_host)
        assert arctic_source_name in arctic.list_libraries()
        library = arctic[arctic_source_name]
        return library.list_symbols()
    
    @staticmethod
    def clear_all_feeds(arctic_source_name: str = 'feeds', arctic_host: str = 'localhost'):
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
