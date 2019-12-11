from paprika.data.data_channel import DataChannel
from paprika.data.data_type import DataType

import pandas as pd
import os
import numpy as np


def test_data_channel():
    DataChannel.clear_all_feeds()
    
    DataChannel.check_register(["GOLD2", "USO"])
    gld = DataChannel.download('GOLD2.OHLCVAC_PRICE', DataChannel.PERMANENT_ARCTIC_SOURCE_NAME, use_redis=False)
    gdx = DataChannel.download('USO.OHLCVAC_PRICE', DataChannel.PERMANENT_ARCTIC_SOURCE_NAME, use_redis=False)
    assert np.all(gld.index == gdx.index)
    
    assert 'PEP.OHLCVAC_PRICE' in DataChannel.check_register([".*OHLC.*"])['mdb']
    assert 'EUX.FBTP201806.OrderBook' in DataChannel.check_register([".*FBTP.*"])['mdb']
    assert 'EUX.FBTP201706.Trade' in DataChannel.check_register([".*FBTP.*"])['mdb']
    assert 'MTA.IT0005322612.Trade' in DataChannel.check_register([".*MTA.*"])['mdb']
    
    assert 'MTA.IT0005322612.Trade' in DataChannel.table_names(DataChannel.PERMANENT_ARCTIC_SOURCE_NAME)
    # unless this name was run in this session...
    assert 'MTA.IT0005322612.Trade' not in DataChannel.table_names(DataChannel.DEFAULT_ARCTIC_SOURCE_NAME)
    
    tckr1 = 'GLD'
    PATH = r'../../../resources/data/'
    ts1 = pd.read_excel(os.path.join(PATH, '{}.xls'.format(tckr1)), usecols=[0, 6])
    ts1 = ts1.sort_values(by=['Date'])
    ts1 = ts1.reset_index(drop=True)
    ts1 = ts1.rename(columns={'Date': 'date'})
    ts1.set_index('date', inplace=True)
    
    assert 'GOLD.TEMPORARY_TYPE' not in DataChannel.check_register([".*TEMPORARY_TYPE.*"], arctic_sources=['feeds'])['feeds']
    assert 'GOLD.TEMPORARY_TYPE' not in DataChannel.check_register([".*TEMPORARY_TYPE.*"], arctic_sources=['mdb'])['mdb']
    assert DataChannel.redis.get('GOLD.TEMPORARY_TYPE') is None
    
    DataType.extend("TEMPORARY_TYPE")
    table_name = DataChannel.name_to_data_type("GOLD", DataType.TEMPORARY_TYPE)
    DataChannel.upload(ts1, table_name, put_in_redis=True)
    
    assert 'GOLD.TEMPORARY_TYPE' in DataChannel.check_register([".*TEMPORARY_TYPE.*"])['feeds']
    assert 'GOLD.TEMPORARY_TYPE' not in DataChannel.check_register([".*TEMPORARY_TYPE.*"])['mdb']
    assert DataChannel.redis.get('GOLD.TEMPORARY_TYPE') is not None
    DataChannel.clear_redis(['GOLD'])
    assert DataChannel.redis.get('GOLD.TEMPORARY_TYPE') is None
    
    DataChannel.upload(ts1, table_name, put_in_redis=True)
    assert DataChannel.redis.get('GOLD.TEMPORARY_TYPE') is not None
    DataChannel.clear_redis()
    assert DataChannel.redis.get('GOLD.TEMPORARY_TYPE') is None
    

