import pandas as pd
import numpy as np
import os

from paprika.exchange.data_processor import DataProcessor
from paprika.exchange.processor_utils import get_return_series
from paprika.data.data_type import DataType

from multiprocessing import Process, Manager

def test_alloc_Kelly():
    DataProcessor.MAKE_AVAILABLE_IN_FEEDS = False

    manager = Manager()
    return_dict = manager.dict()
    
    tckrs = ['GLD', 'GDX', 'IGE', 'KO', 'OIH', 'PEP', 'RKH', 'RTH', 'SPY']
    resource_path = r'../../../resources/data/'
    
    ps = [Process(target=get_return_series, args=(ticker, DataType.OHLCVAC_PRICE)) for ticker in tckrs]
    for p in ps:
        p.start()
    for p in ps:
        p.join()

    
    
    

