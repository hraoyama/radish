import pandas as pd
import numpy as np
import os

from paprika.exchange.data_processor import DataProcessor
from paprika.data.data_channel import DataChannel, DataType


def test_alloc_Kelly():
    
    tckrs = ['GLD', 'GDX', 'IGE', 'KO', 'OIH', 'PEP', 'RKH', 'RTH', 'SPY']
    resource_path = r'../../../resources/data/'

    
    
    

