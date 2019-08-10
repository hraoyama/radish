
from paprika.data.data_type import DataType


def test_data_type():
    a = DataType.TRADES
    DataType.extend('TIMESERIES')
    print(list(DataType))
    for a, b in enumerate(list(DataType)):
        print(a, b)
    
    b = DataType.TIMESERIES
    DataType.extend('SOMETHING')
    DataType.extend('ANYTHING')
    
    print(list(DataType))
    
    assert len(list(DataType)) == 5