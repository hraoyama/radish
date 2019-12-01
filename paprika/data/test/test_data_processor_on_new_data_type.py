from paprika.utils.record import Timeseries
from paprika.data.data_type import DataType
from paprika.data.data_channel import DataChannel
from paprika.data.data_processor import DataProcessor

from datetime import datetime, timedelta
from pprint import pprint as pp
import numpy as np


def test_data_processor_on_new_data_type():
    # manipulate new data types
    start = datetime.now()
    end = start + timedelta(days=20)
    num_obs = 1000
    random_dates = start + (end - start) * np.random.random([1, num_obs])
    random_dates.sort()
    random_numbers = np.random.randn(num_obs)
    new_data = Timeseries()
    for ts, val in zip(list(random_dates[0]), random_numbers):
        new_data.append(ts, val)
    DataType.extend('TIMESERIES')
    
    data4 = DataProcessor(new_data.to_dataframe(column_name="observation"), table_name="MyRandomIdentifier") \
        ("between_time", '08:00:00', '09:00:00') \
        (lambda x: x[np.abs(x["observation"]) < 2.0]).data
    pp(data4)
    
    t1 = DataChannel.download("MyRandomIdentifier", string_format=False)
    pp(t1.head(10))
