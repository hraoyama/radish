from paprika.data.data_processor import DataProcessor
from paprika.data.feed import TimePeriod, TimeFreqFilter

from haidata.fix_colnames import fix_colnames

from pprint import pprint as pp
from datetime import datetime
import matplotlib.pyplot as plt

import numpy as np


def test_data_processor_multiple_instruments():
    # column_name = "Price", return_type = "LOG_RETURN", new_column_name = None, overwrite = False
    
    dp_from_two_together = DataProcessor(["EUX.FDAX201709.Trade", "EUX.FESX201709.Trade"]).\
        index('2017-06-01 08:00', '2017-06-10 08:00').between_time('08:15', '16:30').positive_price()
    pp(dp_from_two_together.data[ dp_from_two_together.data.ISIN == "FDAX201709"].head(5))
    pp(dp_from_two_together.data[dp_from_two_together.data.ISIN == "FESX201709"].head(5))
    pp(dp_from_two_together.data.shape)
