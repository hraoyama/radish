
import pandas as pd
import numpy as np
import os
import sys
import typing
from datetime import datetime
from paprika.data.data_type import DataType
from paprika.data.feed import Feed
from paprika.utils import utils
import matplotlib.pyplot as plt


from collections import defaultdict
from paprika.signals.signal_data import SignalData


class Assessor:

    # should we think about asessing multiple signals at once ? yes!
    _data = None

    def __init__(self, signal_data: SignalData):
        self._data = signal_data

    def plot_returns(self):
        returns = utils.returns_calculator(self._data.get_frame("prices").values, 1)
        port_return = utils.portfolio_return_calculator(self._data.get_frame("postitions").values, returns)
        plt.plot(self._data.get_frame("prices").index, port_return.cumsum())
        plt.xticks(rotation=45)
        plt.ylabel('Cumulative return')
        plt.title("Cointegration of {} vs. {}.".format(self._data.get_frame("prices").columns.values[0], self._data.get_frame("prices").columns.values[1]))
        plt.show()


