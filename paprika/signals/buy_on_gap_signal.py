import numpy as np
import pandas as pd
from typing import List, Tuple

from paprika.data.fetcher import DataType
from paprika.data.feed_subscriber import FeedSubscriber


class BuyOnGap(FeedSubscriber):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.topN = self.get_parameter("TOPN")
        self.positions = pd.DataFrame()

    def handle_event(self, events: List[Tuple[DataType, pd.DataFrame]]):

        super().handle_event(events)
        op = events[0][1][:1].values[0]
        buy_price = events[1][1][:1].values[0]
        ret_gap = events[2][1][:1].values[0]
        ma = events[3][1][:1].values[0]

        has_data = np.where(np.isfinite(ret_gap) & (op < buy_price) & (op > ma))
        has_data = has_data[0]
        positions = np.zeros((1, ret_gap.shape[0]))
        if len(has_data) > 0:
            idx = np.argsort(ret_gap[has_data])
            positions[0, has_data[idx[np.arange(np.min((self.topN, len(idx))))]]] = 1

        self.positions = self.positions.append(pd.DataFrame(positions))
