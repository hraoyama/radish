import numpy as np
from typing import List, Tuple

from paprika.data.data_type import DataType
from paprika.data.feed_subscriber import FeedSubscriber
from paprika.data.feed_filter import *


class TryOutSignal(FeedSubscriber):
    def __init__(self, **kwargs):
        super(TryOutSignal, self).__init__(**kwargs)
        self._parameter_dict["OBSERVATION"] = []
    
    def handle_event(self, events: List[Tuple[DataType, pd.DataFrame]]):
        super(TryOutSignal, self).handle_event(events)
        self._parameter_dict["COUNT"] = self.call_count
        for event in events:
            if 'ISIN' in event[1].columns.values:
                self._parameter_dict["OBSERVATION"].append(
                    (np.max(event[1].index), event[0], event[1].ISIN[0]))
            else:
                self._parameter_dict["OBSERVATION"].append(
                    (np.max(event[1].index), event[0], event[1].iloc[0, 0]))


class RandomSignal(FeedSubscriber):
    def __init__(self, **kwargs):
        super(RandomSignal, self).__init__(**kwargs)
    
    def handle_event(self, events: List[Tuple[DataType, pd.DataFrame]]):
        return np.random.randn(1)[0]


class TryOutCompositeSignal(FeedSubscriber):
    def __init__(self, **kwargs):
        super(TryOutCompositeSignal, self).__init__(**kwargs)
        self._MEASURE = "RANDOM_MEASURE"
        self._parameter_dict[self._MEASURE] = []
        self.member_signals = []
        if "NUM_RND" in self._parameter_dict.keys():
            for i in range(int(self.get_parameter("NUM_RND"))):
                self.member_signals.append(RandomSignal())
    
    def handle_event(self, events: List[Tuple[DataType, pd.DataFrame]]):
        super(TryOutCompositeSignal, self).handle_event(events)
        self._parameter_dict["COUNT"] = self.call_count
        self.get_parameter(self._MEASURE).append(sum([x.handle_event(events) for x in self.member_signals]))
        # events[0][1]
