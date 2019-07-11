from typing import Callable

from paprika.utils.time import (current_millis_round_to_seconds,
                                millis_to_datetime)


class Event:
    def __init__(self, timestamp: int, handler: Callable):
        self.timestamp = timestamp
        self.handler = handler

    def trigger(self):
        self.handler(self)


class TimerEventBacktest(Event):
    def __repr__(self):
        return 'TimerEventBacktest({})'.format(millis_to_datetime(self.timestamp))



class TickEvent(Event):
    pass
