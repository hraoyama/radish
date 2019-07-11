from math import ceil
from threading import Thread

from absl import logging

from paprika.core.events import TimerEventBacktest
from paprika.utils.time import datetime_to_micros, micros_for_frequency


class TimerEventGenerator:
    def __init__(self, handler, frequency, start_datetime, end_datetime):
        self.handler = handler
        self.interval_millis = micros_for_frequency(frequency)
        self.start_millis = datetime_to_micros(start_datetime)
        self.end_millis = datetime_to_micros(end_datetime)
        self.timestamp = ceil(self.start_millis /
                              self.interval_millis) * self.interval_millis

    def get_event(self):
        return TimerEventBacktest(self.timestamp, self.handler)

    def next(self):
        self.timestamp += self.interval_millis
        if self.timestamp > self.end_millis:
            return False
        return True

    def __lt__(self, other):
        return self.timestamp < other.timestamp
