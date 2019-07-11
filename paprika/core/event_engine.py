import asyncio
import heapq
from queue import Empty, Queue
from typing import Callable, Dict, List, Type

from paprika.core.api import get_current_frame_timestamp
from paprika.core.event_generator import TimerEventGenerator
from paprika.core.events import Event, TimerEventBacktest
from paprika.utils.time import seconds_for_frequency


class EventEngine:
    pass


class EventEngineBacktest(EventEngine):
    def __init__(self, start_datetime, end_datetime):
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self._generator_heap = []

    def register_timer(self, handler: Callable, frequency: str):
        event_generator = TimerEventGenerator(
            handler, frequency, self.start_datetime, self.end_datetime)
        heapq.heappush(self._generator_heap, event_generator)

    def run_one_event(self):
        if not self._generator_heap:
            return False
        nearest_generator = heapq.heappop(self._generator_heap)
        event = nearest_generator.get_event()

        from paprika.core.context import get_context
        get_context().clock.set_current_frame_time(event.timestamp)

        event.trigger()

        if nearest_generator.next():
            heapq.heappush(self._generator_heap, nearest_generator)
        return True
