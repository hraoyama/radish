import math
import time
from datetime import datetime, timezone
from typing import Callable, Union


def current_millis() -> int:
    return int(round(time.time() * 1e3))


def current_micros() -> int:
    return int(round(time.time() * 1e6))


def current_millis_round_to_seconds() -> int:
    """Keep precision to seconds."""
    return int(math.ceil(time.time()) * 1e3)


def current_micros_round_to_seconds() -> int:
    """Keep precision to seconds."""
    return int(math.ceil(time.time()) * 1e6)


def datetime_to_millis(dt: datetime) -> int:
    if dt is not None:
        return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1e3)
    else:
        return None


def datetime_to_micros(dt: datetime) -> int:
    if dt is not None:
        return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1e6)
    else:
        return None


def millis_to_datetime(timestamp: int) -> datetime:
    if timestamp is not None:
        return datetime.utcfromtimestamp(timestamp / 1e3)
    else:
        return None


def micros_to_datetime(timestamp: int) -> datetime:
    if timestamp is not None:
        return datetime.utcfromtimestamp(timestamp / 1e6)
    else:
        return None


SECONDS_PER_UNIT = {
    's': 1,
    'm': 60,
    'h': 3600,
    'd': 86400,
}


def seconds_for_frequency(frequency: str) -> int:
    num = int(frequency[:-1])
    unit = frequency[-1]
    return num * SECONDS_PER_UNIT[unit]


def millis_for_frequency(frequency: str) -> int:
    return int(seconds_for_frequency(frequency) * 1e3)


def micros_for_frequency(frequency: str) -> int:
    return int(seconds_for_frequency(frequency) * 1e6)


class FrameClock:
    def __init__(self):
        self._current_millis = None

    def now_utc_datetime(self):
        return micros_to_datetime(self._current_millis)

    def current_frame_timestamp(self):
        return self._current_millis

    def set_current_frame_time(self, t: Union[datetime, int]):
        if isinstance(t, int):
            self._current_millis = t
        elif isinstance(t, datetime):
            self._current_millis = datetime_to_micros(t)
