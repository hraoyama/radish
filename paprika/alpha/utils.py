from typing import Union


def period_to_int(period: Union[str, int]) -> int:
    if isinstance(period, int):
        return period
    elif isinstance(period, str):
        return period_str_to_int(period)
    else:
        raise NotImplementedError


def period_str_to_int(period: str) -> int:
    raise NotImplementedError