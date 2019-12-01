import numbers
from collections import defaultdict
from types import MappingProxyType

from absl import app, logging

from paprika.utils.types import float_type
from paprika.utils.utils import isclose, percentage


class Dist(defaultdict):
    """Distribution over variable X (e.g. str)"""

    def __init__(self, raw_dict={}):
        super().__init__(float_type(), {k: float_type()(v)
                                        for k, v in raw_dict.items()})

    def __repr__(self) -> str:
        return repr({k: "{}".format(v) for k, v in self.items()})

    def __add__(self, value: 'Dist') -> 'Dist':
        a = self.readonly()
        b = value.readonly()
        keys = set(a.keys()).union(b.keys())
        return Dist({k: a[k] + b[k] for k in keys})

    def __sub__(self, value: 'Dist') -> 'Dist':
        a = self.readonly()
        b = value.readonly()
        keys = set(a.keys()).union(b.keys())
        return Dist({k: a[k] - b[k] for k in keys})

    def __mul__(self, value) -> 'Dist':
        a = self.readonly()
        if isinstance(value, numbers.Number):
            return Dist({k: v * value for k, v in a.items()})
        elif isinstance(value, Dist):
            b = value.readonly()
            keys = set(a.keys()).union(b.keys())
            return Dist({k: a[k] * b[k] for k in keys})
        raise NotImplementedError()

    def __truediv__(self, value) -> 'Dist':
        a = self.readonly()
        if isinstance(value, numbers.Number):
            return Dist({k: v / value for k, v in a.items()})
        elif isinstance(value, Dist):
            b = value.readonly()
            keys = set(a.keys()).union(b.keys())
            return Dist({k: a[k] / b[k] for k in keys})
        raise NotImplementedError()

    def abs(self):
        return Dist({k: abs(v) for k, v in self.items()})

    def readonly(self) -> 'Dist':
        """Returns a shallow copy"""
        return Dist(self)

    def normalize(self) -> 'NormDist':
        total = float_type()(sum([abs(k) for k in self.values()]))
        if isclose(total, 0):
            raise ValueError('Distribution sum is too close to zero')

        return NormDist({k: (float_type()(v) / total) for k, v in self.items()})

    def sum(self) -> float:
        return float_type()(sum(self.values()))

class NormDist(Dist):
    """Normalized distribution, i.e. the sum is 1"""

    def __init__(self, raw_dict={}):
        super().__init__(raw_dict)

    def __repr__(self) -> str:
        return repr({k: percentage(v) for k, v in self.items()})


def main(argv):
    del argv

    from paprika.utils.types import enter_decimal_paradise
    enter_decimal_paradise()

    a = Dist({'a': 4, 'b': 6, 'c': 10})
    logging.info(a)

    b = Dist({'a': 1.0, 'b': 2.0, 'c': 0.0, 'd': 10.0})
    logging.info(b.normalize())
    logging.info(b.normalize() * 1000)

    import numpy as np
    c = Dist(
        {'a': np.float64(1e300), 'b': np.float64(1e299), 'c': 1e298}).normalize()
    logging.info(c)

    try:
        d = Dist({'a': 0}).normalize()
    except ValueError as e:
        logging.info(f'Expected exception: {e}')

    logging.info(a * b)
    logging.info(a - b)
    print(a)
    logging.info(a / a)

    print(f'{float_type()}')
    for k, v in a.items():
        print(f'{type(k)} {type(v)}')


if __name__ == '__main__':
    app.run(main)
