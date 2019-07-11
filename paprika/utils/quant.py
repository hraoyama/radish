from decimal import ROUND_DOWN, Decimal, getcontext

from absl import app, logging


def init():
    getcontext().prec = 8

    from json import JSONEncoder

    def _default(self, obj):
        return getattr(obj.__class__, "to_json", _default.default)(obj)
    _default.default = JSONEncoder().default
    JSONEncoder.default = _default


class Quant(Decimal):
    def __new__(cls, value=0):
        if isinstance(value, float):
            return super().__new__(cls, str(value))
        elif isinstance(value, str):
            # Remove ',' in '5,402.750'.
            value = value.replace(',', '')
            return super().__new__(cls, value)
        else:
            return super().__new__(cls, value)

    def __repr__(self):
        return str(self)

    def to_json(self):
        return str(self)


def reduce_precision(amount: Quant):
    """1.414 -> 1.41"""
    if amount.is_zero():
        return amount
    digits_without_last = str(amount)[:-1]
    if not digits_without_last:
        return amount
    return amount.quantize(Quant(digits_without_last), rounding=ROUND_DOWN)


def main(argv):
    del argv

    d = Quant(1.414)
    logging.info('reduce_precision: %s -> %s',
                 d, reduce_precision(d))


if __name__ == '__main__':
    app.run(main)
