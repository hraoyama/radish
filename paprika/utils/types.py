_float_type = float


def float_type():
    return _float_type


def enter_decimal_paradise():
    from paprika.utils.quant import Quant, init
    init()
    global _float_type
    _float_type = Quant
