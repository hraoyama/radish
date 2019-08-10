# this is just a wrapper but we keep the layer in case if increased complexity later on


class DataGenerator(object):
    def __init__(self, generator_function):
        self._generator_function = generator_function
    
    def generate(self, *args, **kwargs):
        return self._generator_function(*args, **kwargs)
    
    @property
    def generator(self):
        return self._generator_function






