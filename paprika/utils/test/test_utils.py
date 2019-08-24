from datetime import datetime
import numpy as np
from pprint import pprint as pp

from paprika.utils.utils import summarize


def test_utils():
    num_obs = 1000
    np.random.seed(num_obs)
    random_numbers = np.random.randn(num_obs)
    pp(summarize(random_numbers, [np.max, np.min, np.median, np.mean, lambda x: np.sum(np.abs(x) > 1.0) / len(x)],
                 column_names=["MAX", "MIN", "MED", "AVG", "LG1"]))
