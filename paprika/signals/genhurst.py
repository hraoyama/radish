# -*- coding: utf-8 -*-
"""
Created on Fri Sep 21 17:08:58 2018

@author: Ernest
"""
import numpy as np
import pandas as pd
import statsmodels.api as sm


def genhurst(z):
    """
    Calculation of the Hurst exponent given the log price series z
    :param z:
    :return:
    """
    z = pd.DataFrame(z)
    # We cannot use tau that is of same magnitude of time series length
    taus = np.arange(1, np.round(len(z)/10)).astype(int)
    log_var = np.empty(len(taus))  # log variance

    for tau in taus:
        log_var[tau-1] = np.log(z.diff(tau).var(ddof=0))
        
    x = np.log(taus)
    y = log_var[:len(taus)]
    x = x[np.isfinite(log_var)]
    y = y[np.isfinite(log_var)]

    x = sm.add_constant(x)
    model = sm.OLS(y, x)
    results = model.fit()
    hurst_val = results.params[1]/2
    p_value = results.pvalues[1]
    return hurst_val, p_value
