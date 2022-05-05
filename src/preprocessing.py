import numpy as np
import pandas as pd


def winsorize_zscore(x, p = 0.025):
    y = x.copy()
    q1, q3 = x.quantile(p), x.quantile(1-p)
    y[y <= q1] = q1
    y[y >= q3] = q3
    return (y-y.mean())/y.std()
    

def drawdown(returns):
    """Returns the drawdown of cumulative returns
    Assumptions: cumulative is in percentage terms [0.4, 0.8, 0.1]
    """
    rets = np.array(returns)
    cumu_returns = np.cumprod(1+rets)
    cumu_returns = np.array(cumu_returns)
    cumu_returns = np.insert(cumu_returns, 0, 1)
    cummax = np.maximum.accumulate(cumu_returns)
    res = np.divide(cumu_returns, cummax) - 1
    return res[1:]
