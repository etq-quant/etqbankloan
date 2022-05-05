import empyrical as em
import numpy as np
import pandas as pd
from empyrical.stats import (
    _aligned_series,
    _create_unary_vectorized_roll_function,
    rolling_window,
)


def rfill(roll_res, window):
    if isinstance(roll_res, pd.Series):
        res = roll_res.values.tolist()
    elif isinstance(roll_res, np.ndarray):
        res = roll_res.tolist()
    return np.array([np.nan] * (window - 1) + res)


def vol_diff(price_vol_data, lkb):
    # Compute sum of volume of past `lkb` to `lkb-1` days for days that are above current price,
    # and for days that are below current price.
    # Then, take the difference of the 2 sum - to measure how much volume is traded under/above the latest price.
    # price_vol_data = df[["Price", "Volume"]].values
    # Formula: VD = SUM_{P0 > Pt} Vt - SUM_{P0 < Pt} Vt
    # Bullish if total volume below current price is greater than total volume above current price.
    z1 = rolling_window(price_vol_data, lkb)  # [-1]
    above = z1[:, -1:, 0] > z1[:, :-1, 0]
    below = z1[:, -1:, 0] < z1[:, :-1, 0]
    vol_sum_above = (z1[:, :-1, 1] * above).sum(axis=1)
    vol_sum_below = (z1[:, :-1, 1] * below).sum(axis=1)
    vol_diff = vol_sum_above - vol_sum_below
    return rfill(vol_diff, lkb)


def term_zscore(x, short=20, long=252, stdev=252):
    return (x.rolling(short).mean() - x.rolling(long).mean()) / x.rolling(stdev).std()


def stochastic_oscillator(x, lkb):
    rw = x.rolling(lkb)
    return (x - rw.min()) / (rw.max() - rw.min())


def roll_zscore(x, mean_lkb, std_lkb):
    return (x - x.rolling(mean_lkb).mean()) / x.rolling(std_lkb).std()


def roll_alpha_beta(returns, benchs, window):
    x = em.roll_alpha_beta(returns, benchs, window)
    return x.iloc[:, 0], x.iloc[:, 1]


def roll_alpha_beta_down(returns, benchs, window):
    benchs_down = benchs[benchs < 0]
    x = em.roll_alpha_beta(returns, benchs_down, window)
    return x.iloc[:, 0], x.iloc[:, 1]


def roll_alpha_beta_up(returns, benchs, window):
    benchs_up = benchs[benchs > 0]
    x = em.roll_alpha_beta(returns, benchs_up, window)
    return x.iloc[:, 0], x.iloc[:, 1]


def roll_exponential_regression(returns, windows):
    x = returns.copy()
    if isinstance(returns, pd.Series):
        x = returns.values

    wins = rolling_window(x, windows)  # construct rolling windows
    Y = np.log(1 + em.cum_returns(wins.T))
    x = np.arange(1, windows + 1, 1)
    X = np.c_[x, np.ones_like(x)]
    coeffs = np.polyfit(x, Y, 1)  # [:,0]
    Yhat = X @ coeffs
    ybar = Yhat.mean(axis=0)
    ssreg = np.sum(np.power(Yhat - ybar, 2), axis=0)
    sstot = np.sum(np.power(Y - ybar, 2), axis=0)
    determination = ssreg / sstot
    slope = coeffs[0, :]
    intercept = coeffs[1, :]

    if isinstance(returns, pd.Series):
        slope = pd.Series(slope, returns.index[(windows - 1) :])
        intercept = pd.Series(intercept, returns.index[(windows - 1) :])
        determination = pd.Series(determination, returns.index[(windows - 1) :])
    return determination, slope, intercept


def roll_standard_regression(price_x, price_y, windows):
    if isinstance(price_x, pd.Series) and isinstance(price_y, pd.Series):
        gen = _aligned_series(price_x, price_y)
        x, y = [j for j in gen]
        idx = x.index
        x, y = x.values, y.values
    else:
        assert len(price_x) == len(price_y)
        x, y = price_x, price_y
    wins_x = rolling_window(x, windows)  # construct rolling windows
    wins_y = rolling_window(y, windows)  # construct rolling windows
    coeffs = [np.polyfit(x1, y1, 1) for x1, y1 in zip(wins_x, wins_y)]
    slope = [j[0] for j in coeffs]
    intercept = [j[1] for j in coeffs]

    coeffs = np.array(coeffs)
    slopes, intercepts = coeffs[:, 0], coeffs[:, 1]

    yhat = slopes[:, np.newaxis] * wins_x
    ybar = yhat.mean(axis=1)
    ssreg = np.sum(np.power(yhat - ybar[:, np.newaxis], 2), axis=1)
    sstot = np.sum(np.power(wins_y - ybar[:, np.newaxis], 2), axis=1)
    determination = ssreg / sstot

    slopes = rfill(slopes, windows)
    intercepts = rfill(intercepts, windows)
    determination = rfill(determination, windows)

    if isinstance(price_x, pd.Series) and isinstance(price_y, pd.Series):
        slopes = pd.Series(slopes, index=idx)
        intercepts = pd.Series(intercepts, index=idx)
        determination = pd.Series(determination, index=idx)
        yhat = pd.Series(yhat, index=idx)

    return determination, slopes, intercepts


def cagr_em(returns, annualized_factor=252, days=None, out=None):
    """faster implementation for roll_cagr_em only
    use metrics_functions.cagr for non-rolling computation
    parameters:
    days - not used!
    """

    allocated_output = out is None
    if allocated_output:
        out = np.empty(returns.shape[1:])

    return_1d = returns.ndim == 1

    if len(returns) < 2:
        out[()] = np.nan
        if return_1d:
            out = out.item()
        return out

    ann_factor = annualized_factor
    out = em.cagr(returns, annualization=ann_factor)
    if return_1d:
        out = out.item()
    return out


def roll_drawdown(x, window, min_periods=1):
    y = (x + 1).cumprod()
    return y / y.rolling(window, min_periods=min_periods).max() - 1


roll_cagr = _create_unary_vectorized_roll_function(cagr_em)
roll_max_drawdown = em.roll_max_drawdown
