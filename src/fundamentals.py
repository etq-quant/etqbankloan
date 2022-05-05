import bql
import pandas as pd
from dateutil.relativedelta import relativedelta

from .data import query_field

bq = bql.Service()


def get_prev_monthenddate(dt, n=1, return_str_date=True):
    ref_dt = dt + relativedelta(months=-n) + relativedelta(day=31)
    if return_str_date:
        ref_dt = ref_dt.strftime("%Y-%m-%d")
    return ref_dt


"""Preprocessing functions"""


def growth_abs(vals, n_qtr):
    val = (vals[-1] - vals[-n_qtr - 1]) / abs(vals[-n_qtr - 1])
    return val


def change(vals, n_qtr):
    val = vals[-1] - vals[-n_qtr - 1]
    return val


def last_n(vals, n_qtr, nth=0):
    return vals[-nth - 1]


def first_n(vals, n_qtr, nth=0):
    return vals[nth]


"""General query function"""


def query_fundamentals(
    tickers,
    field,
    func,
    n_qtr,
    fpo=None,
    fpt="Q",
    fa_adjusted="Y",
    fill="PREV",
    extend_n_qtr=4,
    fwd_n_qtr=0,
    debug=False,
    **kwargs
):
    """Generalized function to query fundamentals data and apply preprocessing function `func`
    field: bq.data.{field}
    n_qtr: the growth relative `n_qtr` quarters ago.
    """
    if fpo is None:
        fpo = bq.func.range(-n_qtr - extend_n_qtr, fwd_n_qtr)
    x1 = query_field(
        univ=tickers,
        field=field,
        fpt=fpt,
        fa_period_offset=fpo,
        fa_adjusted=fa_adjusted,
        fill=fill,
        **kwargs
    ).sort_index()
    tdfs = []
    for idx in x1.index.unique():
        tdf = x1.loc[[idx], :].copy()
        tdf = tdf.sort_values("PERIOD_END_DATE")
        tdf[field] = tdf[field].ffill()
        tdf = (
            tdf.reset_index()
            .groupby("PERIOD_END_DATE")
            .last()
            .reset_index()
            .set_index("ID")
        )
        vals = tdf[field].values
        val = func(vals, n_qtr)
        tdf[field] = val  # override
        tdfs.append(tdf.iloc[-1:])  # and get last value
    if debug:
        return x1
    return pd.concat(tdfs)


"""deprecated"""


def query_growth_abs(
    tickers,
    field,
    n_qtr,
    fpt="Q",
    fa_adjusted="Y",
    fill="PREV",
    extend_n_qtr=4,
    **kwargs
):
    """Compute the malaysia stock biz version of QoQ/YoY growth, i.e. (X - X_old) / abs(X_old) - 1
    field: bq.data.{field}
    n_qtr: the growth relative `n_qtr` quarters ago.
    """
    fpo = bq.func.range(-n_qtr - extend_n_qtr, 0)
    x1 = query_field(
        univ=tickers,
        field=field,
        fpt=fpt,
        fa_period_offset=fpo,
        fa_adjusted=fa_adjusted,
        fill=fill,
        **kwargs
    ).sort_index()
    tdfs = []
    for idx in x1.index.unique():
        try:
            tdf = x1.loc[[idx], :].copy()
            tdf = tdf.sort_values("PERIOD_END_DATE")
            tdf[field] = tdf[field].ffill()
            tdf = (
                tdf.reset_index()
                .groupby("PERIOD_END_DATE")
                .last()
                .reset_index()
                .set_index("ID")
            )
            vals = tdf[field].values
            val = (vals[-1] - vals[-n_qtr - 1]) / abs(vals[-n_qtr - 1])
            tdf[field] = val  # override
            tdfs.append(tdf.iloc[-1:])  # and get last value
        except Exception as err:
            ""
#             print("Error @query_growth_abs", idx, err)
    return pd.concat(tdfs)
