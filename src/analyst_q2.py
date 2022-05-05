import datetime
from functools import partial

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from src.cache import memorize
from src.style import format_decimals

try:
    import bql
    bq = bql.Service()
except Exception as err:
    print("No bql.")


@memorize
def query_field(univ, field, dt1=None, dt2=None, **kwargs):
    if dt1 is not None and dt2 is not None:
        dates = bq.func.range(dt1, dt2)
        kwargs["dates"] = dates
    bq_data_item = getattr(bq.data, field)(**kwargs)
    reqq = bql.Request(univ, {field: bq_data_item})
    res = bq.execute(reqq)
    return res[0].df()


@memorize
def create_custom_index_df(univ, mcap_ftr=1, limit=None, remove_googl=False, currency="USD",**kwargs):
    # Query constituents in an index/given list
    if not isinstance(univ, list):
        univ = bq.univ.members(univ)

    reqq = bql.Request(univ, {
        "id_": bq.data.id(),
        "mcap": bq.data.cur_mkt_cap(),
        "name": bq.data.name(),
        "sector": bq.data.gics_sector_name(),
    },
        with_params={'currency': currency,
                     # 'mode':'cached'
                     }
    )
    res = bq.execute(reqq)
    df1 = res[0].df()
    df2 = res[1].df()
    df3 = res[2].df()
    df4 = res[3].df()
    idx_df = pd.concat([df1, df2, df3, df4], axis=1)

    if remove_googl:
        idx_df = idx_df.loc[idx_df.index != "GOOG UW Equity"]
    if limit is None:
        limit = idx_df.shape[0]
    idx_df2 = idx_df.sort_values("mcap", ascending=False).head(limit)
    idx_df2["mcap2"] = idx_df2["mcap"]**(1/mcap_ftr)
    idx_df2["Weight"] = idx_df2["mcap2"]/idx_df2["mcap2"].sum()
    idx_df2["Weight (%)"] = format_decimals(
        (idx_df2["Weight"]*100).round(2), 2, False
    )
    idx_df2["Ticker"] = idx_df2.index
    idx_df2["Name"] = idx_df2.name
    try:
        temp_df = pd.read_csv(
            "data/temp_us.csv").set_index("ID")
        temp_df = temp_df.loc[~temp_df.index.duplicated()]
        idx_df2 = pd.merge(
            idx_df2, temp_df["SHORT_NAME"], left_index=True, right_index=True, how="left")
        if idx_df2["SHORT_NAME"].isna().mean() < 0.2:
            idx_df2["Name"] = idx_df2["SHORT_NAME"].fillna(idx_df2["name"])
    except Exception as err:
        print(err)
    return idx_df2


@memorize
def create_mcap_based_index(index_cons_list, dt2, top_n=None, mcap_ftr=1, dt1="2016-12-01", **kwargs):
    spx_1 = query_field("SPX Index", field="CUR_MKT_CAP", dt1=dt1, dt2=dt2)
    spx_idx = spx_1.set_index("DATE")["CUR_MKT_CAP"].dropna().index

    pdf_2 = query_field(index_cons_list, field="CUR_MKT_CAP", dt1=dt1, dt2=dt2)
    qtrs = [j.date() for j in pd.date_range(dt1, dt2, freq="1Q")]

    if top_n is None:
        top_n = 1000

    if qtrs[-1] != pd.to_datetime(dt2):
        qtrs.append(pd.to_datetime(dt2))

    prets = []
    for current_qtr_dt, next_qtr_dt in zip(qtrs[:-1], qtrs[1:]):
        q1 = current_qtr_dt+datetime.timedelta(days=1)
        q2 = next_qtr_dt

        pdf_3 = pdf_2.reset_index().pivot(index="DATE", columns="ID", values="CUR_MKT_CAP")
        pdf_sf_3 = pdf_2.reset_index().pivot(
            index="DATE", columns="ID", values="CUR_MKT_CAP").shift(1)
        pdf_3 = pdf_3.reindex(spx_idx).ffill()
        pdf_sf_3 = pdf_sf_3.reindex(spx_idx).ffill()**(1/mcap_ftr)

        rdf_3 = pdf_3.pct_change()
        rk = pdf_3.loc[:current_qtr_dt].tail(1).rank(axis=1, ascending=False).T
        selected_constituents = rk.loc[rk.iloc[:, 0] <= top_n].index

        num = (pdf_sf_3.loc[q1:q2, selected_constituents] *
               rdf_3.loc[q1:q2, selected_constituents]).sum(axis=1)
        den = pdf_sf_3.loc[q1:q2, selected_constituents].sum(axis=1)
        pret = num/den
        prets.append(pret)
    pret_df = pd.concat(prets).sort_index()
    return pret_df


@memorize
def create_price_index(index_df, name, dt1="-1825d", dt2="0d"):
    df = query_field(index_df.index, field="PX_LAST",
                     dt1=dt1, dt2=dt2)
    rdf = df.reset_index().pivot(index="DATE", columns="ID",
                                 values="PX_LAST").pct_change()
    wg = (~rdf.isna())*index_df["Weight"]
    ret = (rdf*wg).sum(axis=1)/wg.sum(axis=1)
    ret_df = pd.DataFrame(ret).fillna(0)
    ret_df.columns = [name]
    return ret_df


@memorize
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
