import datetime
from functools import partial

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from src.cache import memorize
from src.analyst_q2 import query_field, query_growth_abs
from src.style import format_decimals
import plotly.express as px


def last_n(vals, n_qtr, nth=0):
    return vals[-nth - 1]


@memorize
def create_features_v2(universe, as_of_date, selected_period="QoQ"):
    """
    selected_period: 'QoQ' or 'YoY'
    """
    selected = selected_period
    univ = universe.copy()

    config = {
        "QoQ": {"fpt": "Q", "n_qtr": 4, "month_offset": -3},
        "YoY": {"fpt": "LTM", "n_qtr": 4, "month_offset": -12},
    }

    month_offset = config[selected]["month_offset"]
    n_qtr = config[selected]["n_qtr"]

    dt1 = as_of_date  # datetime.datetime.today()
    dt0 = dt1 + relativedelta(months=month_offset)
    dt_fpr = dt1 + relativedelta(months=12)

    df1 = query_field(univ=univ, field="PX_LAST", dates=dt1, fill="PREV")
    """Query fields"""
    # Revenue QoQ%
    df_rev_growth_yoy = query_growth_abs(
        tickers=universe,
        field="SALES_REV_TURN",
        fa_adjusted="Y",
        n_qtr=4,
        as_of_date=as_of_date,
    )
    df_rev_growth_qoq = query_growth_abs(
        tickers=universe,
        field="SALES_REV_TURN",
        fa_adjusted="Y",
        n_qtr=1,
        as_of_date=as_of_date,
    )
    df1["SALES_GROWTH_YOY"] = 100 * (
        df_rev_growth_yoy["SALES_REV_TURN"]
    )
    df1["SALES_GROWTH_QOQ"] = 100 * (
        df_rev_growth_qoq["SALES_REV_TURN"]
    )
    df1[f"Revenue YoY(%)"] = format_decimals(
        df1["SALES_GROWTH_YOY"].round(2), 2, True
    )
    df1[f"Revenue QoQ(%)"] = format_decimals(
        df1["SALES_GROWTH_QOQ"].round(2), 2, True
    )

    # NetIncome QoQ%
    df_nic_growth_qoq = query_growth_abs(
        tickers=universe,
        field="NET_INCOME",
        fa_adjusted="Y",
        n_qtr=1,
        as_of_date=as_of_date,
    )
    df_nic_growth_yoy = query_growth_abs(
        tickers=universe,
        field="NET_INCOME",
        fa_adjusted="Y",
        n_qtr=4,
        as_of_date=as_of_date,
    )
    df1["NET_INC_GROWTH_QOQ"] = 100 * (
        df_nic_growth_qoq["NET_INCOME"]
    )
    df1["NET_INC_GROWTH_YOY"] = 100 * (
        df_nic_growth_yoy["NET_INCOME"]
    )
    df1[f"NetProfit QoQ(%)"] = format_decimals(
        df1["NET_INC_GROWTH_QOQ"].round(2), 2, True
    )
    df1[f"NetProfit YoY(%)"] = format_decimals(
        df1["NET_INC_GROWTH_YOY"].round(2), 2, True
    )

    # Consensus Rating
    cons_df1 = query_field(
        univ=univ, field="eqy_rec_cons", dates=dt1, fill="PREV")
    cons_df2 = query_field(
        univ=univ, field="eqy_rec_cons", dates=dt0, fill="PREV")
    df1["eqy_rec_cons"] = cons_df1["eqy_rec_cons"]
    df1["Diff_eqy_rec_cons"] = cons_df1["eqy_rec_cons"] - cons_df2["eqy_rec_cons"]
    df1[f"Consensus Rating"] = (
        format_decimals(df1["eqy_rec_cons"].round(2), 2)
        + " ("
        + format_decimals(df1["Diff_eqy_rec_cons"].round(2), 2, True)
        + ")"
    )

    # Target Price
    dfbtp1 = query_field(
        univ=univ, field="BEST_TARGET_PRICE", dates=dt1, fill="PREV")
    dfbtp2 = query_field(
        univ=univ, field="BEST_TARGET_PRICE", dates=dt0, fill="PREV")
    df1["Diffpct_Target_Price"] = (
        dfbtp1["BEST_TARGET_PRICE"] - dfbtp2["BEST_TARGET_PRICE"]
    ) / dfbtp2["BEST_TARGET_PRICE"]
    df1["Target Price"] = (
        format_decimals(dfbtp1["BEST_TARGET_PRICE"].round(3), 2)
        + " ("
        + format_decimals(
            (df1["Diffpct_Target_Price"] * 100).round(2), 1, True, zfill=False
        )
        + "%)"
    )
    df1["Target Price(%)"] = (
        format_decimals(
            (df1["Diffpct_Target_Price"] * 100).round(2), 1, True, zfill=False
        )
        + "%"
    )

    # Earnings Revision %
    df_netinc_fwd0 = query_field(
        univ=univ,
        field="NET_INCOME",
        fa_adjusted="Y",
        fpr=dt_fpr.strftime("%Y-%m-%d"),
        as_of_date=dt0.strftime("%Y-%m-%d"),
    )
    df_netinc_fwd1 = query_field(
        univ=univ,
        field="NET_INCOME",
        fa_adjusted="Y",
        fpr=dt_fpr.strftime("%Y-%m-%d"),
        as_of_date=dt1.strftime("%Y-%m-%d"),
    )
    df1["EarningsRevisionPct"] = (
        100
        * (df_netinc_fwd1["NET_INCOME"] - df_netinc_fwd0["NET_INCOME"])
        / df_netinc_fwd0["NET_INCOME"].abs()
    )
    df1["Earnings Revision(%)"] = format_decimals(
        df1["EarningsRevisionPct"].round(2), 2, True
    )

    # Momentum 1w, 1m
    df_wk_ret = query_field(univ=univ, field="TOTAL_RETURN",
                            CALC_INTERVAL="1W", dt2=as_of_date)
    df_mth_ret = query_field(
        univ=univ, field="TOTAL_RETURN", CALC_INTERVAL="1M", dt2=as_of_date)
    df1["1W RETURN(%)"] = format_decimals(
        (df_wk_ret["TOTAL_RETURN"]*100).round(2), 2, True
    )
    df1["1M RETURN(%)"] = format_decimals(
        (df_mth_ret["TOTAL_RETURN"]*100).round(2), 2, True
    )

    # Reported (Days Ago)
    df1_nic = query_field(
        univ=universe,
        field="NET_INCOME",
        fa_adjusted="Y",
        fpt="Q",
        fa_period_offset="0Q",
        fill="PREV",
        as_of_date=as_of_date,
    )
    df1["Reported_Days"] = (dt1 - df1_nic["REVISION_DATE"]).dt.days
    df1["Last Reported"] = df1["Reported_Days"].fillna(-1)

    # df1["WkRet"] = wk_ret["TOTAL_RETURN"]
    # df1["MthRet"] = mth_ret["TOTAL_RETURN"]
    # df1["QtrRet"] = qtr_ret["TOTAL_RETURN"]

    return df1


def plot_price(pdf, start_date, title="Custom MarketCap-Weighted Index"):
    pdf2 = (pdf.loc[start_date:]+1).cumprod()
    plot_df = pdf2.reset_index().melt(id_vars="DATE")
    plot_df.columns = ["DATE", "Index", "Value"]
    fig = px.line(plot_df, x="DATE", y="Value", color="Index")
    fig.update_layout(title=title)
    fig = fig.update_layout(
        autosize=False, width=1100, height=500, template="simple_white"
    )
    return fig


def create_facet_agg_plot(agg_fdf2):
    agg_fdf2["DATE"] = pd.to_datetime(agg_fdf2["DATE"])
    if agg_fdf2.DATE.unique().shape[0] < 2:
        x3 = agg_fdf2.copy()
        x3["DATE"] = x3["DATE"]-datetime.timedelta(days=30)
        df3 = pd.concat([agg_fdf2, x3])
    else:
        df3 = agg_fdf2.copy()
    pdf = df3.melt(id_vars=["DATE", "Index"])
    fig = px.line(pdf, x="DATE", y="value", color="Index",
                  facet_col="Index", facet_row="variable")
    fig.update_layout(autosize=False, width=1200,
                      height=1500, template="simple_white")
    fig.update_yaxes(matches=None)
    return fig


"""deprecated"""


def compute_header_finalize_analyst_df(idx_df, analyst_df, selected_period):
    fdf_analyst = pd.merge(
        idx_df, analyst_df, left_on="ID", right_index=True, how="left"
    )
    analyst_columns = analyst_df.columns
    sectors = fdf_analyst["Sector"].unique().tolist()
    sectors.remove("")

    """To create header (Aggregate values across sector)"""
    tdfs = []
    for sector in sectors:
        tdf = fdf_analyst.loc[fdf_analyst.Sector == sector].copy()
        sum_weight = pd.to_numeric(tdf["Weight (%)"]).sum()

        header_dict = {
            "ID": "",
            "Name": sector,
            "Weight": "",
            "Sector": "",
            "Weight (%)": sum_weight,
        }
        for col in analyst_columns:
            if col.split(" ")[0] in ["Revenue", "NetProfit", "Earnings"]:
                header_dict[col] = "{:+,.2f}".format(
                    pd.to_numeric(
                        tdf[col].apply(lambda k: str(k).replace(",", "")),
                        errors="coerce",
                    )
                    .median()
                    .round(2)
                )

        header_dict["Consensus Rating"] = "{:.2f}".format(
            tdf["Consensus1"].mean()
        ) + " ({:+,.2f})".format(tdf["Consensus2"].median())
        header_dict["Target Price"] = "({:+,.1f}%)".format(
            pd.to_numeric(tdf["Target2"]).median()
        )
        header_dict["Last Reported"] = ""

        header_df = pd.DataFrame(header_dict, index=[sector])
        tdf2 = pd.concat([header_df, tdf[header_df.columns]], axis=0)
        tdfs.append(tdf2.copy())

    all_cols = [
        "ID",
        "Weight (%)",
        f"Revenue {selected_period}(%)",
        f"NetProfit {selected_period}(%)",
        "Consensus Rating",
        "Target Price",
        "Earnings Revision(%)",
        "Last Reported",
    ]
    fdf_analyst2 = pd.concat(tdfs)
    fdf_analyst2["ID"] = fdf_analyst2["Name"]  # .apply(lambda x: x[:16])
    return fdf_analyst2[all_cols]  # fdf_analyst2_styled.render(), fdf_analyst2
