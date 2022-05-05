import datetime
from functools import partial

import bql
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from src.style import format_decimals
from src.data import query_field
from src.fundamentals import last_n, query_fundamentals, query_growth_abs

bq = bql.Service()
from src.preprocessing import winsorize_zscore




def get_indices_pe_ratio(dt):
    # dt = datetime.datetime(2022, 3, 2)
    index_names = ["FBMKLCI", "FBM100", "FBMS"]
    pe_ratios = {sheetname: pd.read_excel('static_xlsx/peratio.xlsx', sheet_name=sheetname).assign(Index=sheetname).set_index("DATE").loc[:dt, "PE_RATIO"].values[-1] for sheetname in index_names}
    return pe_ratios


def compute_analyst_report_df(universe, selected_period, as_of_date):
    """
    selected_period: 'QoQ' or 'YoY'
    """
    selected = selected_period
    univ = universe.copy()

    config = {
#         "QoQ": {"fpt": "Q", "n_qtr": 1, "month_offset": -3},
        "QoQ": {"fpt": "Q", "n_qtr": 4, "month_offset": -3},
        "YoY": {"fpt": "LTM", "n_qtr": 4, "month_offset": -12},
    }

    month_offset = config[selected]["month_offset"]
    fpt = config[selected]["fpt"]
    n_qtr = config[selected]["n_qtr"]

    dt1 = as_of_date  # datetime.datetime.today()
    dt0 = dt1 + relativedelta(months=month_offset)
    dt_fpr = dt1 + relativedelta(months=12)

    """Query fields"""
    #     df_netincgrow = query_field(univ=univ, field="NET_INC_GROWTH", fpt=fpt)
    df1_nic = query_field(
        univ=universe,
        field="NET_INCOME",
        fa_adjusted="Y",
        fpt="Q",
        fa_period_offset="0Q",
        fill="PREV",
        as_of_date=as_of_date,
    )
    df_nic_growth = query_growth_abs(
        tickers=universe,
        field="NET_INCOME",
        fa_adjusted="Y",
        n_qtr=n_qtr,
        as_of_date=as_of_date,
    )

    df_rev_growth = query_growth_abs(
        tickers=universe,
        field="SALES_REV_TURN",
        fa_adjusted="Y",
        n_qtr=n_qtr,
        as_of_date=as_of_date,
    )

    df1 = query_field(univ=univ, field="BEST_TARGET_PRICE", dates=dt1, fill="PREV")
    df2 = query_field(univ=univ, field="BEST_TARGET_PRICE", dates=dt0, fill="PREV")

    cons_df1 = query_field(univ=univ, field="eqy_rec_cons", dates=dt1, fill="PREV")
    cons_df2 = query_field(univ=univ, field="eqy_rec_cons", dates=dt0, fill="PREV")

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

    wk_ret = query_field(univ=univ, field="TOTAL_RETURN", CALC_INTERVAL="1W")
    mth_ret = query_field(univ=univ, field="TOTAL_RETURN", CALC_INTERVAL="1M")
    qtr_ret = query_field(univ=univ, field="TOTAL_RETURN", CALC_INTERVAL="1Q")
    
    df_ltm_nic = query_field(
        univ=universe,
        field="NET_INCOME",
        fa_adjusted="Y",
        fpt="LTM",
        fa_period_offset="0Q",
        fill="PREV",
        as_of_date=as_of_date,
    )
    df_ltm_fwdnic = query_field(
        univ=universe,
        field="NET_INCOME",
        fa_adjusted="Y",
        fpt="LTM",
        fa_period_offset="4Q",
        fill="PREV",
        as_of_date=as_of_date,
    )
    df_mcap = query_field(univ=univ, field="CUR_MKT_CAP", dates=as_of_date, fill="PREV")
    df_shares = query_field(univ=univ, field="EQY_SH_OUT", dates=as_of_date, fill="PREV")
    
    df1_asset = query_field(
        univ=universe,
        field="BS_TOT_ASSET",
        fa_period_offset="0Q",
        fill="PREV",
        as_of_date=as_of_date,
    )
    df1_liab = query_field(
        univ=universe,
        field="BS_TOT_LIAB2",
        fa_period_offset="0Q",
        fill="PREV",
        as_of_date=as_of_date,
    )
    
    df_price = query_field(
        univ=universe,
        field="PX_LAST",
        dates=as_of_date,
        fill="PREV"
    )
    df_beta_klci = query_field(
        univ=universe,
        field="BETA",
        BENCHMARK_TICKER="FBMKLCI Index"
    )
    df_beta_fbm100 = query_field(
        univ=universe,
        field="BETA",
        BENCHMARK_TICKER="FBM100 Index"
    )
    df_beta_fbms = query_field(
        univ=universe,
        field="BETA",
        BENCHMARK_TICKER="FBMS Index"
    )
    df_inv_cap = query_field(
        univ=universe,
        field="TOTAL_INVESTED_CAPITAL",
        fa_period_offset="0Q",
        fill="PREV",
        as_of_date=as_of_date,
    )
    df_roic = query_field(
        univ=universe,
        field="RETURN_ON_INV_CAPITAL",
        fa_period_offset="0Q",
        fill="PREV",
        as_of_date=as_of_date,
    )
    df_nopat = query_field(
        univ=universe,
        field="NET_OPER_PROFIT_AFTER_TAX",
        fa_period_offset="0Q",
        fill="PREV",
        as_of_date=as_of_date,
    )
    df_efc = query_field(
        univ=universe,
        field="EARN_FOR_COMMON",
        fa_period_offset="0Q",
        fill="PREV",
        as_of_date=as_of_date,
    )
    df_roe = query_field(
            univ=universe,
            field="RETURN_COM_EQY",
            fa_period_offset="0Q",
            fill="PREV",
            as_of_date=as_of_date,
        )

    
    """Parse data"""
    df1["PX_LAST"] = df_price["PX_LAST"]
    df1["BETA_KLCI"] = df_beta_klci["BETA"]
    df1["BETA_FBM100"] = df_beta_fbm100["BETA"]
    df1["BETA_FBMS"] = df_beta_fbms["BETA"]
    
    df1["FWD2_NET_INCOME"] = df_netinc_fwd1["NET_INCOME"]
    df1["PERIOD_END_DATE"] = df_netinc_fwd1["PERIOD_END_DATE"]
    
    df1["TOTAL_INVESTED_CAPITAL"] = df_inv_cap["TOTAL_INVESTED_CAPITAL"]
    df1["RETURN_ON_INV_CAPITAL"] = df_roic["RETURN_ON_INV_CAPITAL"]
    df1["NET_OPER_PROFIT_AFTER_TAX"] = df_nopat["NET_OPER_PROFIT_AFTER_TAX"]
    df1["EARN_FOR_COMMON"] = df_efc["EARN_FOR_COMMON"]
    df1["RETURN_COM_EQY"] = df_roe["RETURN_COM_EQY"]
    
    df1["FWD_NET_INCOME"] = df_ltm_fwdnic["NET_INCOME"]
    df1["NET_INCOME"] = df_ltm_nic["NET_INCOME"]
    df1["BS_TOT_ASSET"] = df1_asset["BS_TOT_ASSET"]
    df1["BS_TOT_LIAB2"] = df1_liab["BS_TOT_LIAB2"]
    df1["BS_TOT_EQUITY"] = df1["BS_TOT_ASSET"] - df1["BS_TOT_LIAB2"]
    df1["CUR_MKT_CAP"] = df_mcap["CUR_MKT_CAP"]
    df1["shares_outstanding"] = df_shares["EQY_SH_OUT"]
    df1["Reported_Days"] = (dt1 - df1_nic["REVISION_DATE"]).dt.days
    u1 = df1["Reported_Days"] <= 90
#     ref_dt = (
#         dt1 + relativedelta(months=-4) + relativedelta(day=31) + relativedelta(days=-1)
#     )
#     u1 = df1["PERIOD_END_DATE"].between(ref_dt, dt1)
    df1["Reported"] = ""
    df1.loc[u1, "Reported"] = "Yes"

    ## override data error
    df1_nic.loc["MAY MK Equity", "REVISION_DATE"] = datetime.datetime(2022, 2, 24)

    df1.loc[u1, "Reported"] = (
        df1.loc[u1, "Reported"] + " (" + df1.loc[u1, "Reported_Days"].astype(str) + ")"
    )
    df1["Reported (Days Ago)"] = df1["Reported"]

    df1["Diffpct_Target_Price"] = (
        df1["BEST_TARGET_PRICE"] - df2["BEST_TARGET_PRICE"]
    ) / df2["BEST_TARGET_PRICE"]
    df1["Diff_eqy_rec_cons"] = cons_df1["eqy_rec_cons"] - cons_df2["eqy_rec_cons"]
    df1["NET_INC_GROWTH"] = 100 * (
        df_nic_growth["NET_INCOME"]
    )  # (df1_nic["NET_INCOME"] - df0_nic["NET_INCOME"])/df0_nic["NET_INCOME"].abs()
    df1["SALES_GROWTH"] = 100 * (
        df_rev_growth["SALES_REV_TURN"]
    )  # 100*(df1_rev["SALES_REV_TURN"] - df0_rev["SALES_REV_TURN"])/df0_rev["SALES_REV_TURN"].abs()
    df1["eqy_rec_cons"] = cons_df1["eqy_rec_cons"]
    df1["EarningsRevisionPct"] = (
        100
        * (df_netinc_fwd1["NET_INCOME"] - df_netinc_fwd0["NET_INCOME"])
        / df_netinc_fwd0["NET_INCOME"].abs()
    )
    #     df1.loc[df1["Earnings_Chg"] = df_netinc_fwd1["net_income"] - df_netinc_fwd0["net_income"]
    #     df1["Earnings Revision"] = -1
    #     df1.loc[df1["Earnings_Chg"] > 0, "Earnings Revision"] = 1
    #     df1.loc[df1["Earnings_Chg"] == 0, "Earnings Revision"] = 0
    df1["WkRet"] = wk_ret["TOTAL_RETURN"]
    df1["MthRet"] = mth_ret["TOTAL_RETURN"]
    df1["QtrRet"] = qtr_ret["TOTAL_RETURN"]

    """Format numbers to string"""
    df1[f"Revenue {selected}(%)"] = format_decimals(
        df1["SALES_GROWTH"].round(2), 2, True
    )
    df1[f"NetProfit {selected}(%)"] = format_decimals(
        df1["NET_INC_GROWTH"].round(2), 2, True
    )

    df1[f"Consensus Rating"] = (
        format_decimals(df1["eqy_rec_cons"].round(2), 2)
        + " ("
        + format_decimals(df1["Diff_eqy_rec_cons"].round(2), 2, True)
        + ")"
    )
    df1["Target Price"] = (
        format_decimals(df1["BEST_TARGET_PRICE"].round(3), 2)
        + " ("
        + format_decimals(
            (df1["Diffpct_Target_Price"] * 100).round(2), 1, True, zfill=False
        )
        + "%)"
    )
    df1["Earnings Revision (%)"] = format_decimals(
        df1["EarningsRevisionPct"].round(2), 2, True
    )

    df1["Consensus1"] = df1["eqy_rec_cons"].round(2)
    df1["Consensus2"] = df1["Diff_eqy_rec_cons"].round(2)

    df1["Target2"] = (df1["Diffpct_Target_Price"] * 100).round(2)

    ## Add new columns for sentiment ranking
    df1["rk_Revenue"] = winsorize_zscore(df1["SALES_GROWTH"])
    df1["rk_NetProfit"] = winsorize_zscore(df1["NET_INC_GROWTH"])
    df1["rk_DiffConsensusRating"] = winsorize_zscore(
        pd.to_numeric(df1["Diff_eqy_rec_cons"])
    )
    df1["rk_DiffPctTargetPrice"] = winsorize_zscore(
        pd.to_numeric(df1["Diffpct_Target_Price"])
    )
    df1["rk_EarningsRevisionPct"] = winsorize_zscore(
        pd.to_numeric(df1["EarningsRevisionPct"])
    )
    df1["rk_WkRet"] = winsorize_zscore(pd.to_numeric(df1["WkRet"]))
    df1["rk_MthRet"] = winsorize_zscore(pd.to_numeric(df1["MthRet"]))
    df1["rk_QtrRet"] = winsorize_zscore(pd.to_numeric(df1["QtrRet"]))

    df1["Score"] = (
        df1[["rk_Revenue", "rk_NetProfit"]].mean(axis=1).fillna(0) / 3
        + df1[
            [
                "rk_DiffConsensusRating",
                "rk_DiffPctTargetPrice",
                "rk_EarningsRevisionPct",
            ]
        ]
        .mean(axis=1)
        .fillna(0)
        / 3
        + df1[["rk_WkRet", "rk_MthRet", "rk_QtrRet"]].mean(axis=1).fillna(0) / 3
    )
    return df1


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
        header_dict["Reported (Days Ago)"] = ""

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
        "Earnings Revision (%)",
        "Reported (Days Ago)",
    ]
    fdf_analyst2 = pd.concat(tdfs)
    fdf_analyst2["ID"] = fdf_analyst2["Name"]  # .apply(lambda x: x[:16])
    return fdf_analyst2[all_cols]  # fdf_analyst2_styled.render(), fdf_analyst2


def style_final_table(df, selected_period, sectors):
    fdf_analyst2 = df.copy()
    all_cols = [
        " ",
        "Weight (%)",
        f"Revenue {selected_period}(%)",
        f"NetProfit {selected_period}(%)",
        "Consensus Rating",
        "Target Price",
        "Earnings Revision (%)",
        "Reported (Days Ago)",
    ]
    fdf_analyst2.columns = all_cols

    # Final override
    #     u1 = pd.to_numeric(fdf_analyst2["Earnings Revision"]) < 0
    #     u2 = pd.to_numeric(fdf_analyst2["Earnings Revision"]) == 0
    #     u3 = pd.to_numeric(fdf_analyst2["Earnings Revision"]) > 0
    #     fdf_analyst2.loc[u1, "Earnings Revision"] = "Down"
    #     fdf_analyst2.loc[u2, "Earnings Revision"] = "NC"
    #     fdf_analyst2.loc[u3, "Earnings Revision"] = "Up"

    def highlight_table(df):
        style_df = df.copy()
        #     style_df["Name"] = ''
        style_df["Weight (%)"] = ""
        style_df["Reported (Days Ago)"] = ""
        style_df["Consensus Rating"] = pd.to_numeric(
            style_df["Consensus Rating"]
            .str.split("(")
            .apply(lambda x: x[1].split(")")[0]),
            errors="coerce",
        )
        style_df["Target Price"] = pd.to_numeric(
            style_df["Target Price"].str.split("(").apply(lambda x: x[1].split("%")[0]),
            errors="coerce",
        )
        style_cols = [
            f"Revenue {selected_period}(%)",
            f"NetProfit {selected_period}(%)",
            "Earnings Revision (%)",
        ] + ["Consensus Rating", "Target Price"]
        for col in style_cols:
            #             if col == "Earnings Revision":
            #                 x1 = style_df[col]
            #                 u_na =  x1.isna()
            #                 u1 =  x1 == "Up"
            #                 u2 =  x1 == "Down"
            #                 u3 =  x1 == "NC"
            #             else:
            x1 = pd.to_numeric(
                style_df[col].apply(lambda k: str(k).replace(",", "")), errors="coerce"
            )
            u_na = x1.isna()
            u1 = x1 > 0
            u2 = x1 < 0
            u3 = x1 == 0
            style_df.loc[u1, col] = "color: green; padding: 2px"  ##45f56b
            style_df.loc[u2, col] = "color: red; padding: 2px"  ##cc3004
            style_df.loc[u3, col] = 'background-color: ""'
            style_df.loc[u_na, col] = "font-weight: bold; color: #5e6469; padding: 2px"
        mask = style_df[" "].isin(sectors)
        style_df.loc[mask, :] = style_df.loc[mask, :].apply(
            lambda x: x + "; font-weight: bold; background-color: #FEEB75; padding: 2px"
        )
        return style_df

    fdf_analyst2_styled = fdf_analyst2.style.apply(highlight_table, axis=None)
    fdf_analyst2_styled = (
        fdf_analyst2_styled.set_properties(width="50px")
        .set_properties(**{"text-align": "right"})
        .set_properties(subset=[" "], **{"min-width": "150px", "text-align": "left"})
        .set_properties(
            subset=["Weight (%)"],
            **{"padding": "3px", "min-width": "70px", "text-align": "right"},
        )
        .set_properties(
            subset=[f"Revenue {selected_period}(%)"],
            **{"padding": "3px", "text-align": "right"},
        )
        .set_properties(
            subset=[f"NetProfit {selected_period}(%)"],
            **{"padding": "3px", "text-align": "right"},
        )
        .set_properties(
            subset=["Consensus Rating"],
            **{"padding": "3px", "min-width": "120px", "text-align": "right"},
        )
        .set_properties(
            subset=["Target Price"],
            **{"padding": "3px", "min-width": "120px", "text-align": "right"},
        )
        .set_properties(
            subset=["Earnings Revision (%)"],
            **{"padding": "3px", "text-align": "right"},
        )
        .set_properties(
            subset=["Reported (Days Ago)"],
            **{"padding": "3px", "min-width": "70px", "text-align": "right"},
        )
        .set_table_styles(
            [{"selector": ".row_heading, .blank", "props": [("display", "none;")]}]
        )
    )  #                 .set_properties(subset=['Name'], **{'width': '65px','text-align':'left'})
    return fdf_analyst2_styled.render(), fdf_analyst2


def compute_index_performance(universe, index_name, selected_period, as_of_date):
    """Compute QoQ/YoY Revenue, NetIncome growth; PE, Forward PE for an index
    universe: universe tickers list
    index_name: str
    selected_period: "QoQ/YoY"
    as_of_date: str/datetime (usually datetime.datetime.now())
    """
    dt = as_of_date
    if selected_period == "QoQ":
        fpo_upper = "-1Q"
    elif selected_period == "YoY":
        fpo_upper = "-4Q"

    d = {}
    d["Index"] = index_name
    """Revenue QoQ or YoY"""
    col = "SALES_REV_TURN"
    rev_df1 = query_fundamentals(
        universe,
        col,
        func=last_n,
        n_qtr=4,
        fpo=bq.func.range("-4Q", "0Q"),
        fpt="Q",
        fa_adjusted="Y",
        fa_act_est_data="A",
        fill="PREV",
        as_of_date=dt,
    )
    rev_df0 = query_fundamentals(
        universe,
        col,
        func=last_n,
        n_qtr=4,
        fpo=bq.func.range("-8Q", fpo_upper),
        fpt="Q",
        fa_adjusted="Y",
        fa_act_est_data="A",
        fill="PREV",
        as_of_date=dt,
    )
    d[f"Revenue {selected_period}(%)"] = rev_df1[col].sum() / rev_df0[col].sum() - 1

    """NetIncome QoQ or YoY"""
    col = "NET_INCOME"
    netinc_df1 = query_fundamentals(
        universe,
        col,
        func=last_n,
        n_qtr=4,
        fpo=bq.func.range("-4Q", "0Q"),
        fpt="Q",
        fa_adjusted="Y",
        fa_act_est_data="A",
        fill="PREV",
        as_of_date=dt,
    )
    netinc_df0 = query_fundamentals(
        universe,
        col,
        func=last_n,
        n_qtr=4,
        fpo=bq.func.range("-8Q", fpo_upper),
        fpt="Q",
        fa_adjusted="Y",
        fa_act_est_data="A",
        fill="PREV",
        as_of_date=dt,
    )
    d[f"NetIncome {selected_period}(%)"] = (
        netinc_df1[col].sum() / netinc_df0[col].sum() - 1
    )

    """PE and Forward PE"""
    """Custom calculations (removed temp)"""
#     mcap_df = query_field(universe, "CUR_MKT_CAP")
#     netinc_df_fwd = query_fundamentals(
#         universe,
#         col,
#         func=last_n,
#         n_qtr=4,
#         fpo=bq.func.range("0Q", "4Q"),
#         fpt="LTM",
#         fa_adjusted="Y",
#         fa_act_est_data="AE",
#         fill="PREV",
#         as_of_date=dt,
#     )
#     netinc_df_ann_curr = query_fundamentals(
#         universe,
#         col,
#         func=last_n,
#         n_qtr=4,
#         fpo=bq.func.range("-4Q", "0Q"),
#         fpt="LTM",
#         fa_adjusted="Y",
#         fa_act_est_data="A",
#         fill="PREV",
#         as_of_date=dt,
#     )
#     d["PE"] = mcap_df["CUR_MKT_CAP"].sum() / netinc_df_ann_curr["NET_INCOME"].sum()
#     d["Forward PE"] = mcap_df["CUR_MKT_CAP"].sum() / netinc_df_fwd["NET_INCOME"].sum()
    d["PE"] = get_indices_pe_ratio(as_of_date)[index_name]
    return d


def style_idx_perf_table(idx_perf_df, selected_period):
    idx_perf_df = idx_perf_df.copy()
    idx_perf_df[f"Revenue {selected_period}(%)"] = format_decimals(
        (idx_perf_df[f"Revenue {selected_period}(%)"] * 100).round(2)
    )
    idx_perf_df[f"NetIncome {selected_period}(%)"] = format_decimals(
        (idx_perf_df[f"NetIncome {selected_period}(%)"] * 100).round(2)
    )
    idx_perf_df["PE"] = format_decimals((idx_perf_df["PE"]).round(2))
#     idx_perf_df["Forward PE"] = format_decimals((idx_perf_df["Forward PE"]).round(2))
    idx_perf_df_styled = (
        idx_perf_df.style.set_properties(width="80px")
        .set_properties(subset=[f"PE"], **{"padding": "3px", "text-align": "left"})
        .set_properties(
            subset=[f"Revenue {selected_period}(%)"],
            **{"padding": "3px", "text-align": "right"},
        )
        .set_properties(
            subset=[f"NetIncome {selected_period}(%)"],
            **{"padding": "3px", "text-align": "right"},
        )
        .set_properties(subset=[f"PE"], **{"padding": "3px", "text-align": "right"})
#         .set_properties(
#             subset=[f"Forward PE"], **{"padding": "3px", "text-align": "right"}
#         )
        .set_table_styles(
            [{"selector": ".row_heading, .blank", "props": [("display", "none;")]}]
        )
    )
    return idx_perf_df_styled.render()
