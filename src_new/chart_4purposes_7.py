import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def add_title(fig, title, ylabel="Amount (billion) [MYR]"):
    fig.update_layout(template='ygridoff',
                      title=title,
                    yaxis=dict(
                    title=ylabel,
                ),
                )
    return fig


def plot_multi_lines_2(df, x_axis, y_axes):
    colors = ["rgb(252, 32, 3)", "rgb(255, 172, 161, 0.5)", "rgb(0, 176, 88)", "rgb(149, 240, 194, 0.5)"]
    opacitys = [1, 0.5, 1, 0.5]
    fig = go.Figure()
    for col, color, opacity in zip(y_axes, colors, opacitys):
        fig.add_trace(go.Scatter(x=df[x_axis], y=df[col]/1000, name=col, 
                                 line=dict(
                                     # opacity=opacity,
                                     color=color
                                 )
                                )
                     )
    fig.update_xaxes(dtick='M12')
    return fig

def create_single_purpose_loan_fig(application_df_2, approval_df_2, dts_1, dts_2, col, smooth=True):
    width = 1200
    height = 625

    appl_df = application_df_2.reset_index().copy()
    appr_df = approval_df_2.reset_index().copy()
    p_df = appl_df[['Date',col]].rename(columns={col:'Application'})\
                        .merge(appr_df[['Date',col]].rename(columns={col:'Approval'})).copy()
    if smooth:
        p_df['Application (Avg)'] = p_df['Application'].rolling(12).mean()
        p_df['Approval (Avg)'] = p_df['Approval'].rolling(12).mean()
    fig = plot_multi_lines_2(p_df, 'Date', ['Application (Avg)', 'Application','Approval (Avg)', 'Approval'])
    fig = add_title(fig, col + " (Rolling 12 Months Average)")

    fillcolor = "#a7becf"
    layer = "below"
    dt1, dt2 = dts_1
    dt1b, dt2b = dts_2
    fig = fig.update_layout(
        autosize=False,
        width=width,
        height=height,)
    return fig

def create_4_purposes_figs(application_df_2, approval_df_2):
    app_cols = ["Passenger Cars", "Residential Mortgages", "Credit Card", "Working Capital"]
    app_cols_2 = ["Passenger Cars", "Residential Mortgages", "Credit Card", "Working Capital"]
    
    dates_1 = [("2013-01-01", "2013-12-31"), ("2013-10-01", "2014-12-31"), ("2017-01-01", "2018-12-31"), ("2015-12-01", "2016-09-30")]
    dates_2 = [("2021-01-01", "2022-01-31"), ("2021-01-01", "2022-01-31"), ("2021-01-01", "2022-01-31"), ("2020-02-01", "2020-12-31")]
    figs = []
    for col, dts_1, dts_2 in zip(app_cols, dates_1, dates_2):
        fig_ = create_single_purpose_loan_fig(application_df_2, approval_df_2,  dts_1, dts_2, col)
        figs.append(fig_)
    return figs

def create_4_purposes_texts(appl_df_1, appr_df_1):
    app_cols = ["Passenger Cars", "Residential Mortgages", "Credit Card", "Working Capital"]
    purpose_texts = []
    for col in app_cols:
        v1 = appl_df_1.loc[col, :][-1]
        v2 = appr_df_1.loc[col, :][-1]
        txt = "{} Application YoY Change is {:.2f}%; Approval YoY Change is {:.2f}%".format(col, v1, v2)
        purpose_texts.append(txt)
    return purpose_texts
