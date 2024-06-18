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

def plot_multi_lines(df, x_axis, y_axes):
    fig = go.Figure()
    for col in y_axes:
        fig.add_trace(go.Scatter(x=df[x_axis], y=df[col]/1000, name=col))
    fig.update_xaxes(dtick='M12')
    return fig

def create_fig_application(application_df_2):
    width = 1200
    height = 625
    purposes = [j for j in application_df_2.columns if j not in ['Transport Vehicles', 'Consumer Durable Goods', 'TOTAL']]
    
    fig_application = plot_multi_lines(application_df_2.reset_index(), 'Date', purposes)
    fig_application = add_title(fig_application, 'Application by Purpose', ylabel = "Amount (billion MYR)")
    fig_application = fig_application.update_layout(
        autosize=False,
        width=width,
        height=height,)
    return fig_application

def create_fig_approval(approval_df_2):
    width = 1200
    height = 625
    purposes = [j for j in approval_df_2.columns if j not in ['Transport Vehicles', 'Consumer Durable Goods', 'TOTAL']]
    
    fig_approval = plot_multi_lines(approval_df_2.reset_index(), 'Date', purposes)
    fig_approval = add_title(fig_approval, 'Approval by Purpose', ylabel = "Amount (billion MYR)")
    fig_approval = fig_approval.update_layout(
        autosize=False,
        width=width,
        height=height,)
    return fig_approval
