import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots



def create_loan_app_fig(application_df_1, approval_df_1, opr_df, ref_start_loan_date = pd.to_datetime("2021-07-01")):
    width = 1200
    height = 625
    
    appl_df = application_df_1.reset_index().copy()
    appr_df = approval_df_1.reset_index().copy()
    
    fig2 = go.Figure()
    
    fig2.add_trace(go.Scatter(x = appl_df['Date'],
                             y = appl_df['TOTAL']/1000,
                             marker_color='#ffcfcf',
                             name = 'Application'))
        
    fig2.add_trace(go.Scatter(x = appl_df['Date'],
                             y = appl_df['TOTAL'].rolling(12).mean()/1000,
                             marker_color='red',
                             name = 'Application (Avg)'))
    
    fig2.add_trace(go.Scatter(x = appr_df['Date'],
                             y = appr_df['TOTAL']/1000,
                             marker_color='#b6fac1',
                            name = 'Approval'),
                 )
    
    fig2.add_trace(go.Scatter(x = appr_df['Date'],
                             y = appr_df['TOTAL'].rolling(12).mean()/1000,
                             marker_color='green',
                            name = 'Approval (Avg)'),
                 )

    fig2.add_trace(go.Scatter(x = opr_df.query(f'DATE >= "{ref_start_loan_date}"')['DATE'],
                             y = opr_df.query(f'DATE >= "{ref_start_loan_date}"')['px_last'],
                             marker_color='blue',
                             mode='lines',
                             name = 'OPR (%)',
                            yaxis="y2"),
                 )

    fig2.update_layout(
        title='Total Bank Loan Application and Approval (Rolling 12 Months Average)',
        yaxis=dict(
            title="Application / Approval  (billion) [MYR]",
            titlefont=dict(
                color="#7f7f7f"
            ),
            tickfont=dict(
                color="#7f7f7f"
            )
        ),
        yaxis2=dict(
            title="OPR (%)",
            titlefont=dict(
                color="blue"
            ),
            tickfont=dict(
                color="blue"
            ),
            anchor="x",
            overlaying="y",
            side="right",
            position=0.85
        )
    )

    fig2.update_layout(
        template='ygridoff',
        xaxis = dict(
            dtick = "M12"
        )
    )
    fig2.update_layout(
        autosize=False,
        width=width,
        height=height,)
    return fig2


def get_total_applr_text(application_df_1, approval_df_1):
    x1 = application_df_1.tail(13)
    appl_1, appl_2, appl_3 = x1.loc[:, "TOTAL"].values[0], x1.loc[:, "TOTAL"].values[-2], x1.loc[:, "TOTAL"].values[-1]
    appl_yoy = appl_3/appl_1-1
    appl_mom = appl_3/appl_2-1
    
    x1 = approval_df_1.tail(13)
    appr_1, appr_2, appr_3 = x1.loc[:, "TOTAL"].values[0], x1.loc[:, "TOTAL"].values[-2], x1.loc[:, "TOTAL"].values[-1]
    appr_yoy = appr_3/appr_1-1
    appr_mom = appr_3/appr_2-1
    
    latest_month = x1.index[-1].strftime("%B %Y")
    loan_text = "As of {}, Total Bank Loan Application MoM and YoY change are {:.2f}% and {:.2f}% respectively;".format(latest_month, appl_mom*100, appl_yoy*100)
    loan_text += " Total Bank Loan Approval MoM and YoY change are {:.2f}% and {:.2f}% respectively.".format(appr_mom*100, appr_yoy*100)
    return loan_text
