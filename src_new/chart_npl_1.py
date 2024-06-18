import pandas as pd
import plotly.graph_objects as go

def create_overview_fig(m3_price_m, srr_m, npl, opr_df):
    width = 1200
    height = 625
    fig = go.Figure()
    fig.add_trace(go.Scatter(x = npl['Date'],
                             y = npl['Gross NPL'],
                             marker_color='#7f7f7f',
                             name = 'Gross NPL (%)',
                            yaxis="y2"))

    fig.add_trace(go.Scatter(x = m3_price_m['year_month'],
                             y = m3_price_m['px_last'],
                             marker_color='#ff7f0e',
                             name = 'M3 YoY (%)'),
                 )

    fig.add_trace(go.Scatter(x = srr_m['year_month'],
                             y = srr_m['px_last'],
                             marker_color='red',
                             name = 'SRR (%)'),
                 )


    fig.add_trace(go.Scatter(x = opr_df['DATE'],
                             y = opr_df['px_last'],
                             marker_color='blue',
                             mode='lines',
                             name = 'OPR (%)',
                             yaxis="y2"),
                 )

    fig.update_layout(
        title='Gross NPL',
        yaxis=dict(
            title="M3 / SRR (%)",
            titlefont=dict(
                color="red"
            ),
            tickfont=dict(
                color="red"
            )
        ),
        yaxis2=dict(
            title="Gross NPL / OPR (%)",
            anchor="x",
            overlaying="y",
            side="right",
            position=0.85
        )
    )

    fig.update_layout(
        template='ygridoff',
        xaxis = dict(
            dtick = "M12"
        )
    )
    fig.update_layout(
        autosize=False,
        width=width,
        height=height,)
    return fig


def get_npl_text(m3_price_m, npl):
    x1 = m3_price_m.tail(13)
    d1, d2 = x1.year_month.values[0], x1.year_month.values[-1]
    m3_1, m3_2 = x1.loc[:, "px_last"].values[0], x1.loc[:, "px_last"].values[-1]
    
    x1 = npl.tail(13)
    d1, d2 = x1.Date.values[0], x1.Date.values[-1]
    npl_1, npl_2 = x1.loc[:, "Gross NPL"].values[0], x1.loc[:, "Gross NPL"].values[-1]
    
    if npl_2 >= npl_1:
        compare = "higher"
    else:
        compare = "lower"
    overview_text = "M3 YoY Growth is {:.2f}%. Non-performing Loan (NPL) is {:.2f}%, which is {} compared to last year ({:.2f}%)".format(m3_2, npl_2, compare, npl_1)
    return overview_text, npl_2, npl_1

