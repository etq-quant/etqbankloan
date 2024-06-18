import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def create_total_loans_growth_fig(total_loans_df):
    width = 1200
    height = 625

    # Assuming you have the DataFrame total_loans_df with "Total_Loans" column
    # and the YoY change is calculated and added as "YoY_Change" column

    # Step 6: Annotate the last value of each time series
    last_total_loans = total_loans_df["Total_Loans"].iloc[-1]
    last_yoy_change = total_loans_df["YoY_Change"].iloc[-1]
    last_total_loans_yref = (last_total_loans- total_loans_df["Total_Loans"].min())/( total_loans_df["Total_Loans"].max()- total_loans_df["Total_Loans"].min())
    last_yoy_change_yref = (last_yoy_change- total_loans_df["YoY_Change"].min())/( total_loans_df["YoY_Change"].max()- total_loans_df["YoY_Change"].min())

    # Step 3: Create a subplot with two y-axes
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Step 4: Add the main time series plot for "Total_Loans"
    fig.add_trace(go.Scatter(x=total_loans_df.index, y=total_loans_df["Total_Loans"], mode='lines', name='Total Loans', marker_color='blue',), secondary_y=False)

    # Step 5: Add the secondary y-axis plot for "YoY_Change"
    fig.add_trace(go.Scatter(x=total_loans_df.index, y=total_loans_df["YoY_Change"], mode='lines', line=dict(dash='dot'), opacity=0.85, name='YoY Change', marker_color='red',), secondary_y=True)

    # Step 6: Annotate the last value of each time series
    fig.add_annotation(x=total_loans_df.index[-1], y=last_total_loans_yref,
                       text=f'RM {last_total_loans:,.2f}b', yref="paper",
                       showarrow=False, arrowhead=1, yshift=10)

    fig.add_annotation(x=total_loans_df.index[-1], y=last_yoy_change_yref+0.05,
                       text=f'{last_yoy_change:.2f}%', yref="paper",
                       showarrow=False, arrowhead=1, yshift=-40, secondary_y=True)

    # Step 7: Update the layout to add axis labels and titles
    fig.update_layout(
        title='Time Series of Total Loans with YoY Change',
        xaxis_title='Date',
        yaxis_title='Total Loans',
        yaxis2_title='YoY Change (%)'
    )

    fig.update_layout(
            title='Total Loans Growth',
            yaxis=dict(
                title="Total Loans (RM'b)",
                titlefont=dict(
                    color="black"
                ),
                tickfont=dict(
                    color="black"
                )
            ),
            yaxis2=dict(
                title="YoY Growth (%)",
                anchor="x",
                overlaying="y",
                side="right",
                position=0.85
            )
        )

    fig.update_layout(
    #     template='plotly_white',
        template='ygridoff',
        xaxis = dict(
            dtick = "M12"
        )
    )
    fig.update_layout(
        autosize=False,
        width=width,
        height=height,)


    # Step 8: Show the plot
    return fig


def get_loans_growth_text(total_loans_df):
    lg_yoy = total_loans_df['Total_Loans'].pct_change(12).values[-1]
    lg_mom = total_loans_df['Total_Loans'].pct_change(1).values[-1]
    loans_growth_text = "Total Loans Growth (YoY) is {:.2%}; MoM Growth is {:.2%}.".format(lg_yoy, lg_mom)
    return loans_growth_text
