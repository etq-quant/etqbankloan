import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def preproc_app_df(df):
    df = df.copy()
    df=df.drop(columns = ['TOTAL', 'Transport Vehicles', 'Consumer Durable Goods'])
    app_df_1 = df.pct_change(12).dropna().iloc[-4:]
    col_1 = app_df_1.T.columns[-1]
    new_col = [j.strftime("%b%y-YoY") for j in app_df_1.T.columns]
    table_application_1 = (  app_df_1.T.sort_values(col_1, ascending=False)*100).round(2)
    table_application_1.columns = new_col
    return table_application_1

def create_styled_table(app_df):
    app_df_1 = app_df.copy()
    app_df_1.index.name = "Purpose"
    app_df_2 = app_df_1.reset_index()
    idxs = app_df_2.index[app_df_2["Purpose"].isin(["Working Capital", "Credit Card", "Residential Mortgages", "Passenger Cars"])]

    fdf_styled = app_df_2.style.set_properties(**{
        "text-align": "right",
        'font-size': '8pt',
        'white-space': 'pre-wrap',
    })
    cols = fdf_styled.columns
    bd_color = "#acacad"
    props = {
        col: {"padding": "3px",  "text-align": "right", 'border-right': f'2px solid {bd_color}'}
        for col in cols
    }
    fdf_styled = fdf_styled.format(
        precision=2, subset=cols)

    for key, val in props.items():
        fdf_styled = fdf_styled.set_properties(subset=[key], **val)

    fdf_styled = fdf_styled.set_table_styles(
        [
            {
                "selector": ".row_heading, .blank", "props": [
                    ("display", "none;"),
                    # ('border-bottom', '2px solid #e3987b')
                ]
            },
            {'selector': 'th', 'props': [
                ('font-size', '8pt'),
                ('text-align', 'right'),
                ('max-width', '35px'),
                ('border-bottom', '3px solid #acacad')
            ]}
        ]
    )
    idx = pd.IndexSlice
    fdf_styled = fdf_styled.set_properties(**{"background-color": "#d4e1ff"}, subset=idx[idxs,:])
    return fdf_styled
