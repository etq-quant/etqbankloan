import base64
from io import BytesIO

import pandas as pd


def encode_graph(fig):
    # tmpfile = BytesIO()
    # fig.to_image(tmpfile, format='png', bbox_inches='tight')
    val = fig.to_image(format='png')
    # encoded = base64.b64encode(tmpfile.getvalue()).decode('utf-8')
    encoded = base64.b64encode(val).decode('utf-8')
    fig_html = '<img src=\'data:image/png;base64,{}\'>'.format(encoded)

    return fig_html


def style_final_table(df):
    df = df.copy().reset_index()

    def highlight_table(df):
        style_df = df.copy()
        # Columns with No style:
        no_style_cols = ["Name"]
        for col in no_style_cols:
            style_df[col] = ""

        # Special style
        style_df["Consensus_*_Rating"] = pd.to_numeric(
            style_df["Consensus_*_Rating"]
            .str.split("(")
            .apply(lambda x: x[1].split(")")[0]),
            errors="coerce",
        )
        style_df["Target_*_Price(%)"] = pd.to_numeric(
            style_df["Target_*_Price(%)"].str.split("%").apply(lambda x: x[0]),
            errors="coerce",
        )

        # Highlight above zero
        style_cols = {
            "Revenue_*_YoY(%)": [0, 0, 0, False],
            "NetProfit_*_YoY(%)": [0, 0, 0, False],
            "Earnings_*_Revision(%)": [0, 0, 0, False],
            "Consensus_*_Rating": [0, 0, 0, False],
            "Target_*_Price(%)": [0, 0, 0, False],
            # "1W_*_RETURN(%)": [0, 0, 0, False],
            # "1M_*_RETURN(%)": [0, 0, 0, False],
            "Last_*_Reported": [31, 89, 90, True],
        }
        for col, threshs in style_cols.items():
            x1 = pd.to_numeric(
                style_df[col].apply(lambda k: str(k).replace(",", "")), errors="coerce"
            )
            u_na = x1.isna()
            # u1 = x1 >= threshs[0]
            # u2 = x1 < threshs[1]
            if threshs[3]:
                u1 = x1 <= threshs[0]
                u2 = x1 > threshs[1]
            else:
                u1 = x1 >= threshs[0]
                u2 = x1 < threshs[1]
            style_df[col] = ""
            style_df.loc[u1, col] = "color: green; padding: 2px"  # 45f56b
            style_df.loc[u2, col] = "color: red; padding: 2px"  # cc3004
            style_df.loc[u_na,
                         col] = "font-weight: bold; color: #5e6469; padding: 2px"
        # mask = style_df[" "].isin(sectors)
        # style_df.loc[mask, :] = style_df.loc[mask, :].apply(
        #     lambda x: x + "; font-weight: bold; background-color: #FEEB75; padding: 2px"
        # )
        return style_df

    all_cols = [
        "Name",
        "Revenue YoY(%)",
        "NetProfit YoY(%)",
        "Consensus Rating",
        "Target Price(%)",
        "Earnings Revision(%)",
        # "1W RETURN(%)",
        # "1M RETURN(%)",
        "Last Reported",
    ]
    fdf_analyst2 = df[all_cols].copy()
    fdf_analyst2.columns = [j.replace(" ", "_*_")
                            for j in fdf_analyst2.columns]

    fdf_analyst2_styled = fdf_analyst2.style.apply(highlight_table, axis=None)

    # fdf_analyst2_styled.columns = [j.replace(" ", "\n") for j in all_cols]
    fdf_analyst2_styled = fdf_analyst2_styled.set_properties(**{
        # "width": f"{width}px",
        "text-align": "right",
        'font-size': '8pt',
        'white-space': 'pre-wrap',
    })
    # .set_properties(subset=[" "], **{"min-width": "150px", "text-align": "left"})

    # bd_color =
    bd_color = "#ffdf80"
    id_width = 40
    width = round((480-id_width)/(len(all_cols)-1))
    idcol_width = f"{id_width}px"
    col_width = f"{width}px"
    props = {
        "Name": {"padding": "3px", "min-width": idcol_width, "text-align": "left", 'border-right': f'2px solid {bd_color}'},
        "Revenue_*_YoY(%)": {"padding": "3px", "min-width": col_width, "text-align": "center"},
        "NetProfit_*_YoY(%)": {"padding": "3px", "min-width": col_width, "text-align": "center", 'border-right': f'2px solid {bd_color}'},
        "Consensus_*_Rating": {"padding": "3px", "min-width": col_width, "text-align": "center"},
        "Target_*_Price(%)": {"padding": "3px",  "min-width": col_width, "text-align": "center"},
        "Earnings_*_Revision(%)": {"padding": "3px", "min-width": col_width, "text-align": "center", 'border-right': f'2px solid {bd_color}'},
        # "1W_*_RETURN(%)": {"padding": "3px",  "min-width": col_width, "text-align": "center"},
        # "1M_*_RETURN(%)": {"padding": "3px",  "min-width": col_width, "text-align": "center"},
        "Last_*_Reported": {"padding": "3px",  "min-width": col_width, "text-align": "center"},
    }
    fdf_analyst2_styled = fdf_analyst2_styled.format(
        precision=0, subset=["Last_*_Reported"])

    for key, val in props.items():
        fdf_analyst2_styled = fdf_analyst2_styled.set_properties(subset=[
                                                                 key], **val)
    fdf_analyst2_styled = fdf_analyst2_styled.set_table_styles(
        [
            {
                "selector": ".row_heading, .blank", "props": [
                    ("display", "none;"),
                    # ('border-bottom', '2px solid #e3987b')
                ]
            },
            {'selector': 'th', 'props': [
                ('font-size', '8pt'),
                ('text-align', 'center'),
                ('max-width', '35px'),
                ('border-bottom', '3px solid #ffd454')
            ]}
        ]
    )
    return fdf_analyst2_styled
