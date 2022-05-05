import numpy as np
import pandas as pd

def format_decimals(sr, n=2, use_plus=False, zfill=False):
    """format numeric pandas series to string"""
    if zfill:
        fmt = "{:+06.2f}"
        return sr.apply(lambda x: fmt.format(x))

    if use_plus:
        fmt = "{:+,." + str(n) + "f}"
        return sr.apply(lambda x: fmt.format(x))
    else:
        fmt = "{:,." + str(n) + "f}"
        return sr.apply(lambda x: fmt.format(x))


def parse_fig_to_html(fig):
    """render plotly fig to html"""
    return fig.to_html(
        default_width="55em",
        default_height="35em",
        full_html=False,
        config={"staticPlot": True, "displayModeBar": False},
    )


def add_sector_header(df, name_column, sector_column):
    """Add sector header in each row"""
    tdf = df.copy()
    tdf = tdf.sort_values(sector_column)
    sectors = tdf[sector_column].unique()
    tdfs = []
    for sector in sectors:
        tdf2 = tdf.loc[tdf.Sector == sector,:].copy()
        header_dict = {}
        for col in tdf2:
            header_dict[col] = ""
        header_dict[name_column] = sector
        header_df = pd.DataFrame(header_dict, index=[sector])
        tdf3 = pd.concat([header_df, tdf2], axis=0)
        tdfs.append(tdf3.copy())
    top50_whdf = pd.concat(tdfs)
    return top50_whdf


def style_with_header_df(wh_df, name_col, sector_col, left_cols=[], right_cols=[]):
    """Style the `with_header_df`"""
    wh_df = wh_df.copy()
    wh_df = wh_df.rename(columns={name_col: " "})
    sectors = wh_df[sector_col].unique().tolist()
    sectors.remove("")
    def highlight_table(wh_df):
        style_df = wh_df.copy()
        mask = style_df[" "].isin(sectors)
        for col in style_df.columns:
            style_df[col] = ""
        style_df.loc[mask, :] = style_df.loc[mask, :].apply(
            lambda x: x + "; font-weight: bold; background-color: #FEEB75; padding: 2px"
        )
        return style_df
    
    wh_df = wh_df.drop(columns = [sector_col])
    print(wh_df.columns)
    wh_df_styled = wh_df.style.apply(highlight_table, axis=None)
    wh_df_styled = wh_df_styled.set_properties(width="80px")
    for left_col in left_cols:
        if left_col in [sector_col]:
            continue
        wh_df_styled = wh_df_styled.set_properties(subset=[left_col], **{"padding": "3px", "min-width": "50px", "text-align": "left"})
        
    for right_col in right_cols:
        if right_col in [sector_col]:
            continue
        wh_df_styled = wh_df_styled.set_properties(subset=[right_col], **{"padding": "3px", "min-width": "100px", "text-align": "right"})
        
    wh_df_styled = wh_df_styled.set_table_styles(
            [{"selector": ".row_heading, .blank", "props": [("display", "none;")]}]
        )
    return wh_df_styled

        
        
        