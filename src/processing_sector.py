import bql
import numpy as np
import pandas as pd


def style_sector_table(df, sector_list):
    sectors = sector_list
    all_cols = df.columns
    
    def highlist_sector(x):
        df = x.copy()
        mask = df['ID'].isin(sectors)
        df.loc[mask, :] = 'font-weight: bold; background-color: #FEEB75; padding: 5px'
        df.loc[~mask,:] = 'background-color: ""'
        return df    

    df_styled = df.style.apply(highlist_sector, axis=None).set_properties(width='50px')\
                .set_properties(**{'text-align':'left'})\
                .set_properties(subset=['ID'], **{'width': '15px','text-align':'left'})\
                .set_properties(subset=['Name'], **{'width': '65px','text-align':'left'})\
                .set_properties(subset=['Weight (%)'], **{'width': '15px','text-align':'right'})\
                .set_table_styles([{'selector': '.row_heading, .blank', 'props': [('display', 'none;')]}])
    return df_styled.render()


def get_index_sector_df(idx_df, ssl_df):
    idx_df = idx_df.copy()
    ssl_df = ssl_df.copy()
    ssl_df["ID2"] = ssl_df["ID"].str.upper()
    idx_df["Ticker2"] = idx_df["Ticker"].str.upper()
    mdf = pd.merge(idx_df, ssl_df, left_on="Ticker2", right_on="ID2", how = "left")
    mdf2 = mdf[["Ticker", "Name", "Weight", "sector"]]
    mdf2.columns = ["ID", "Name", "Weight", "Sector"]
    mdf3 = mdf2.set_index("ID")
    mdf3["Sector"] = mdf3["Sector"].fillna("Z - Sector Not Defined")
    mdf3["Weight (%)"] = (mdf3["Weight"]).round(2).apply(lambda x: "{:.2f}".format(x))
    mdf3 = mdf3.drop_duplicates()
    
    mdf3 = mdf3.sort_values(["Sector", "Weight"], ascending=[True, False])
    sectors = mdf3.Sector.unique()
    tdfs = []
    for sector in sectors:
        tdf = mdf3.query('Sector =="{}"'.format(sector)).drop_duplicates().copy()
        sum_weight = tdf["Weight"].sum()
        tdf2 = tdf.copy()
        tdf.loc[sector, :] = np.nan
        tdf.loc[sector, "Weight (%)"] =  "{:.2f}".format(sum_weight)
        tdf.loc[sector, :] = tdf.loc[sector, :].fillna("")
        tdf3 = tdf.loc[[sector], :].append(tdf2)
        tdfs.append(tdf3.copy())
    fdf = pd.concat(tdfs).reset_index()
    fdf2 = style_sector_table(fdf[["ID", "Name", "Weight (%)"]], sector_list=sectors)
    return fdf2, fdf