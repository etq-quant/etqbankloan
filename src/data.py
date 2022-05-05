import bql
import pandas as pd

bq = bql.Service()


def query_field(univ, field, **kwargs):
    bq_data_item = getattr(bq.data, field)(**kwargs)
    reqq = bql.Request(univ, {field: bq_data_item})
    res = bq.execute(reqq)
    return res[0].df()


def query_fields(univ, field_confs, **kwargs):
    """Query multiple fields
    return dfs
    """
    ds = {}
    for field, conf in field_confs.items():
        ds[field] = getattr(bq.data, field)(**conf).dropna()
    reqq = bql.Request(univ, ds)
    res = bq.execute(reqq)
    dfs = [j.df() for j in res]
    return dfs


def get_index_df(index_name = "KLTEC Index"):
    bq = bql.Service()
    index_name = bq.univ.members(index_name)
    reqq = bql.Request(index_name, {"px": bq.data.id()})
    res = bq.execute(reqq)
    idx_df = res[0].df().reset_index()
    idx_df =  idx_df[["ID", "Weights"]].copy()
    idx_df["Name"] = idx_df["ID"]
    idx_df.columns = ["Ticker", "Weight", "Name"]
    return idx_df


def load_ssl_df(method="2"):
    """Load SSL (Selected Stocks List) (etiqa_sector.xlsx) with predefined sector labels and custom_sector.xlsx (new labels)"""
    if method=="1":
        ssl_df = pd.read_excel("etiqa_sector.xlsx")
        custom_impute_df = pd.read_excel("custom_sector.xlsx")
        ssl_df = ssl_df.drop_duplicates().set_index("ID")
        for _id, sector in zip(custom_impute_df["ID"], custom_impute_df["sector"]):
            ssl_df.loc[_id, "sector"] = sector
        ssl_df = ssl_df.reset_index()
    elif method=="2":
        ssl_df = pd.read_csv("static_xlsx/full_sector.csv")
    return ssl_df