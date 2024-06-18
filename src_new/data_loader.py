from datetime import timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime
import pandas as pd
import requests

def get_year_month(df):  
    df['year_month'] = df['DATE'].map(lambda x: datetime(x.year, x.month, 1).date())
    return df

def filter_date(df,start_date, end_date, date_col='year_month'):
    return df.query('{dtcol} >= "{}" and {dtcol} <= "{}"'.format(start_date, end_date, dtcol=date_col))

def read_csv(file):
    '''
    with parse_date
    '''
    return pd.read_csv(file, parse_dates=['DATE'])

# os.listdir('data/Loans Stats/')

def trim_df(df):
    dff = df[list(range(15))].copy()
    dff.columns  = dff.iloc[3][:2].append(dff.iloc[2][2:]).tolist()
    dff = dff.iloc[4:].copy().dropna(subset=['Month'])
    dff['Year'] = dff['Year'].ffill()
    dff['year_month'] = dff.apply(lambda x: '{}-{}-01'.format(x['Year'], str(x['Month']).zfill(2)),1)
    return dff

def process_npl(npl):
    npl.columns= ['Date', 'Gross NPL']
    npl = npl.iloc[npl[npl['Date']=='Monthly'].index[0]+1:].dropna().copy()
    npl['Date'] = pd.to_datetime(npl['Date'])
    return npl

def process_opr(opr_df):
    def label(x):
        if x > 0:
            return 'hike'
        elif x< 0 :
            return 'cut'
        else:
            return 'stay'
    
    opr_df['label'] = opr_df['px_last'].diff().map(label) 
    opr_month = pd.pivot_table(opr_df, index='year_month', columns='label', values='px_last').fillna(0)\
                  .applymap(lambda x: 1 if x!= 0 else 0).reset_index()
    opr_month['label'] = opr_month.apply(lambda x: 'cut' if x['cut']==1 else ('hike' if x['hike'] == 1 else 'stay'), 1)
    return opr_df, opr_month


def query_approval_data():
    latest_year = datetime.now().year
    
    dfs = []
    for yr in range(2021, latest_year+1, 1):
        url = f"https://api.bnm.gov.my/public/msb/1.12/year/{yr}"
        content = requests.get(url, headers={'Accept': 'application/vnd.BNM.API.v1+json'})
        js = content.json()
        df = pd.DataFrame(js['data'])
        u1 = df['purpose'] == 'Jumlah / Total'
        tdf = df.loc[u1].copy()
        dfs.append(tdf)
    df = pd.concat(dfs)
    df['Date'] = df['year_dt'].astype(str) + df['month_dt'].astype(str).apply(lambda x:"-"+x.zfill(2)+"-01")
    df['Date'] = pd.to_datetime(df['Date'])
    df2 = df.loc[:, 'pur_sec':].set_index('Date')
    
    columns_mapper = {
        "pur_sec": "Purchase of Securities",
        "plb_tot": "Purchase of Fixed Assets (exLandBuilding)",
        "plb_ptv_tot": "Purchase of Transport Vehicles",
        "plb_ptv_pur_pas_car": "Purchase of Passenger Cars",
        "plb_ptv_oth": "PTV_Others",
        "plb_oth": "Others",
        "pur_res_pro": "Purchase of Residential Property",
        "pur_non_res_pro": "Purchase of Non-Residential Property",
        "per_use_tot": "Personal Uses Total",
        "per_use_pur_con_goo": "Purchase of Consumer Durable Goods",
        "per_use_oth": "Personal Uses Others",
        "cre_car": "Credit Card",
        "con": "Construction",
        "wor_cap": "Working Capital",
        "oth_pur": "Other Purposes",
        # "tot_loa_fin_apr": "Total Loan/Financing Approved",
        "tot_loa_fin_apr": "TOTAL",
    }
    df2 = df2.rename(columns=columns_mapper)
    for j in df2:
        df2[j] = df2[j].astype(float)
    return df2


def query_application_data():
    latest_year = datetime.now().year
    
    dfs = []
    for yr in range(2021, latest_year+1, 1):
        url = f"https://api.bnm.gov.my/public/msb/1.10/year/{yr}"
        content = requests.get(url, headers={'Accept': 'application/vnd.BNM.API.v1+json'})
        js = content.json()
        df = pd.DataFrame(js['data'])
        u1 = df['purpose'] == 'Jumlah / Total'
        tdf = df.loc[u1].copy()
        dfs.append(tdf)
    df = pd.concat(dfs)
    df['Date'] = df['year_dt'].astype(str) + df['month_dt'].astype(str).apply(lambda x:"-"+x.zfill(2)+"-01")
    df['Date'] = pd.to_datetime(df['Date'])
    df2 = df.loc[:, 'pur_sec':].set_index('Date')
    
    columns_mapper = {
        "pur_sec": "Purchase of Securities",
        "plb_tot": "Purchase of Fixed Assets (exLandBuilding)",
        "plb_ptv_tot": "Purchase of Transport Vehicles",
        "plb_ptv_pur_pas_car": "Purchase of Passenger Cars",
        "plb_ptv_oth": "PTV_Others",
        "plb_oth": "Others",
        "pur_res_pro": "Purchase of Residential Property",
        "pur_non_res_pro": "Purchase of Non-Residential Property",
        "per_use_tot": "Personal Uses Total",
        "per_use_pur_con_goo": "Purchase of Consumer Durable Goods",
        "per_use_oth": "Personal Uses Others",
        "cre_car": "Credit Card",
        "con": "Construction",
        "wor_cap": "Working Capital",
        "oth_pur": "Other Purposes",
        # "tot_loa_app": "Total Loan/Financing Applied",
        "tot_loa_app": "TOTAL",
    }
    df2 = df2.rename(columns=columns_mapper)
    for j in df2:
        df2[j] = df2[j].astype(float)
    return df2


class Data():
    def __init__(self):
        start_date = "2010-01-01"
        opr_df         = read_csv('data/opr/opr_rate.csv')
        end_date = opr_df["DATE"].max() - pd.offsets.BMonthEnd(1)
        
        self.opr_df         = read_csv('data/opr/opr_rate.csv').pipe(get_year_month).pipe(filter_date, start_date, end_date, "DATE")
        self.m3_price       = read_csv('data/opr/m3_rate.csv').pipe(get_year_month).pipe(filter_date, start_date, end_date, "DATE") 
        self.srr            = read_csv('data/opr/srr_rate.csv').pipe(get_year_month).pipe(filter_date, start_date, end_date, "DATE")
        self.opr_df, self.opr_month = process_opr(self.opr_df)
        
        self.m3_price_m = self.m3_price.groupby(['ID','year_month'])['px_last'].last().reset_index()
        self.srr_m = self.srr.groupby(['ID','year_month'])['px_last'].last().reset_index()

        npl = pd.read_excel("data_new/npl.xlsx", parse_dates=['Date'])[['Date', 'Gross NPL']]
        self.npl = npl.set_index("Date").loc['2010':].copy().reset_index()
        
        total_loans_df = pd.read_excel("data_new/npl.xlsx", parse_dates=['Date'])[['Date', 'Total Loans']]
        self.total_loans_df = total_loans_df.set_index("Date").loc['2006':].copy()
        self.total_loans_df.columns = ['Total_Loans']
        self.total_loans_df['Total_Loans'] = self.total_loans_df['Total_Loans']/1000
        self.total_loans_df['YoY_Change'] = self.total_loans_df['Total_Loans'].pct_change(12)*100

    def query_bnm(self):
        dt_str = datetime.now().strftime("%Y-%m-%d")
        approval_df = query_approval_data()
        approval_df.to_excel("data_new/approval.xlsx")
        approval_df.to_excel(f"data_new/{dt_str}_approval.xlsx")
        self.approval_df = approval_df

        appl_df = query_application_data()
        appl_df.to_excel("data_new/application.xlsx")
        appl_df.to_excel(f"data_new/{dt_str}_application.xlsx")
        self.application_df = appl_df

        self.parse_app_dfs()
        
    def parse_app_dfs(self):    
        cols = [
                'Purchase of Securities', 'Purchase of Transport Vehicles', 'Purchase of Passenger Cars', 'Purchase of Residential Property', 
                'Purchase of Non-Residential Property', 'Others', 'Personal Uses Total', 'Credit Card', 'Purchase of Consumer Durable Goods',
                'Construction', 'Working Capital', 'Other Purposes', 'TOTAL'
        ]
        rename_cols = ['Purchase of Securities', 'Transport Vehicles', 'Passenger Cars',
               'Residential Mortgages', 'Non-Residential Mortgages',
               'Purchase of Fixed Assets ', 'Personal Uses', 'Credit Card',
               'Consumer Durable Goods', 'Construction', 'Working Capital',
               'Other Purposes', 'TOTAL']
        self.application_df_2 = self.application_df[cols].copy()
        self.application_df_2.columns = rename_cols

        self.approval_df_2 = self.approval_df[cols].copy()
        self.approval_df_2.columns = rename_cols

    def subset_date(self, dt, loan_dt):
        self.approval_df = self.approval_df.loc[:loan_dt].copy()
        self.application_df = self.application_df.loc[:loan_dt].copy()
        self.approval_df_2 = self.approval_df_2.loc[:loan_dt].copy()
        self.application_df_2 = self.application_df_2.loc[:loan_dt].copy()
        self.total_loans_df = self.total_loans_df.loc[:loan_dt].copy()
        self.npl = self.npl.set_index('Date').loc[:loan_dt].reset_index().copy()
        
        self.srr_m['year_month'] = pd.to_datetime(self.srr_m['year_month'])
        self.srr_m = self.srr_m.set_index('year_month').loc[:dt].reset_index()
        self.m3_price_m['year_month'] = pd.to_datetime(self.m3_price_m['year_month'])
        self.m3_price_m = self.m3_price_m.set_index('year_month').loc[:dt].reset_index()
        self.m3_price['year_month'] = pd.to_datetime(self.m3_price['year_month'])
        self.m3_price = self.m3_price.set_index('year_month').loc[:dt].reset_index()
        self.opr_df['year_month'] = pd.to_datetime(self.opr_df['year_month'])
        self.opr_df = self.opr_df.set_index('year_month').loc[:dt].reset_index()


