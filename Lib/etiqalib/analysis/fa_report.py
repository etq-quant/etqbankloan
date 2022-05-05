import pandas as pd
import numpy as np

from ..preprocessing import *
from ..bqfields import BaseGetData


def process_revenue(df):
    '''
    
    
    '''
    df['Revenue'] = df['net_rev'].fillna(df['sales_rev_turn'])
    return df.drop(['net_rev','sales_rev_turn'],1)

def get_FA(bq, tickers, fa_conf, currency_mapping, **params):
    '''
    eg. start_date: last_year_quarter, end_date: this_quarter_date
    example config
    fa_conf = {'net_rev': {'params': {'adj': 'Y', 'fa_act_est_data': 'A'},
                          'field_id': 'net_rev'},
               'sales_rev_turn': {'params': {'adj': 'Y', 'fa_act_est_data': 'A'},
                                  'field_id': 'sales_rev_turn'},
               'NetProfit': {'params': {'adj': 'Y', 'fa_act_est_data': 'A'},
                             'field_id': 'net_income'}}
    '''
    ticker_curr = pd.DataFrame({'tickers':[[i] for i in tickers], 'currency':[currency_mapping.get(i.split()[1]) for i in tickers]})
    ticker_curr = ticker_curr.groupby('currency')['tickers'].sum().reset_index()

    df_list = []
    for cur, stocks in ticker_curr.values:
        params1 = dict(currency=cur,**params) #fpr=bq.func.range(start=start_date, end=end_date), 
        fields_and_params = {k: getattr(bq.data, v['field_id'])(**params1, **v['params']) for k,v in fa_conf.items()}
        df = BaseGetData(bq).get_data(stocks, fields_and_params)
        if 'sales_rev_turn' in fields_and_params:
            df = process_revenue(df)
            
        df_list.append(df)
    
    ## do NOT dropna(subset=['REVISION_DATE']), needed for semiannuals detection
    return pd.concat(df_list)


def _detect_semiannuals(df, by='Revenue'):
    '''
    Use Revenue column as detection column from quarterly downloaded data (1 year interval)
    '''
    dff = df.reset_index().groupby('ID')[by].apply(lambda x: len(x) - x.isnull().sum()).reset_index(name='not_null')    
    semiannual_companies = dff[dff['not_null']<=1]['ID'].tolist()
    return semiannual_companies

def filter_reported_and_period(FA_df, fpt_col='quarter', now='2020Q2', then='2019Q2'):
    '''
    Drop Stocks that is not reported in *now* column,
    Filter comparables: *now* vs *then*
    Combine rows by *fpt_col* using aggsum()
    '''
    drop_cols = ['REVISION_DATE','CURRENCY','AS_OF_DATE']
    group_cols = ['ID',fpt_col]
    numeric_cols = FA_df.select_dtypes(include=np.number).columns
    
    reported_stocks = FA_df[FA_df[fpt_col]==now].dropna(subset=numeric_cols).ID.unique()
    
    FA_full = FA_df[(FA_df.ID.isin(reported_stocks))&(FA_df[fpt_col].isin([now, then]))].copy()
    return FA_full
    FA_full = FA_full.groupby(group_cols)[numeric_cols].sum().reset_index()
    return FA_full

def FA_changes(FA_df, fpt_col='quarter',label='QoQ', group_level='ID'):
    '''
    Compute difference and pct_change
    '''
    group_cols = [group_level,fpt_col]
    
    numeric_cols = FA_df.select_dtypes(include=np.number).columns
    FA_long = pd.melt(FA_df, id_vars=group_cols, value_vars=numeric_cols, var_name='FA')
    FA_long = FA_long.groupby(group_cols +['FA'])['value'].sum().reset_index()
    
    FA_long[label] = FA_long.groupby([group_level,'FA'])['value'].diff()
    
    FA_long[label+' (%)'] = FA_long.groupby([group_level,'FA'])['value'].apply(lambda x: (x-x.shift())/abs(x.shift())*100)
    
    FA_long.dropna(subset=[label], inplace=True)
    FA_chg = pd.pivot_table(FA_long, 
                             index=[group_level, fpt_col], 
                             columns=['FA'], 
                             values=[label,label+' (%)']).reset_index()
    FA_chg.columns = [j + '_' + i if j else i for i,j in FA_chg.columns]
    return FA_chg


def process_growh_fields(df, quarterly=False, semiannually=False, annually=False, rev_col='revenue', prof_col='net_income'):
    '''
    yoy growth. Window depends on Financial reporting period.
    '''
    if quarterly:
        window = 4
    elif semiannually:
        window = 2
    elif annually:
        window = 1
        
    df['topline_growth'] = df[rev_col].pct_change(window)
    df['bottomline_growth'] = df[prof_col].pct_change(window)
    return df

def get_pct_reported(index_members_df, FA_df, semiannual_companies_not_this_q, by='Index'):
    reported_by_index = index_members_df.copy().merge(FA_df, on='ID').groupby(by)['ID'].count().reset_index(name='reported')
    
    members_this_q = index_members_df[~index_members_df['ID'].isin(semiannual_companies_not_this_q)]\
                                    .groupby(by)['ID'].count().reset_index(name='exist')
#     return reported_by_index, members_this_q
    pct_reported_df = reported_by_index.merge(members_this_q, on=by)
    pct_reported_df['% Reported'] = pct_reported_df['reported']/pct_reported_df['exist']
    return pct_reported_df.drop(['reported','exist'],1)