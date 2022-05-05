from datetime import datetime
import pandas as pd
import numpy as np
import bql

zscore = lambda x: (x - x.mean()) / x.std()
robustscaler = lambda x: (x - np.median(x)) / (np.percentile(x, 75) - np.percentile(x, 25))

def get_data(bq, security, fields, reset_index=False):
    drop_items = ['ORIG_IDS','ITERATION_DATE','ITERATION_ID', 'CURRENCY','REVISION_DATE']
    
    request =  bql.Request(security, fields)
    response = bq.execute(request)
    data = [r.df().drop(drop_items, axis='columns', errors='ignore') for r in response]
    if reset_index:
        data = [df.rename(columns={'AS_OF_DATE':'DATE'}).reset_index().set_index(['ID', 'DATE']) for df in data]
    return pd.concat(data, axis=1)

def stlye_value(bq, universe, date_range):
    '''
    Q1 cheapest
    '''
    fields= {'forward12M_earn_yld': bq.data.earn_yld(dates=date_range, fpo='1',fpt='A', fa_act_est_data='E', fill='PREV'),
             'trailing12M_book_yld': 1/bq.data.BOOK_VAL_PER_SH(dates=date_range,fpo='0', fpt='LTM', fa_act_est_data='AE', fill='PREV')}
    
    df = get_data(bq, universe, fields, True).reset_index().dropna()
    
    for c in df.select_dtypes(include=np.number).columns:
#         df[c+'_q'] = df.groupby(['DATE'])[c].apply(lambda x: pd.qcut(x, 4, labels=['Q1', 'Q2', 'Q3', 'Q4']).map(str))    
#         df[c+'_log'] = df[c].transform(lambda x : np.log(x))
        df[c+'_zscore'] = df.groupby(['DATE'])[c].transform(zscore) #+'_log'
    
    df['value_zscore'] = df[[c for c in df.columns if '_zscore' in c]].mean(axis=1)
    return df 

def style_growth(bq, universe, date_range):
    '''
    Q1 highest growth
    '''
    fields = {
        'next2Y_EPS':bq.data.IS_EPS(dates=date_range, fpo='2',fpt='A', fa_adjusted='Y', fa_act_est_data='E', fill='PREV'),
        'next1Y_EPS':bq.data.IS_EPS(dates=date_range, fpo='1',fpt='A', fa_adjusted='Y', fa_act_est_data='E', fill='PREV'),
        'thisY_EPS':bq.data.IS_EPS(dates=date_range, fpo='0',fpt='A', fa_adjusted='Y', fa_act_est_data='AE', fill='PREV'),
        'last1Y_EPS':bq.data.IS_EPS(dates=date_range, fpo='-1',fpt='A', fa_adjusted='Y', fa_act_est_data='A', fill='PREV'),
        'last2Y_EPS':bq.data.IS_EPS(dates=date_range, fpo='-2',fpt='A', fa_adjusted='Y', fa_act_est_data='A', fill='PREV'),
        'last3Y_EPS':bq.data.IS_EPS(dates=date_range, fpo='-3',fpt='A', fa_adjusted='Y', fa_act_est_data='A', fill='PREV'),
        'ebitda_margin':bq.data.EBITDA_MARGIN(dates=date_range, fpo='-1',fpt='A', fa_adjusted='Y', fa_act_est_data='A', fill='PREV'),
        'ebitda_margin_2Y':bq.data.EBITDA_MARGIN(dates=date_range, fpo='-2',fpt='A', fa_adjusted='Y',fa_act_est_data='A', fill='PREV')
    }
    
    df = get_data(bq, universe, fields, True).reset_index().dropna()
    df['next2Y_EPS_avg'] = (df['next1Y_EPS']+df['next2Y_EPS'])/2
    df['this2Y_EPS_avg'] = (df['thisY_EPS']+df['last1Y_EPS'])/2
    df['last2Y_EPS_avg'] = (df['last2Y_EPS']+df['last3Y_EPS'])/2

    df['next2Y_avg_EPSg'] = df['next2Y_EPS_avg']/df['this2Y_EPS_avg'] - 1
    df['last2Y_avg_EPSg'] = df['this2Y_EPS_avg']/df['last2Y_EPS_avg'] - 1
    df['last2Y_ebitda_margin_chg'] = df['ebitda_margin']/df['ebitda_margin_2Y'] - 1
    
    for c in ['next2Y_avg_EPSg','last2Y_avg_EPSg','last2Y_ebitda_margin_chg']:
        df[c+'_zscore'] = df.groupby(['DATE'])[c].transform(zscore) #+'_log'
        
#     df['growth_q'] = df.groupby(['DATE'])['growth'].apply(lambda x: pd.qcut(x, 4, labels=['Q4', 'Q3', 'Q2', 'Q1']).map(str))
    df['growth_zscore'] = df[[c for c in df.columns if '_zscore' in c]].mean(axis=1)
    df['growth_zscore'] = df['next2Y_avg_EPSg'].map(lambda x: -9 if x <0 else x)

    return df

def style_yield(bq, universe, date_range):
    '''
    Q1 highest yield
    #no yield fill with 0?
    '''
    fields = {'FCF': bq.data.CF_FREE_CASH_FLOW(dates=date_range,  fpt='LTM', fill='PREV'),
              'mkt_cap':bq.data.cur_mkt_cap(dates=date_range, fill='PREV'),
              'IS_DIV_PER_SHR': bq.data.IS_DIV_PER_SHR(as_of_date=date_range, fpo="2", fpt='A', fill='PREV'),
              'px_last': bq.data.px_last(dates=date_range, fill='PREV')} 
    
    df = get_data(bq, universe, fields, True).reset_index()
    df['year_month'] = df['DATE'].map(lambda x: '{}-{:02}'.format(x.year, x.month))
    df = df.drop('PERIOD_END_DATE',1).groupby(['ID','year_month'])[['IS_DIV_PER_SHR','px_last','FCF','mkt_cap']].max().reset_index()
    df['forward_dvd_yield'] = (df['IS_DIV_PER_SHR'] / df['px_last'])*100
    df['fcf_yield'] = (df['FCF'] / df['mkt_cap'])*100
    
    df['fcf_yield_zscore'] = df.groupby('year_month')['fcf_yield'].transform(zscore)
    df['div_zscore'] = df.groupby('year_month')['forward_dvd_yield'].transform(zscore)

    df['yield_zscore'] = df[[c for c in df.columns if '_zscore' in c]].mean(axis=1)
    return df

def style_momentum(bq, universe, date_range):
    '''
    Q1 highest return/momentum
    3 & 12-month total return
    highly skewed. use robustscaler here ?
    '''
    start = date_range.parameters['start'] 
    end = date_range.parameters['end'] 
    frq = date_range.parameters['frq']
    
    date_range2 = bq.func.range(start=datetime(start.year-1, start.month, start.day), end=end, frq=frq)
    
    fields = {'px_last': bq.data.px_last(dates=date_range2, fill='PREV')}
    
    df = get_data(bq, universe, fields).reset_index()
    df['3M ret'] = df.groupby('ID')['px_last'].pct_change(3)
    df['6M ret'] = df.groupby('ID')['px_last'].pct_change(6)
    df['12M ret'] = df.groupby('ID')['px_last'].pct_change(12)
    df.dropna(inplace=True)
    
    df['3M ret scaled'] = df.groupby('DATE')['3M ret'].transform(robustscaler)
    df['3M ret_zscore'] = df.groupby('DATE')['3M ret scaled'].transform(zscore)
    df['6M ret scaled'] = df.groupby('DATE')['6M ret'].transform(robustscaler)
    df['6M ret_zscore'] = df.groupby('DATE')['6M ret scaled'].transform(zscore)
    df['12M ret scaled'] = df.groupby('DATE')['12M ret'].transform(robustscaler)
    df['12M ret_zscore'] = df.groupby('DATE')['12M ret scaled'].transform(zscore)

    df['momentum_zscore'] = df[['3M ret_zscore','6M ret_zscore','12M ret_zscore']].mean(axis=1)
    df['momentum_zscore'] = df.apply(lambda x: -9 if ((x['3M ret_zscore']<0) or (x['6M ret_zscore']<0) or (x['12M ret_zscore']<0)) else x['momentum_zscore'], 1)

    return df

def style_revision(bq, universe, date_range):
    '''
    Difficulties in not having much data. the rules change accordingly to fit Malaysia's market
    '''
    est_params = {'fpo': '1', 'fpt': 'Q', 'fill': 'prev'}
    rev_stat_params = {'fa_stat_revision':'COUNT'}
    rev_win_params = {'fa_revision_window': '4W'}
    bql_field = bq.data.net_income(currency='MYR')
    fields = {'Earnings Rev Up': bql_field.with_additional_parameters(dates=date_range, **est_params, **rev_win_params, fa_stat_revision='NETUP'),
              'Earnings Rev Down': bql_field.with_additional_parameters(dates=date_range, **est_params, **rev_win_params, fa_stat_revision='NETDN'),
              'Revision Count':bql_field.with_additional_parameters(dates=date_range, **est_params,**rev_win_params, **rev_stat_params)}
    df = get_data(bq, universe, fields, True).reset_index().fillna(0)
    df['Earnings %Rev Up'] = df['Earnings Rev Up']/df['Revision Count'] + -1*df['Earnings Rev Down']/df['Revision Count']
    return df.fillna(0).drop('PERIOD_END_DATE',1)

def style_quality(bq, universe, date_range):
    '''
    ROE not using future?
    '''
    fields = {'ROE': bq.data.RETURN_COM_EQY(dates=date_range, fpt='Q', fill='PREV'),
              'ROIC':bq.data.return_on_inv_capital(dates=date_range, fpt='Q', fill='PREV'),
              'FCF per share': bq.data.FREE_CASH_FLOW_PER_SH(dates=date_range, fill='PREV')}
    df = get_data(bq, universe, fields, True).reset_index().fillna(0)
    
    for c in df.select_dtypes(include=np.number).columns:
        df[c+'_zscore'] = df.groupby(['DATE'])[c].transform(zscore)
    
    df['quality_zscore'] = df[[c for c in df.columns if '_zscore' in c]].mean(axis=1)
#     df['ROE_q'] = df.groupby(['DATE'])['ROE'].apply(lambda x: pd.qcut(x, 4, labels=['Q4', 'Q3', 'Q2', 'Q1']).map(str))
    return df



def view_stocks_by_styles(stock_styles_full, stock_styles_w_style, agg_date_col = 'year_month', date_col = 'DATE'):
    from ipywidgets import ToggleButtons, VBox, HBox, HTML, Dropdown
    
    dates = Dropdown(options = sorted(stock_styles_w_style[agg_date_col].unique()), value=stock_styles_w_style[agg_date_col].max())
    styles = ToggleButtons(options = sorted(stock_styles_w_style['Style'].unique()), value='Is_Momentum')

    show_cols = {'Is_Value' : ['forward12M_earn_yld', 'trailing12M_book_yld',
                               'forward12M_earn_yld_zscore', 'trailing12M_book_yld_zscore', 'value_zscore'],
                'Is_growth': ['next2Y_EPS', 'next1Y_EPS', 'thisY_EPS', 'last1Y_EPS', 'last2Y_EPS', 'last3Y_EPS',
                               'ebitda_margin', 'ebitda_margin_2Y','next2Y_avg_EPSg',
                               'last2Y_avg_EPSg', 'last2Y_ebitda_margin_chg','growth_zscore'],
                'Is_Quality': ['ROE', 'ROIC', 'FCF per share', 'quality_zscore'],
                'Is_Yield': ['IS_DIV_PER_SHR','forward_dvd_yield', 'fcf_yield', 'fcf_yield_zscore','div_zscore','yield_zscore'],
                'Is_Momentum': ['3M ret','6M ret','12M ret','3M ret_zscore', '6M ret_zscore', '12M ret_zscore', 'momentum_zscore']}

    def filter_content(*args):
        dff = stock_styles_w_style.query('{} == "{}" & Style == "{}"'.format(agg_date_col, dates.value, styles.value))
        
        id_list = list(dff.ID.unique())
        dff2 = stock_styles_full.query('{} == "{}" & ID == @id_list'.format(agg_date_col, dates.value))[['ID',date_col]
                                                                    +show_cols.get(styles.value)]
        content.children = [HTML('{} no of stocks'.format(len(dff2))), 
                            HTML(dff2.sort_values(dff2.columns[-1], ascending=False).style.render())]

    dates.observe(filter_content)    
    styles.observe(filter_content)

    content = VBox()

    return VBox([dates, styles, content])