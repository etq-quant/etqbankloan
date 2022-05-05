from datetime import datetime, timedelta
import pandas as pd


def download_price_dvd_data(bq, univ:list, start:str, end:str):
    '''
    
    e.g. suspended stocks: IJM MK Equity 2021-06-09
    
    '''
    params =  dict(dates = bq.func.range(start=start, end=end))
    join_on = ['ID','DATE'] 
    
    def get_price_vol(bq, univ, start, end):
        
        df = get_data(bq, univ, {'px_last': bq.data.px_last(**params),
                                 'px_low':bq.data.px_low(**params),
                                 'px_high':bq.data.px_high(**params),
                                 'px_open':bq.data.px_open(**params),
                                 'mkt_cap':bq.data.cur_mkt_cap(**params),
                                 'volume' : bq.data.turnover(**params)}).reset_index().dropna()
        
        univ_ids = df[['ID']].drop_duplicates()
        full_dates = df[['DATE']].drop_duplicates()
        df = univ_ids.assign(key=1).merge(full_dates.assign(key=1), on='key').drop('key',1)\
                     .merge(df, on=['ID','DATE'], how='left')
        return df

    price_df = download_incremental(bq, univ, get_price_vol, start, end)
    price_df['px_last'] = price_df.groupby('ID')['px_last'].ffill()
    price_df['mkt_cap'] = price_df.groupby('ID')['mkt_cap'].ffill()
    
    for p in ['px_high', 'px_open', 'px_low']:
        price_df[p] = price_df[p].fillna(price_df['px_last'])
        
    price_df['volume'] = price_df['volume'].fillna(0)
    
    def get_dvd(bq, univ, start, end):
        df = get_data(bq, univ, {'cash_divs':bq.func.dropna(bq.data.cash_divs(**params))})
        return df.dropna().reset_index()

    dvd_df = download_incremental(bq, univ, get_dvd, start, end)
    dvd_df = dvd_df.groupby(join_on)['cash_divs'].sum().reset_index()

    price_w_dvd = price_df.merge(dvd_df.drop(['CURRENCY','Partial Errors'],1, errors='ignore'), 
                              on=join_on, how='left')
    return price_w_dvd

def download_return_data(bq, univ:list, start:str, end:str, per='D'):
    
    params =  dict(calc_interval = bq.func.range(start=start, end=end))
    
    def get_ret(bq, univ, start, end):
        df = get_data(bq, univ, {'return': bq.data.return_series(per=per, **params)})
        return df.dropna().reset_index()
    
    prev1month = (datetime.strptime(start, '%Y-%m-%d')-timedelta(31)).date().isoformat()
    return_df = download_incremental(bq, univ, get_ret, prev1month, end) 
    return return_df
    
def download_benchmark_riskfree(bq, bticker:str, rfticker:str, start:str, end:str, per='D'):
    '''
    Parameters
    ----------
    bq: bql.Service object
    bticker: str
        backtest benchmark ticker
    rfticker: str
        riskfree ticker
    start: str
        start date
    end: str
        end date
    per: str
        frequency
        
    Returns
    -------
    benchmark_df: DataFrame
        dataframe of benchmark values
    risk_free: DataFrame
        risk free rate times series
    
    Examples
    --------
    >>> bticker = 'FBM100 Index'
    >>> rfticker = 'MAOPRATE Index'
    >>> start = '2021-01-01'
    >>> end = '2021-03-31'
    >>> per = 'D'
    >>> benchmark_df, risk_free = download_benchmark_riskfree(bq, bticker, rfticker)
    
    '''
    params = dict(dates=bq.func.range(start=start, end=end), per=per)
    benchmark_df = get_data(bq, bticker, {'px_last':bq.data.px_last(**params)}).reset_index()

    risk_free = get_data(bq, rfticker , {'riskfree': bq.data.px_last(**params)})\
                        .rename(columns={'DATE':'date'}).drop('CURRENCY',1).set_index('date')/100
    return benchmark_df, risk_free
    
def cross_join(df1, df2):
    if df1.shape[1] != 1 or  df2.shape[1] != 1:
        raise 'Input dfs must be single column'
        
    cross_join = df1.assign(key=1).merge(df2.assign(key=1), on='key').drop('key',1)
    return cross_join

def get_full_ids_dates(df, id_col='ID', date_col='DATE'):
    ids = df[[id_col]].sort_values(id_col).drop_duplicates()
    dates = df[[date_col]].sort_values(date_col).drop_duplicates()
    ids_dates = cross_join(ids, dates) 
    return ids_dates

def get_full_dates_member(df, univ_df):
    univ_df.rename(columns={'DATE':'DATE_members'}, inplace=True)

    ids = univ_df[['ID']].sort_values('ID').drop_duplicates()
    dates = df[['DATE']].sort_values('DATE').drop_duplicates()
    univ_dates = univ_df[['DATE_members']].sort_values('DATE_members').drop_duplicates()
    
    date_maps = pd.merge_asof(dates, univ_dates, left_on='DATE', right_on='DATE_members')
    member_dates = date_maps.merge(univ_df[['ID','DATE_members']], on='DATE_members', how='left').assign(index='In')
#     return member_dates
    ids_dates = cross_join(ids, dates)
    full_dates_member = ids_dates.merge(member_dates, on=['ID','DATE'], how='left').fillna('Out')
    return full_dates_member

def get_data(bq, security, fields):
    import bql
    request =  bql.Request(security, fields)
    response = bq.execute(request)
    df = bql.combined_df(response)
    return df

def download_incremental(bq, ticker, func, start_date, end_date, steps=1, **kwargs):
    start_year = int(start_date.split('-')[0])
    end_year = int(end_date.split('-')[0])
    
    steps = min(end_year-start_year, steps)
    if start_year != end_year:
        years = list(range(start_year, end_year+1, steps))  
        df_list = []
        for y in range(len(years)-1):
            if years[y+1] != years[-1]:
                end = '{}-12-31'.format(years[y+1]-1)
            else:
                end = end_date

            if years[y] == years[0]:
                start=start_date
            else:
                start = '{}-01-01'.format(years[y])
                
#             print('1.', start, end)
            df = func(bq, ticker, start, end, **kwargs)
            df_list.append(df)

        return pd.concat(df_list)
    else:
#         print('2.', start_date, end_date)
        return func(bq, ticker, start_date, end_date, **kwargs)