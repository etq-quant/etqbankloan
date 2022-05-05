import pandas as pd

id_col = 'ID'
date_col = 'DATE'

def process_signal_df(df_calls, price_w_dvd):
    '''
    Process the signals
    
    Parameters
    ----------
    df_calls: DataFrame
        stock dataframe with buy sell hold signals only
    price_w_dvd: DataFrame
        stock dataframe with stock prices and cash dividends
    
    Returns
    -------
    df_calls: DataFrame
        stock dataframe with cash dividends added to holding period,
        size reduced to only contains stocks with actions/signals
    '''
    df_calls = stock_with_signals(df_calls)
    df_calls = df_calls.drop(['px_last','cash_divs'], 1, errors='ignore')\
                        .merge(price_w_dvd[[id_col,date_col,'px_last','cash_divs']], on=[id_col,date_col],how='left')
    df_calls['px_last'] = df_calls.groupby(id_col)['px_last'].ffill()
    df_calls = price_dvd_adjusted(df_calls)
    
    return df_calls

def stock_with_signals(df_calls):
    '''
    Filter the df_calls to only contains stocks that have signals (reducing size of the df)
    
    Parameters
    ----------
    df_calls: DataFrame
        stock dataframe with buy sell hold signals
    
    Returns
    -------
    df_calls: DataFrame
        stock dataframe with only stocks that has action
    '''
    stock_with_calls = df_calls.groupby(id_col)['action'].nunique().reset_index(name='no_of_actions')
    stock_with_calls = stock_with_calls[stock_with_calls['no_of_actions']>1][id_col].tolist()
    df_calls = df_calls[df_calls[id_col].isin(stock_with_calls)].copy()
    return df_calls

def price_dvd_adjusted(df_calls):
    '''
    Calculate new adjusted price (px_last + cash_divs) when the stock is in buy/hold position
    
    Parameters
    ----------
    df_calls: DataFrame
        stock dataframe with buy sell hold signals and cash_divs column
    
    Returns
    -------
    df_calls: DataFrame
        stock dataframe with px_adjusted column added 
    '''
    df_calls['period'] = (df_calls['action'].map(lambda x: 1 if x == "buy" else 0) + \
                          df_calls['action'].map(lambda x: 1 if x == "sell" else 0).shift().fillna(0)).cumsum()
    fill0 = df_calls['action']!= 'na'
    df_calls.loc[fill0, 'cash_divs'] = df_calls.loc[fill0, 'cash_divs'].fillna(0)
    df_calls['px_adjusted'] = df_calls.groupby('period')['cash_divs'].cumsum() + df_calls['px_last']
    cash_divs_by_period = df_calls.groupby('period').agg({'DATE':'last', 'cash_divs':'sum'}).reset_index()\
                                  .rename(columns={'cash_divs':'tot_cash_divs_by_period'})
    df_calls = df_calls.drop('tot_cash_divs_by_period',1,errors='ignore').merge(cash_divs_by_period, on=['period','DATE'], how='left')
    return df_calls

def calculate_twrr(df_calls_return, ret_col = 'return'):
    '''
    [In]
    df_calls_return: dataframe with buy sell signals and stock returns
    
    [Out]
    |   ID  |  twrr  |
    |-------|--------|
    |stock 1|  1.00  |
    |stock 1|  1.01  |
    |stock 1|  1.20  |
    '''

    df_calls_return['return+1'] = df_calls_return.apply(lambda x: x[ret_col]+1 if x['action'] not in ['buy','na'] else 1 , 1)
    df_calls_return['twrr'] = df_calls_return.groupby('ID')['return+1'].cumprod()
    return df_calls_return
    
def pivot_start_end_by_ID(df, 
                            col='state', 
                            date_col='DATE',
                            start_col = 'start',
                            end_col = 'end'):
    '''
    |   ID  | date | state |      |   ID  | start |  end |
    |-------|------|-------|      |-------|-------|------|  
    |stock 1|  a   | stay  |      |stock 1|   b   |   e  |
    |stock 1|  b   | start | ---> |stock 2|   f   |   m  |
    |stock 1| ..   |  ...  |
    |stock 1|  e   | end   |
    
    '''
    df = df[(df[col]==end_col) | (df[col] == start_col)][[col,'ID',date_col]].copy()
    df['index'] = df.groupby(id_col)[date_col].apply(lambda x: (x.rank()-1)//2)
    df = pd.pivot_table(df, values= date_col,columns=col,index=['index',id_col], aggfunc='first')[[start_col,end_col]].reset_index()
    return df.drop('index',1).sort_values(id_col)

def calculate_total_ret(df_calls, date_col = 'DATE', price_col = 'px_last'):
    '''
    Price return
    
        [Out]
    |   ID  |total_return|
    |-------|------------|
    |stock 1|    2.345   |
    '''
    df_calls_hori = pivot_start_end_by_ID(df_calls, col='action', start_col='buy', end_col='sell')
    df_calls_hori = df_calls_hori.fillna(df_calls[date_col].max())
    
    df_calls_hori = df_calls_hori\
        .merge(df_calls[[id_col,date_col,price_col]].rename(columns={price_col:'sell_price',date_col:'sell'}), on=[id_col,'sell'], how='left')\
        .merge(df_calls[[id_col,date_col,price_col]].rename(columns={price_col:'buy_price',date_col:'buy'}), on=[id_col,'buy'], how='left')
    df_calls_hori['total_return'] = df_calls_hori['sell_price']/df_calls_hori['buy_price'] - 1
    df_calls_tot_return = df_calls_hori.groupby(id_col)['total_return'].sum() + 1
    return df_calls_tot_return.reset_index()