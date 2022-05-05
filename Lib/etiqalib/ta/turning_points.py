from matplotlib import pyplot as plt
from findiff import FinDiff
import pandas as pd
import numpy as np
from tqdm import tqdm

id_col = 'ID'
date_col = 'DATE'
px_close = 'px_last'
px_high = 'px_high'
px_low = 'px_low'
px_open = 'px_open'

def find_derivative(series): #1 day interval
    '''
    Compute the first and second derivatives (1-step interval) of a given series.
    
    Parameters
    ----------
    series: np.array
        series of values to find derivatives
        
    Returns
    -------
    mom: np.array
        first derivative
    momacc: np.array
        second derivative
        
    Examples
    --------
    >>> series
        array([6.85, 7.06, 7.31, 8.  , 7.72, 7.27, 6.57, 7.66, 8.27, 8.31])
        
    >>> mom, momacc = find_derivative(series)
    >>> mom
        array([ 0.19 ,  0.23 ,  0.47 ,  0.205, -0.365, -0.575,  0.195,  0.85 , 0.325, -0.245])
    >>> momacc
        array([-0.36,  0.04,  0.44, -0.97, -0.17, -0.25,  1.79, -0.48, -0.57, -0.66])
    '''
    
    d_dx = FinDiff(0, 1, 1)
    d2_dx2 = FinDiff(0, 1, 2)
    clarr = np.asarray(series)
    mom = d_dx(clarr)
    momacc = d2_dx2(clarr)
    return mom, momacc

def find_local_min_max(series, mom, momacc):
    '''
    Find local minimum and maximum points from a series
    
    Parameters
    ----------    
    series: np.array
        series of values to find derivatives
    mom: np.array
        first derivative
    momacc: np.array
        sescond derivative
        
    Returns
    -------
    local_mins: dict
        dictionary of index and value of local minimum of the series
    local_max: dict
        dictionary of index and value of local maximum of the series
        
    Examples
    --------
    >>> series
        array([6.85, 7.06, 7.31, 8.  , 7.72, 7.27, 6.57, 7.66, 8.27, 8.31])
        
    >>> local_mins, local_maxs = find_local_min_max(series, mom, momacc)
    >>> local_mins
        {6: 6.57}
        
    >>> local_maxs
        {3: 8.0, 9: 8.31}
    '''
    local_mins = []
    local_maxs = []

    for i in range(len(mom)-1):
        series_dict = {i: series[i], i+1: series[i+1]}
        if mom[i] <0 and mom[i+1]> 0:
            local_mins.append(min(series_dict, key=series_dict.get))
        elif mom[i] > 0  and mom[i+1]<0:
            local_maxs.append(max(series_dict, key=series_dict.get))
        elif mom[i] == 0 and momacc[i] >0:
            local_mins.append(i)
        elif mom[i] == 0 and momacc[i] <0:
            local_maxs.append(i)
            
    local_mins = {i : series[i] for i in local_mins}        
    local_maxs = {j : series[j] for j in local_maxs}    
    return  local_mins, local_maxs

def get_state_local_min_max(dff, col = 'px_high', ma1 = 5, ma2 = 22):
    '''
    Main function to get trendline. NOTE: shifted one day late to avoid look-ahead bias
    
    Step 1:
        Label period as up and down based on the spread between short ma and long ma
        i) short ma > long ma: up trend
        ii) long ma > short ma: down trend
        Label state when there is a change in state up - down / down - up
            state 1, 2, 3, ...
        Aggregate max or min of state.
    
    Step 2:
        Find local min and max points of the col input 
        
    Step 3:
        Filter rows where local_max == max_in_state or local_min == min_in_state
        Transform the rows into wide form, calculate the m, c that connects the two points
        
    Parameters
    ----------
    dff: DataFrame
        stock df with DATE and ohlc prices, re-index to start from 0 is necessary
    col: str
        price high or price low. px_high to get resistance line (down trend), px_low to get support line (up trend)
    ma1: int
        short moving average period (in days)
    ma2: int
        long moving average period (in days)
    
    Returns
    -------
    dff2: DataFrame
        dataframe with ma_1st, ma_2nd, state and local_min/max 
    line_df: DataFrame
        dataframe of the y equation, start and end period date of the support/resist line
    
    '''
#     dff['ma_1st'] = dff[col].rolling(ma1).mean()
#     dff['ma_2nd'] = dff[col].rolling(ma2).mean()
    
    dff['ma_1st'] = dff[col].ewm(span=ma1, min_periods = ma1, adjust=False).mean()
    dff['ma_2nd'] = dff[col].ewm(span=ma2, min_periods = ma2, adjust=False).mean()
    
    dff['spread'] = (dff['ma_1st'] - dff['ma_2nd']).shift()
    dff.dropna(subset=['spread'], inplace=True)
    dff.reset_index(drop=True, inplace=True)
    dff['sign'] = dff['spread'].map(lambda x: 'up' if x>0 else 'down')
    dff['state'] = (dff['sign']!=dff['sign'].shift()).astype(int).cumsum()
    
    mom, momacc = find_derivative(dff[col].values)
    local_mins, local_maxs = find_local_min_max(dff[col].values, mom, momacc)
    
    return dff, local_mins, local_maxs

def refine_end_filter(end_filter_df, local_):
    end_of_state=end_filter_df.groupby('state')[date_col].rank(ascending=False) ==1
    end_filter_df.loc[end_of_state, local_] = None
    end_filter_df[local_] = end_filter_df.groupby('state')[local_].ffill()
    return end_filter_df.dropna()
    
def get_line(df, local_='local_maxs', start_='up', agg = 'max', m_increase = 1):
    '''
    local_ = 'local_maxs'
    start_ = 'up'
    agg = 'max'
    m_increase = 1
    '''
    start_rule = df['sign'] == start_

    start_filter = df[start_rule].copy()

    start_filter = start_filter[start_filter[local_] == start_filter.groupby('state')[local_].transform(agg)]\
                    .reset_index()[[id_col,'index', date_col,'state',local_]]
    start_filter = start_filter.assign(state=start_filter.state+1)
    next_start_filter = start_filter.assign(next_start_dt=start_filter[date_col].shift(-1)).fillna(df[date_col].max())
    
    cols = list(start_filter.columns)
    start_filter.columns = ['start_'+i  if i not in [id_col,'state'] else i for i in start_filter.columns]

    end_rule = df['sign'] != start_

    end_filter = df[end_rule].dropna(subset=[local_]).reset_index()
#     end_filter = refine_end_filter(end_filter, local_)
    start_end_filter = start_filter.merge(end_filter[cols], on=[id_col,'state'], how='left').dropna()\
                                    .merge(next_start_filter[[id_col, 'state','next_start_dt']], on=[id_col, 'state'], how='left') #######
    
    start_end_filter['m'] = (start_end_filter[local_] - start_end_filter['start_' + local_]) / \
                            (start_end_filter['index'] - start_end_filter['start_index'])
    start_end_filter['c'] = start_end_filter[local_] - start_end_filter['m']*start_end_filter['index']

    gradient_sign = (m_increase*start_end_filter['m'] < m_increase*start_end_filter.groupby('state')['m'].shift()).map(lambda x: 1 if not x else None)

    start_end_filter['m'] = (start_end_filter['m'] * gradient_sign).ffill()
    start_end_filter['c'] = (start_end_filter['c'] * gradient_sign).ffill()
    start_end_filter['line_group'] = gradient_sign.cumsum().ffill()
    start_end_filter = start_end_filter[m_increase*start_end_filter['m']<0].drop_duplicates(subset=[date_col], keep='last')
    
    dff2 = df.merge(start_end_filter.drop('index',1), 
                    on=[id_col,date_col,'state', local_], how='left').ffill()
    fillins = (dff2[date_col]>dff2['next_start_dt']).map(lambda x: None if x  else 1)
    dff2['y'] = (dff2['m']*dff2.index + dff2['c'])*fillins
    
    dff2['y2'] = dff2['m']*dff2.index + dff2['c']
    dff2['cross'] = m_increase*dff2[px_close] > m_increase*dff2['y']

    first_cross = dff2[dff2['cross']==True].reset_index().groupby('line_group')[date_col].first().reset_index().assign(first_cross=1)

    dff2 = dff2.merge(first_cross, on=['line_group',date_col], how='left').drop('cross',1)
    dff2['first_cross'] = dff2['first_cross'].fillna(0)
    start_end_filter = start_end_filter.merge(first_cross.rename(columns={date_col:'cross_'+date_col}), on='line_group', how='left')

    return dff2, start_end_filter

def _trendline_doc_string(original):
    def wrapper(target):
        target.__doc__ = original.__doc__
        return target
    return wrapper

@_trendline_doc_string(get_state_local_min_max)
def get_down_trendline(dff, col = 'px_high', ma1 = 5, ma2 = 22):
    dff = dff.reset_index(drop=True)
    dff, _, local_maxs = get_state_local_min_max(dff, col, ma1, ma2)
    dff['local_maxs'] = dff.index.map(local_maxs)
    dff2, line_df = get_line(dff, local_='local_maxs', start_='up', agg = 'max', m_increase = 1)
    return dff2, line_df

@_trendline_doc_string(get_state_local_min_max)
def get_up_trendline(dff, col='px_low', ma1=5, ma2=22):
    dff = dff.reset_index(drop=True)
    dff, local_mins, _ = get_state_local_min_max(dff, col, ma1, ma2)
    dff['local_mins'] = dff.index.map(local_mins)
    dff2, line_df = get_line(dff, local_='local_mins', start_='down', agg = 'min', m_increase = -1)
    return dff2, line_df
    
def cal_ret(price_df, col='px_last', ret_days=None, shift_days=0):
    '''
    Calculate the future return, i.e. forward return from today. 
    Will return NaN if the days in future not present yet
    
    Parameters
    ----------
    price_df: DataFrame
        dataframe with stock prices
    
    Returns
    -------
    price_df: DataFrame
        dataframe with forward returns calculated
    '''
    
    if ret_days == None:
        ret_days = [10, 30]
    for d in ret_days:
        price_df['%dD_return'%d] = price_df[col].pct_change(d).shift(-1*(d+shift_days))*100
    return price_df #[['DATE',col]+]
        
def add_features(df):
    '''
    Add feature to df (on the cross date)
    
    Parameters
    ----------
    df: DataFrame
        df with required fields to generate features
        
    Returns
    -------
    df: DataFrame
        df with added features
    '''
    
#     cols = df.columns.tolist()
    df['price_change_5D'] = df['px_last'].pct_change(5)*100
    df['price_change_f0'] = df['px_last'].pct_change()*100
    df['price_change_f1'] = df['px_last'].pct_change().shift(-1)*100
    
    df['open-close_f0'] = (df['px_last']/df['px_open']-1)*100
    df['open-close_f1'] = (df['px_last']/df['px_open']-1).shift(-1)*100
    
    df['accel'] = df['px_high'].diff().diff()
    df['avat'] = df['volume']/df['volume'].rolling(20).mean()
    
#     feature_cols = list(set(df.columns).difference(set(cols)))
    return df

def full_ma_line_run(df, col='px_high', ma1=5, ma2=22):
    '''
    Generate full trendline and crosses
    
    get_down_trendline
    
    Parameters
    ----------
    df: DataFrame
        full stock df with prices
    col: str
        px_high for downtrend, px_low for uptrend
    ma1: int
        short moving average (days)
    ma2: int
        long moving average (days)
  
    Returns
    -------
    trend_line_df: DataFrame
        line_df generated from trendline_func
    stock_ma_line_df: DataFrame
        full_stock_df with merged line_df and its repective crosses after the last_DATE
        
    Examples
    --------
    >>> stock_ma_line_df, trend_line_df = full_ma_line_run(df, 'px_high', ma1=5, ma2=22, feature_func=add_features)
    
    '''
    if 'high' in col:
        trendline_func = get_down_trendline
    else:
        trendline_func = get_up_trendline
        
    stock_ma_line_df = pd.DataFrame()
    trend_line_df = pd.DataFrame()
    
    for stock in tqdm((sorted(df[id_col].unique()))):
        dff = df[df[id_col]==stock].sort_values(date_col).copy()
        try:
            dff2, line_df = trendline_func(dff)
            stock_ma_line_df = stock_ma_line_df.append(dff2)
            trend_line_df = trend_line_df.append(line_df)
        except Exception as e:
            print(stock, e)
        
    return stock_ma_line_df.reset_index(drop=True), trend_line_df
################################################ Channel Breakout ########################################################

from sklearn.linear_model import LinearRegression

def channel_lr(stock_df, start_date, end_date):
    train_df = stock_df[(stock_df[date_col]>=start_date)&(stock_df[date_col]<=end_date)].copy()

    y = train_df[px_close]
    X = train_df.index.values

    lr = LinearRegression()
    lr.fit(X.reshape(-1,1), y)
    a = lr.coef_[0]
    b = lr.intercept_

    y_pred = a*X + b 
    BU = max(train_df[px_high] - y_pred)
    BL = min(train_df[px_low] - y_pred)
    
    return dict(a=a, b=b, BU=BU, BL=BL)

def channel_project(stock_df, line_df, m_increase):
    stock_df = stock_df.reset_index(drop=True)
    line_df = line_df.drop_duplicates(subset=['line_group'])
    
    channel_lr_df = []
    for lrow in line_df.to_dict(orient='records'):
        channel_lr_params = channel_lr(stock_df, lrow['start_' + date_col], lrow[date_col])
        channel_lr_df.append({**lrow, **channel_lr_params})

    channel_lr_df = pd.DataFrame(channel_lr_df)

    stock_df = stock_df.merge(channel_lr_df[[id_col,date_col, 'a','b','BU','BL']], how='left').ffill()
    
    fillins = (stock_df[date_col]>stock_df['next_start_dt']).map(lambda x: None if x  else 1)
    
    stock_df['project'] = (stock_df['a']*stock_df.index + stock_df['b'] + stock_df['a'] + m_increase*stock_df['BU'])*fillins

    stock_df['cross'] = m_increase*stock_df[px_close] > m_increase*stock_df['project']
    first_cross = stock_df[stock_df['cross']==True].reset_index().groupby('line_group')[date_col]\
                            .first().reset_index().assign(first_channel_cross=1)
    stock_df = stock_df.merge(first_cross, on=['line_group',date_col], how='left').drop('cross',1)
    stock_df['first_cross'] = stock_df['first_cross'].fillna(0)
    channel_lr_df = channel_lr_df.merge(first_cross.rename(columns={date_col:'channel_cross_'+date_col}), on='line_group', how='left')

    return stock_df, channel_lr_df

def full_channel_run(stock_ma_line_df, trend_line_df, col='px_high'):
    m_increase = 1 if 'high' in col else -1

    stock_channel_df = pd.DataFrame()
    full_channel_df = pd.DataFrame()

    for stock in tqdm((sorted(stock_ma_line_df[id_col].unique()))):
        stock_df = stock_ma_line_df[stock_ma_line_df[id_col]==stock]
        line_df = trend_line_df[trend_line_df[id_col]==stock]
        try:
            dff2, channel_df = channel_project(stock_df, line_df, m_increase)

            stock_channel_df = stock_channel_df.append(dff2)
            full_channel_df = full_channel_df.append(channel_df)
        except Exception as e:
            print(stock, e)

    cross_dates = ['cross_%s'%date_col,'channel_cross_%s'%date_col]
    full_channel_df['later_cross_date'] = full_channel_df[cross_dates].max(axis=1)
    full_channel_df['both'] = full_channel_df[cross_dates].isnull().sum(axis=1).map(lambda x: 1 if x==0 else 0)

    return stock_channel_df, full_channel_df
################################################ Visualization ########################################################
import plotly.graph_objects as go
from ipywidgets import interact, interactive, Dropdown, HTML, VBox, HBox

def plt_trendline(df, line_df, stock, col='px_high'):
    '''
    Plot price with trendline
    
    Parameters
    ----------
    df: DataFrame
        dataframe with dates and stock prices
    line_df: DataFrame
        dataframe which contains start end index and date of trendline
    stock: str
        stock name for plot title
    col: str
        px_high or px_low 
    '''
    if 'high' in col:
        local_ = 'local_maxs'
    else:
        local_ = 'local_mins'
        
    plt.rcParams['figure.figsize'] = (20,8)
    fig, ax = plt.subplots()
    
    df = df.set_index(date_col)
    df[col].plot(color='black')
    df[['ma_1st','ma_2nd']].plot(alpha=0.5, ax=ax) if 'ma_1st' in df.columns else None
    
    plt.scatter(df.query('first_cross==1').index, df.query('first_cross==1')['y'], marker='x', color='red', s=100)

    for line_g in df['line_group'].dropna().unique():
        df_plot = df[df['line_group']==line_g].dropna(subset=['y']).iloc[[0, -1]].copy()
        df_plot['y'].plot(color='red', linewidth=1)

    for row in line_df.to_dict(orient='records'):
        plt.plot([row['start_' + date_col], row[date_col]], 
                 [row['start_' + local_] , row['m']*row['index'] + row['c']], color='purple', linewidth=1)
        
    plt.title(stock)
    return plt
    
def interactive_plt_trendline(df, ma1=5, ma2=22, direction='down'):
    
    if direction == 'down':
        trendline_func = get_down_trendline
        col = 'px_high'
    else:
        trendline_func = get_up_trendline
        col = 'px_low'
        
    stock_selec = Dropdown(options = sorted(df.ID.unique()))
    
    @interact()
    def plot(stock = stock_selec):
        dff = df[df[id_col]==stock].reset_index(drop=True).copy()
        dff2, line_df = trendline_func(dff, ma1=ma1, ma2=ma2)
        plt_trendline(dff2, line_df, stock, col)
    
def plt_channel(channel_df, channel_line_df, stock):
    
    fig, ax = plt.subplots()
    channel_df = channel_df.set_index(date_col)
    channel_df[px_close].plot(color='black')
    channel_df[['ma_1st','ma_2nd']].plot(alpha=0.5, ax=ax) if 'ma_1st' in channel_df.columns else None

    for crow in channel_line_df.to_dict(orient='records'):
        line_g = channel_df[channel_df['line_group']==crow['line_group']]
        dff2_plot = line_g.dropna(subset=['project']).iloc[[0,-1]].copy()
        dff2_plot['project'].plot(color='red', linewidth=1)
        
        cross = line_g.query('first_channel_cross==1')
        if cross.shape[0] :
            plt.scatter(cross.index, cross[px_close], marker='x', color='red', s=100)
            
        date_X = [crow['start_'+date_col], crow[date_col]]
        X = np.array([crow['start_index'], crow['index']])
        plt.plot(date_X, crow['a']*X+crow['b'], color='brown')
        plt.plot(date_X, crow['a']*X+crow['b']+crow['BU'], color='cyan')
        plt.plot(date_X, crow['a']*X+crow['b']+crow['BL'], color='cyan')
        
    plt.title(stock)
    return plt

def interactive_plt_channel(df, ma1=5, ma2=22, direction='down'):
    
    if direction == 'down':
        trendline_func = get_down_trendline
        col = px_high
        m_increase = 1
    else:
        trendline_func = get_up_trendline
        col = px_low
        m_increase = -1
        
    stock_selec = Dropdown(options = sorted(df.ID.unique()))
    
    @interact()
    def plot(stock = stock_selec):
        dff = df[df[id_col]==stock].reset_index(drop=True).copy()
        dff2, line_df = trendline_func(dff, ma1=ma1, ma2=ma2)
        dff3, channel_df = channel_project(dff2, line_df, m_increase)
        plt_channel(dff3, channel_df, stock)
        
def interactive_plt_channel2(stock_channel_df, channel_line_df):
    
    def _plot_cross(cross):
        stock = stock_selec.value
        
        stock_df = stock_channel_df[stock_channel_df[id_col]==stock].reset_index(drop=True).copy()
        channel_df = channel_line_df[channel_line_df[id_col]==stock]
        
        if cross == 'All':
            plt_channel(stock_df, channel_df, stock)
        else:
            plt_channel(stock_df, channel_df.iloc[cross:cross+1], stock)
        
    def update_cross_selec(stock):
        cross_selec.options = ['All'] + list(range(channel_line_df[channel_line_df[id_col]==stock].shape[0]))
    
    stock_selec = Dropdown(options = sorted(stock_channel_df[id_col].unique()))
    init = channel_line_df[channel_line_df['ID']==stock_selec.value].shape[0]
    cross_selec = Dropdown(options = range(init))
    
    j = interactive(update_cross_selec, stock=stock_selec)
    i = interactive(_plot_cross, cross=cross_selec)
    k = VBox()
    display(j)
    display(i)
    display(k)
    
import plotly.graph_objects as go

def plotly_trendline(df, line_df, stock, fig=None):
    
    if not fig:
        fig = go.Figure()
    
    fig.add_trace(go.Candlestick(x=df[date_col], 
                                 open=df[px_open], 
                                 high=df[px_high], 
                                 low=df[px_low], 
                                 close=df[px_close], showlegend=False))
    
    local_ = [i for i in line_df.columns if 'start_' in i and date_col not in i and 'index' not in i][0]
    
    for row in line_df.to_dict(orient='records'):
        line_g = df[df['line_group']==row['line_group']]
        df_plot = line_g.dropna(subset=['y']).iloc[[0, -1]].copy()
        fig.add_trace(go.Scatter(x=df_plot[date_col], y=df_plot['y'], mode='lines', showlegend=False,
                                 hoverinfo='skip', line = dict(color = 'purple', width=1)))
        
        cross = line_g.query('first_cross==1')
        if cross.shape[0] :
            fig.add_trace(go.Scatter(x=cross[date_col], y=cross[px_close], showlegend=False, 
                                     mode='markers', marker_symbol='x', marker_color='black'))
            
        fig.add_trace(go.Scatter(x=[row['start_' + date_col], row[date_col]], 
                                 y=[row[local_] , row['m']*row['index'] + row['c']], 
                                 mode='lines', line_color='black', showlegend=False))
        
    fig.update_layout(title=stock, template='ygridoff', xaxis_rangeslider_visible=False)
    return fig
        
def plotly_channel(channel_df, channel_line_df, stock, fig=None):
    
    if not fig:
        fig = go.Figure()
    
#     fig.add_trace(go.Scatter(x=channel_df[date_col], y=channel_df[px_close], line_color='black', showlegend=False))
    fig.add_trace(go.Candlestick(x=channel_df[date_col], 
                                 open=channel_df[px_open], 
                                 high=channel_df[px_high], 
                                 low=channel_df[px_low], 
                                  close=channel_df[px_close], showlegend=False))
    
    for line_g in channel_line_df['line_group'].dropna().unique():
        dff2_plot = channel_df[channel_df['line_group']==line_g].iloc[[0,-1]].copy()
        
    
    for crow in channel_line_df.to_dict(orient='records'):
        date_X = [crow['start_'+date_col], crow[date_col]]
        
        line_g = channel_df[channel_df['line_group']==crow['line_group']]
        dff2_plot = line_g.dropna(subset=['project']).iloc[[0,-1]].copy()
        fig.add_vline(dff2_plot[date_col].iloc[0] ,line_dash="dot", line=dict(color='black'))
        fig.add_trace(go.Scatter(x=dff2_plot[date_col], y=dff2_plot['project'], mode='lines', showlegend=False,
                                 hoverinfo='skip', line = dict(color = 'black', width=1)))
        
        cross = line_g.query('first_channel_cross==1')
        if cross.shape[0] :
            fig.add_trace(go.Scatter(x=cross[date_col], y=cross[px_close], showlegend=False, 
                                     mode='markers', marker_symbol='x', marker_color='black'))
            
        X = np.array([crow['start_index'], crow['index']])
        fig.add_trace(go.Scatter(x=date_X, y=crow['a']*X+crow['b'], mode='lines', line_color='black', showlegend=False))
        fig.add_trace(go.Scatter(x=date_X, y=crow['a']*X+crow['b']+crow['BU'], mode='lines',
                                 hoverinfo='skip', showlegend=False,line = dict(color = 'blue', width=1)))
        fig.add_trace(go.Scatter(x=date_X, y=crow['a']*X+crow['b']+crow['BL'], mode='lines',
                                 hoverinfo='skip', showlegend=False,line = dict(color = 'blue', width=1)))

    fig.update_layout(title=stock, template='ygridoff', xaxis_rangeslider_visible=False)
    return fig


def interactive_plt_channel3(stock_channel_df, channel_line_df):
    
    def _plot_cross(cross):
        stock = stock_selec.value
        
        stock_df = stock_channel_df[stock_channel_df[id_col]==stock].reset_index(drop=True).copy()
        channel_df = channel_line_df[channel_line_df[id_col]==stock]
        
        if cross == 'All':
            fig = plotly_channel(stock_df, channel_df, stock)
            fig2 = plotly_trendline(stock_df, channel_df, stock)
        else:
            fig = plotly_channel(stock_df, channel_df.iloc[cross:cross+1], stock)
            fig2 = plotly_trendline(stock_df, channel_df.iloc[cross:cross+1], stock)
            
        k.children= [go.FigureWidget(fig2), go.FigureWidget(fig)]
        
        
    def update_cross_selec(stock):
        cross_selec.options = ['All'] + list(range(channel_line_df[channel_line_df[id_col]==stock].shape[0]))
        _plot_cross('All')
    
    stock_selec = Dropdown(options = sorted(stock_channel_df[id_col].unique()))
    init = channel_line_df[channel_line_df['ID']==stock_selec.value].shape[0]
    cross_selec = Dropdown(options = range(init))
    
    j = interactive(update_cross_selec, stock=stock_selec)
    i = interactive(_plot_cross, cross=cross_selec)
    k = VBox()
    display(j)
    display(i)
    display(k)
