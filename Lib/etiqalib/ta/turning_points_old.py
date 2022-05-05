from matplotlib import pyplot as plt
from findiff import FinDiff
import pandas as pd
import numpy as np

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

def get_up_filter_1(dff):
    dff['min_in_state'] = dff.groupby('state')['local_mins'].transform(min)
    return dff

def get_down_filter_1(dff, threshold = 0.05):
    '''
    Refine the max of state.
    
    if two local max are very near and did not exceed the pct difference threshold, use the later max as the max in state. 
    
    Parameters
    ----------
    dff: DataFrame
        Dataframe with DATE, state and local_maxs
    threshold: float (decimal)
        proportion threshold for % difference in the local max
        
    Returns
    -------
    dff: DataFrame
        Dataframe with local max refined, new column named local_maxs_f1
    '''
    
    def get_action(df, grp_col, col):
        def create_window(a, b):
            if a==1 and b==2:
                return 'start'
            elif a==-1 and b==-2:
                return 'end'
            else:
                return 'na'

        df['%s_diff'%col] = df.groupby(grp_col)[col].diff(periods=1).fillna(2)
        df['action'] = list(zip(df[col], df['%s_diff'%col]))
        df['group'] = df['action'].map(lambda x: create_window(*x))
        df = df.drop(['%s_diff'%col, 'action'], axis=1)
        return df
    
    down_rule = dff['sign'] == 'down'
    down_filter_1 = dff[down_rule].dropna(subset=['local_maxs'])
    
    interval = down_filter_1.reset_index().groupby('state')['index'].diff().values
    local_maxs_diff = down_filter_1.groupby('state')['local_maxs'].diff().map(lambda x: abs(x))
    down_filter_1['pct_change'] =  (local_maxs_diff / interval).fillna(0)
    down_filter_1['diff'] = down_filter_1['pct_change'].map(lambda x: 1 if x<threshold else -1)
    down_filter_1 = get_action(down_filter_1, 'state','diff')
    down_filter_1['group'] = down_filter_1['group'].map(lambda x: 1 if x == 'start' else 0).cumsum()
    down_filter_1 = down_filter_1.merge(down_filter_1.groupby(['state','group'])['local_maxs'].last().reset_index())
    down_filter_1 = down_filter_1.rename(columns={'local_maxs':'local_maxs_f1'}).drop(['diff','group','pct_change'],1)
    dff = dff.merge(down_filter_1, how='left')
    
    up_rule = dff['sign'] == 'up'
    dff.loc[up_rule,'local_maxs_f1'] = dff.loc[up_rule,'local_maxs_f1'].fillna(dff.loc[up_rule,'local_maxs'])
    dff['max_in_state'] = dff.groupby('state')['local_maxs_f1'].transform(max)

    return dff

def get_line(df, col='local_maxs_f1', filter_col='max_in_state', date_col = 'DATE'):
    '''
    
    Note: input from get_down_filter_1
    
    Parameters
    ----------
    df: DataFrame      
        DataFrame with group, state, max_in_state and local_maxs (f1)
    
    Returns
    -------
    df: DataFrame
        DataFrame with:
        - first and last DATE
        - first and last value
        - m (gradient) 
        - c(intercept) connecting two points (high-low or low-high)
        
    Examples
    --------
    >>> df
    
    	DATE	    max_in_state	local_maxs_f1
        ...
    44	2021-04-08	    5.51	       NaN
    45	2021-04-09	    5.51	       5.51
    46	2021-04-12	    6.06	       NaN
        ...
    
    >>> filter_df
    
        DATE	    local_maxs_f1
    45	2021-04-09	    5.51
    57	2021-04-27	    6.06
    77	2021-05-31	    5.20
    
    >>> get_line(df)
    
        first_DATE	last_DATE	first_loc..	last_loc..	first_index	last_index	    m	      c
    0	2021-04-09	2021-04-27	    5.51	    6.06	    45	        57	    0.045833	3.4475
    3	2021-04-27	2021-05-31	    6.06	    5.20	    57	        77	    -0.043000	8.5110
    
    '''
    agg = {i:['first','last'] for i in [date_col,col, 'index']}
    filter_df = df[(df[filter_col] == df[col])].drop_duplicates(subset=['state'], keep='last').copy()
    filter_df = filter_df.reset_index()
    df_even = filter_df.assign(group=filter_df.index//2).groupby('group').agg(agg).reset_index()
    df_even.columns = [j+'_'+i if j else i for i, j in df_even.columns]
    df_odd = filter_df.assign(group=(filter_df.index+1)//2).groupby('group').agg(agg).reset_index()
    df_odd.columns = [j+'_'+i if j else i for i, j in df_odd.columns]
    df = df_even.append(df_odd).drop('group',1).reset_index(drop=True)
    df = df[df['first_'+date_col]!=df['last_'+date_col]].copy()
    df['m'] = (df['last_'+col] - df['first_'+col])/(df['last_index'] - df['first_index'])
    df['c'] = df['first_'+col] - df['m']*df['first_index']
    return df.sort_values('first_'+date_col)

def refine_line_df(df, col='local_maxs_f1'):
    '''
    Combine line df where days overlaps
    
    Parameters
    ----------
    df: DataFrame
        line_df
    col: str
        name of column : local_maxs or local_mins
    
    Returns
    -------
    df: DataFrame
        grouped line_df
    
    Examples
    --------
    >>> line_df
    	first_DATE	last_DATE	first_loc... last_loc... first_index last_index
    	2020-10-19	2020-10-28	    9.72	    9.00	     36	        43	
    	2020-10-28	2020-12-17	    9.00	    6.84	     43	        78	
    	2020-12-17	2021-05-19	    6.84	    5.50	     78	        179	
    
    >>> refine_line_df(line_df)
    
    	first_DATE	last_DATE	first_lo...	last_loc...	first_index	last_index	    m	    c
    0	2020-10-19	2021-05-19	    9.72	    5.5	        36	        179  	-0.02951   10.782378
    '''
    
    df['day_diff'] = (df['first_DATE'] - df['last_DATE'].shift()).map(lambda x: x.days).fillna(0)
    df['group'] = df['day_diff'].map(lambda x: 1 if x>0 else 0).cumsum()

    agg = {i:'first' if 'first' in i else 'last' for i in [c for c in df.columns if 'first' in c or 'last' in c] }

    df = df.groupby('group').agg(agg).reset_index()
    df['m'] = (df['last_'+col] - df['first_'+col])/(df['last_index'] - df['first_index'])
    df['c'] = df['first_'+col] - df['m']*df['first_index']
    return df.sort_values('first_DATE').drop('group',1)

def get_state_local_min_max(dff, col = 'px_high', ma1 = 5, ma2 = 22):
    '''
    Main function to get trendline. 
    
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
    dff['ma_1st'] = dff[col].rolling(ma1).mean()
    dff['ma_2nd'] = dff[col].rolling(ma2).mean()
    dff['spread'] = dff['ma_1st'] - dff['ma_2nd']
    dff.dropna(subset=['spread'], inplace=True)
    dff.reset_index(drop=True, inplace=True)
    dff['sign'] = dff['spread'].map(lambda x: 'up' if x>0 else 'down')
    dff['state'] = (dff['sign']!=dff['sign'].shift()).astype(int).cumsum()
    
    mom, momacc = find_derivative(dff[col].values)
    local_mins, local_maxs = find_local_min_max(dff[col].values, mom, momacc)
    
    return dff, local_mins, local_maxs

def _trendline_doc_string(original):
    def wrapper(target):
        target.__doc__ = original.__doc__
        return target
    return wrapper

@_trendline_doc_string(get_state_local_min_max)
def get_down_trendline(dff, col = 'px_high', ma1 = 5, ma2 = 22):
    dff, _, local_maxs = get_state_local_min_max(dff, col, ma1, ma2)
    dff['local_maxs'] = dff.index.map(local_maxs)
    dff2 = get_down_filter_1(dff)
    line_df = get_line(dff2, col='local_maxs_f1', filter_col='max_in_state').query('m <= 0')
    line_df = refine_line_df(line_df, col='local_maxs_f1')
    return dff2, line_df

@_trendline_doc_string(get_state_local_min_max)
def get_up_trendline(dff, col='px_low', ma1=5, ma2=22):
    dff, local_mins, _ = get_state_local_min_max(dff, col, ma1, ma2)
    dff['local_mins'] = dff.index.map(local_mins)
    dff2 = get_up_filter_1(dff)
    line_df = get_line(dff2, col='local_mins', filter_col='min_in_state').query('m >= 0')
    line_df = refine_line_df(line_df, col='local_mins')
    return dff2, line_df
    
def cal_ret(price_df, col='px_last', ret_days=None):
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
        price_df['%dD_return'%d] = price_df[col].pct_change(d).shift(-1*d)*100
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
    
    df['fprice_change_2D'] = df['px_last'].pct_change(2).shift(-2)*100
    df['open-close_lag1'] = (df['px_last']/df['px_open']-1).shift(-1)*100
    df['open-close_lag2'] = (df['px_last']/df['px_open']-1).shift(-2)*100
    df['slope_2D'] = (df['px_high'].shift(-2) - df['px_high'].shift(2))/4
    df['accel'] = df['px_high'].diff().diff()
    df['avat'] = df['volume']/df['volume'].rolling(20).mean()
    return df, ['fprice_change_2D','slope_2D', 'accel','avat', 'open-close_lag1', 'open-close_lag2']

def get_trendline_crosses(price_df, one_direction_line, col='px_high', feature_func=None):
    '''
    Get dates where the trendline crosses the prices graph after the last_DATE.
    
    Parameters
    ----------
    price_df: DataFrame
        dataframe with stock prices and it's forward return
    one_direction_line: DataFrame
        line_df, either uptrend line or downtrend line
    col: str
        px_high for downtrend and px_low for uptrend
    feature_func: function
        a function passed in to generate features. Default no additional features will be generated
    
    Returns
    -------
    crosses_df: DataFrame
        dataframe with line_df and the dates of crosses of the line
        
    Examples
    --------
    >>> one_direction_line
        first_DATE	last_DATE	first_loc... last_loc... first_index last_index	      m	       c
    0	2020-07-09	2020-10-22	    3.35	    3.01	       6	    77	    -0.004789	3.378732
    1	2020-12-14	2021-01-22	    3.79	    3.34	       113	    140	    -0.016667	5.673333
    2	2021-02-15	2021-05-27	    3.44	    2.96	       153	    220	    -0.007164	4.536119
    
    >>> crosses_df = get_trendline_crosses(dff, line_df)
    >>> crosses_df
        first_DATE	last_DATE	first_loc.	last_loc. first_index last_index	m	 c	    cross_DATE	cross	10D_ret	30D_ret	group
    0	2020-07-09	2020-10-22	    3.35	    3.01	    6	    77	    -0.00	3.38	2020-11-06	  1	    10.00	19.00	0
    1	2020-12-14	2021-01-22	    3.79	    3.34	    113	    140	    -0.02	5.67	2021-02-03	  1	    -1.23	-7.41	1
    2	2020-12-14	2021-01-22	    3.79	    3.34	    113	    140	    -0.02	5.67	2021-03-04	  1	     4.05	3.38	1
    3	2021-02-15	2021-05-27	    3.44	    2.96	    153	    220	    -0.01	4.54	2021-06-02	  1      NaN	NaN	    2
    '''
    
    scores = []
    sign = 1 if 'high' in col else -1
    
    crosses_df = pd.DataFrame()
    
    idx = len(price_df)
    
    ret_cols = [i for i in price_df.columns if '_return' in i]
    
    for i, d in enumerate(one_direction_line.to_dict(orient='records')):
        X = np.arange(d['first_index'], idx) #d['last_index']+addition_days+1
        y = d['m']*X + d['c']

        part_df = price_df.iloc[d['first_index']: ].copy() #d['last_index']+addition_days+1
        part_df['line'] = y
        part_df['spread'] = part_df[col] - part_df['line']

        part_df['sign'] =  part_df['spread'].map(lambda x: -1*sign if x<0 else sign)
        part_df['cross'] = (part_df['sign'] - part_df['sign'].shift().fillna(0)).map(lambda x: 1 if x==2 else 0)
            
        if feature_func:
            part_df, feature_cols = feature_func(part_df)  
        else:
            feature_cols = []
        
        crosses = part_df[(part_df['DATE'] > d['last_DATE']) & (part_df['cross']==1)].rename(columns={'DATE':'cross_DATE'})
        
        crosses_df = crosses_df.append(crosses[['cross_DATE','cross'] + feature_cols + ret_cols]\
                                       .assign(last_DATE=d['last_DATE'])\
                                       .assign(group=i))
#             plt.plot(X, y)
#             plt.plot(X, part_df['px_high'])
#             plt.scatter(crosses.index - price_df.index[0], crosses['px_high'])
    crosses_df = one_direction_line.merge(crosses_df, on='last_DATE')

    return crosses_df

def full_cross_run(df, col='px_high', ma1=5, ma2=22, feature_func=None):
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
    feature_func: function
        a function passed in to generate features. Default no additional features will be generated
        
    Returns
    -------
    trend_line_df: DataFrame
        line_df generated from trendline_func
    cross_line_df: DataFrame
        line_df and its repective crosses after the last_DATE
        
    Examples
    --------
    >>> trend_line_df, cross_line_df = full_cross_run(df, 'px_high', ma1=5, ma2=22, feature_func=add_features)
    
    '''
    if 'high' in col:
        trendline_func = get_down_trendline
    else:
        trendline_func = get_up_trendline
        
    cross_line_df = pd.DataFrame()

    for stock in sorted(df.ID.unique()):
        dff = df[df['ID']==stock].copy()
        dff = cal_ret(dff)
        try:
            _, line_df = trendline_func(dff, col, ma1=ma1, ma2=ma2)
            cross = get_trendline_crosses(dff, line_df, col, feature_func=feature_func) 

            cross_line_df = cross_line_df.append(cross.assign(ID = stock))
        except Exception as e:
            print(stock, e)
        
    trend_line_df = cross_line_df[['ID'] + list(line_df.columns)].drop_duplicates()
    
    return trend_line_df, cross_line_df.reset_index(drop=True)

def process_cross_df(cross_line_df):
    '''
    Steps
    1. Remove channels with no cross_date that is not the latest
    2. Remove channels where cross date is after the next new channel start date
    
    Parameters
    ----------
    cross_line_df: DataFrame
        cross_line_df of all stocks
        
    Returns
    -------
    cross_line_df: DataFrame
        cross_line_df of all stocks with unwanted rows removed
    '''
    # Remove not-crossed that is not latest
    cross_line_df['latest'] = cross_line_df['last_DATE'] == cross_line_df.groupby(['ID','group'])['first_DATE'].transform(max)
    cross_line_df = cross_line_df[~((cross_line_df['cross']==False)&(cross_line_df['latest']==False))].copy().drop('latest',1)

    next_first_date = cross_line_df.drop_duplicates(subset=['ID','group'])[['ID','first_DATE','last_DATE']].copy()
    next_first_date['prev_first_DATE'] = next_first_date['first_DATE'].shift(-1)

    cross_line_df = cross_line_df.merge(next_first_date, on=['ID','first_DATE','last_DATE'], how='left')
    cross_line_df['first-cross_date'] = (cross_line_df['prev_first_DATE'] - cross_line_df['cross_DATE']).map(lambda x: x.days).fillna(0)

    cross_line_df = cross_line_df[cross_line_df['first-cross_date']>=0]#.drop(['first-cross_date','prev_cross_DATE'],1,errors='ignore')

    return cross_line_df

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
    plt.rcParams['figure.figsize'] = (20,8)
    fig, ax = plt.subplots()
    df.set_index('DATE')[[col]].plot(ax=ax)
    df.set_index('DATE')[['ma_1st','ma_2nd']].plot(alpha=0.5, ax=ax) if 'ma_1st' in df.columns else None
    plt.ylim(df[col].min() - df[col].std(), df[col].max() + df[col].std())

    if 'high' in col:
        filter_df = df[(df['max_in_state'] == df['local_maxs_f1'])].drop_duplicates(subset=['state'], keep='last').copy()
        plt.scatter(filter_df['DATE'], filter_df['local_maxs'], color='green')
        plt.scatter(df['DATE'], df['local_maxs'], color='red', marker='x')
    else:
        filter_df = df[(df['min_in_state'] == df['local_mins'])].drop_duplicates(subset=['state'], keep='last').copy()
        plt.scatter(filter_df['DATE'], filter_df['local_mins'], color='green')
        plt.scatter(df['DATE'], df['local_mins'], color='red', marker='x')

    plt.title(stock)

    def draw_straight_line(ax, df, line_df, x_col='DATE'):
        step = 15
        for row in line_df.to_dict(orient='records'): #.tail(1)
            start_idx, end_idx = row['first_index']-step, row['last_index']+step

            start_idx = 0 if start_idx <0 else start_idx 
            end_idx = len(df)-1 if end_idx>len(df)-1 else end_idx     
            x = np.array([start_idx, end_idx])
            y = row['m']*x+row['c']
    #             y = row['m']*x+row['c']
    #             ax.plot([df[x_col].iloc[0], df[x_col].iloc[-1]], y, color='black')
            ax.plot([df.iloc[start_idx]['DATE'], df.iloc[end_idx]['DATE']], y )   

    draw_straight_line(ax, df, line_df)
    
def interactive_plt_trendline(df, ma1=10, ma2=60, direction='down'):
    
    if direction == 'down':
        trendline_func = get_down_trendline
        col = 'px_high'
    else:
        trendline_func = get_up_trendline
        col = 'px_low'
    stock_selec = Dropdown(options = sorted(df.ID.unique()))
    
    @interact()
    def plot(stock = stock_selec):
        dff = df.query('ID == "%s"'%stock).copy()
        dff2, line_df = trendline_func(dff, ma1=ma1, ma2=ma2)
        plt_trendline(dff2, line_df, stock, col)
    
def interactive_plt_crossline(full_df, full_line_df, col='px_high'):
    '''
    Plot price_df and trendline interactively
    
    Parameters
    ----------
    full_df: DataFrame
        dataframe of stock prices of all stocks
    full_line_df: DataFrame
        line_df of all stocks
    col: str
        px_high for downtrend, px_low for high
    
    '''
    
    def draw_straight_line2(ax, df, line_df, x_col='DATE'):
        step = 40
        for row in line_df.to_dict(orient='records'): #.tail(1)
            start_diff = df[df['DATE']==row['first_DATE']].index[0] - row['first_index']
            end_diff = df[df['DATE']==row['last_DATE']].index[0] - row['last_index']
            start_idx, end_idx = row['first_index']-10, row['last_index']+step
            
            start_idx = -start_diff if start_idx+start_diff <0 else start_idx 
            end_idx = len(df)-1- end_diff if end_idx+end_diff >len(df)-1 else end_idx    

            x = np.array([start_idx, end_idx])
            y = row['m']*x+row['c']
            ax.plot([df.iloc[start_idx+start_diff]['DATE'], df.iloc[end_idx+end_diff]['DATE']], y )  
            ax.scatter([row['first_DATE'], row['last_DATE']],[row['first_local_maxs_f1'], row['last_local_maxs_f1']], color='red')
            ax.axvline(row['cross_DATE'], color='black',linestyle='dashed')
    
    def _plot_cross(cross):
        line_df = full_line_df[full_line_df['ID']==stock_selec.value].iloc[cross:cross+1]
        dff = full_df[full_df['ID']==stock_selec.value].copy().reset_index(drop=True)

        fig, ax = plt.subplots()
        dff.set_index('DATE')[['px_high']].plot(ax=ax)
        draw_straight_line2(ax, dff, line_df)    
        print(line_df[[i for i in line_df.columns if 'return' in i]].round(2))     
        
    def update_cross_selec(stock):
        cross_selec.options = range(full_line_df[full_line_df['ID']==stock].shape[0])
    
    stock_selec = Dropdown(options = sorted(full_line_df.ID.unique()))
    init = full_line_df[full_line_df['ID']==stock_selec.value].shape[0]
    cross_selec = Dropdown(options = range(init))
    
    j = interactive(update_cross_selec, stock=stock_selec)
    i = interactive(_plot_cross, cross=cross_selec)
    k = VBox()
    display(j)
    display(i)
    display(k)

    


# def plot_candlestick_trendline(stock, plot_start_date='2020-10-01'):
#     dff = df.query('ID == "{}" and DATE >= "{}"'.format(stock, plot_start_date)).copy().reset_index(drop=True) #
#     line_df = recent_cross.query('ID == "%s"'%stock).iloc[-1:]
    
#     features = ['fprice_change_2D','accel','m','open-close_lag1','open-close_lag2']
#     pred  = rf_clf.predict(line_df[features])[0]
#     pred_prob = round(rf_clf.predict_proba(line_df[features])[:,pred][0]*100)
    
#     fig = go.Figure()
    
#     fig.add_trace(go.Candlestick(x=dff['DATE'], name='index',
#                                 open=dff['px_open'], high=dff['px_high'],
#                                 low=dff['px_low'], close=dff['px_last'],
#                                 showlegend=False))

#     fig.update_layout(title = stock + '\t (predicted {} with Probability {}%)'.format({1:'UP', 0: 'DOWN'}.get(pred), pred_prob), 
#                       height=300,  xaxis_rangeslider_visible=False,
#                      margin=dict(l=30,r=30,b=50,t=50,pad=4))
#     fig.update_yaxes(range=[dff['px_high'].min() - dff['px_high'].std(), 
#                             dff['px_high'].max() + dff['px_high'].std()])
    
# #     x = np.array([0,len(dff)-1])
#     step = 100
#     for row in line_df.to_dict(orient='records'): #.tail(1)
            
#         start_diff = dff[dff['DATE']==row['first_DATE']].index[0] - row['first_index']
#         end_diff = dff[dff['DATE']==row['last_DATE']].index[0] - row['last_index']
#         start_idx, end_idx = row['first_index']-10, row['last_index']+step

#         start_idx = -start_diff if start_idx+start_diff <0 else start_idx 
#         end_idx = len(dff)-1- end_diff if end_idx+end_diff >len(dff)-1 else end_idx    

#         x = np.array([start_idx, end_idx])
#         y = row['m']*x+row['c']

#         fig.add_trace(go.Scatter(x=[dff.iloc[start_idx+start_diff]['DATE'], dff.iloc[end_idx+end_diff]['DATE']], y=y , mode='lines',
#                                 line=dict(color='black', width=1), showlegend=False))
#         fig.add_trace(go.Scatter(x=[dff.iloc[row['first_index']+start_diff]['DATE'], dff.iloc[row['last_index']+end_diff]['DATE']], 
#                                  y=[row['first_local_maxs_f1'], row['last_local_maxs_f1']] , mode='markers',
#                                  marker_symbol='x', marker_color='black', showlegend=False))
#         fig.add_vline(row['cross_DATE'],line_dash="dash")

#     return fig

'''
Run all

cross_line_df = pd.DataFrame()

for stock in sorted(df.ID.unique())[:5]:
    dff = df[df['ID']==stock].copy()
    dff = cal_ret(dff)
    
    _, line_df = tp.get_down_trendline(dff, ma1=5, ma2=22)
    cross = get_trendline_crosses(dff, line_df)  #, feature_func=add_features
    
    cross_line_df = cross_line_df.append(cross.assign(ID = stock))
'''