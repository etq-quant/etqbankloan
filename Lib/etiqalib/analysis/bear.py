from .interval import *
from typing import List
import numpy as np
import math

def get_max_min_dt_price_1period(df, 
                                 start_date, 
                                 end_date, 
                                 date_col='DATE', 
                                 price_col='px_last'):
    '''
    df : full df
    Filter the data between dates, aggregate the min and max price within the period of time
    '''
    win_df = df.query('DATE >= "{}" and DATE <="{}"'.format(start_date, end_date))
    min_price_date = win_df[win_df[price_col]==win_df[price_col].min()][date_col].iloc[0]

    win_df = win_df[win_df[date_col] <= min_price_date]
    win_df = win_df[(win_df[price_col] == win_df[price_col].min())|
                    (win_df[price_col] == win_df[price_col].max())]
    result = win_df[[date_col,price_col]].iloc[[0,-1]].values
    return {'columns':['peak_dt','peak_pc', 'low_dt','low_pc'],
            'data':[result[0][0], result[0][1], result[1][0],result[1][1]]}


def get_agg_datespan_full(df, 
                          df_span, 
                          func, 
                          start_col = 'win_start', 
                          end_col = 'win_end', 
                          date_col='DATE', 
                          price_col='price'):
    '''
    df : full df
    df_span : df containings the spanning dates
    Wrapper function for get_max_min_dt_price_1period
    '''
    df_agg = []
    for i,row in df_span.iterrows():
        s = row[start_col]
        e = row[end_col]
        res = func(df,s,e)
        df_agg.append(res['data'])
    df_refined = pd.DataFrame(df_agg, columns=res['columns'])
    return df_refined

def bear_market_1(df, col ='px_last', pct_change_win = 365, percentile = 10, verbose=False):
    '''
    Mark day as bear if the price change compared to 365 (default) days prior 
    is less than 10 percentile (default) of all the price change
    
    Parameters
    ----------
    df: pd.DataFrame
        dataframe that contains the date and price columns
    col: string
        price column name
    pct_change_win: int
        window of days to calculate the percentage change in price
    percentile: int
        percentile (left tail) to decide extreme drop in price
    
    Returns
    -------
    df: pd.DataFrame
        dataframe with column marking if the % change in price is lower than a certain threshold (percentile)
        
    Example
    -------
    >> df
    
            ID	       DATE   	px_last	
    0	INDU Index	1900-01-01	66.08	
    1	INDU Index	1900-01-02	68.13	
    2	INDU Index	1900-01-03	66.61	
    3	INDU Index	1900-01-04	67.15	
    4	INDU Index	1900-01-05	66.71	
    5	INDU Index	1900-01-06	66.02	
    6	INDU Index	1900-01-07	66.02	
    
    >> bear_market_1(df)
    
            ID	        DATE	px_last	px_last_pctchg365d	bear
    365	INDU Index	1901-01-01	70.71	    0.070067	    False
    366	INDU Index	1901-01-02	70.44	    0.033906	    False
    367	INDU Index	1901-01-03	67.97	    0.020417	    False
    368	INDU Index	1901-01-04	69.33	    0.032465	    False
    369	INDU Index	1901-01-05	67.68	    0.014541	    False
    370	INDU Index	1901-01-06	67.68	    0.025144	    False
    371	INDU Index	1901-01-07	67.12	    0.016662	    False
    '''
    
    df[col+'_pctchg{}d'.format(pct_change_win)] = df[col].pct_change(pct_change_win)

    recess_threshold = np.percentile(df['{}_pctchg{}d'.format(col,pct_change_win)].dropna(),percentile)
    df['bear'] = df['{}_pctchg{}d'.format(col, pct_change_win)].map(lambda x: x< recess_threshold)
    
    if verbose:
        print('Bear market marked when price change is less than {}%'.format(recess_threshold*100))
        h = plt.hist(df['{}_pctchg{}d'.format(col,pct_change_win)],bins=100)
        plt.axvline(recess_threshold, color='red')
        plt.show()
    return df

def bear_market_2(df, col ='px_last', window=90, degree=-5):
    '''
    Mark day as bear if max price in 90 (default) days prior 
    is less than -5 (default) degree of current price
    '''
    
    def y_to_degree(radian, x=20):
        """
        radian: price change percentage
        x: duration, default 20 days
        get_y(radian_to_degree(0.05))
        = 0.05
        """
        return np.rad2deg(math.atan(radian*100/x))

    df['price_winmax'] = df[col].rolling(window, 1).max()
    df['max_change'] = (df[col]/df['price_winmax']-1).map(lambda x: y_to_degree(x, window))
    df['bear'] = df['max_change']<degree
    return df

def extra_bear_logic(df, df_bear, droppct=-0.27, keep=False, date_col='DATE', price_col ='px_last'):
    df_bear = df_bear.merge(df[[date_col,price_col]].rename(columns={date_col:'peak_dt',price_col:'peak_price'}), on='peak_dt',how='left')\
                     .merge(df[[date_col,price_col]].rename(columns={date_col:'low_dt',price_col:'low_price'}), on='low_dt',how='left')
    df_bear['droppct'] = df_bear['low_price']/df_bear['peak_price']-1
    df_bear = df_bear[df_bear['droppct'] < droppct].reset_index(drop=True)
    if keep:
        return df_bear
    else:
        return df_bear.drop(['peak_price','low_price','droppct'],1 )
    
#######################################################################################################
class Pipeline:
    
    def __init__(self, df, functions: List[tuple]):
        self.functions = functions
        self.df = df
        
    def run(self):
        for func in self.functions:
            if isinstance(func, tuple):
                self.df = func[0](self.df, **func[1])
            else:
                self.df = func(self.df)
        return self.df
    
def pipeline_detect_bear(df, percentile=15, pre_win=600):
    '''
    dj, dj_bear, dj_bear_refined = pipeline_detect_bear(dj)
    '''
    df = Pipeline(df.sort_values('DATE'), [(bear_market_1, dict(percentile=percentile)), get_start_end]).run()
    df_bear = Pipeline(df, [pivot_start_end, grouping_short_intervals]).run()
    df_bear_refined = Pipeline(df, [(get_agg_datespan_full, dict(df_span=enveloping_period(df_bear, pre_win_length=pre_win), 
                                                                 func=get_max_min_dt_price_1period)),
                                    (grouping_short_intervals, dict(col ='peak_dt',col_shift ='low_dt'))]).run()
    return df, df_bear, df_bear_refined

def minibull(df, verbose=False):
    from sklearn.preprocessing import PolynomialFeatures
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import r2_score

    def plot_lr(dates, y, pred_y, res_df):
        plt.rcParams['figure.figsize'] = (20,12)
        fig, ax = plt.subplots(3, 1)

        ax[0].plot(dates, y)
        ax[0].plot(dates, pred_y)
        ax[0].set_title('Regression Fit', fontsize=20)

        x = np.arange(len(dates))
        ax[1].scatter(x, y -pred_y)
        ax[1].axhline(0, color='orange')
        ax[1].set_title('Residual plot', fontsize=20)

        ax[2].plot(dates, y)
        ax[2].scatter(res_df['DATE'].values, res_df['px_last'], color='red')
        for row in pd.pivot_table(res_df, index='line', values=['DATE', 'px_last'], columns='sign', aggfunc='first').dropna().values:
            ax[2].plot(row[:2], row[2:],color='red')
        ax[2].set_title('Mini Bull', fontsize=20)

        plt.subplots_adjust(hspace=0.4)
    
    dates = df['DATE']
    y = df['px_last'].values 
    lr = LinearRegression()

    x = np.arange(len(y))
    lr.fit(x[:, np.newaxis],y.reshape(-1,1))
    pred_y = lr.coef_[0]*x + lr.intercept_
    r2 = r2_score(y, pred_y)
    print(r2) if verbose else None
    
    if r2 < 0.86 and dates.iloc[-1].year != 2020:
        polynomial_features= PolynomialFeatures(degree=3)
        x_poly = polynomial_features.fit_transform(x[:, np.newaxis])
        lr.fit(x_poly,y.reshape(-1,1))
        pred_y = lr.predict(x_poly).reshape(1,-1)[0]
        r2 = r2_score(y, pred_y)
        print(r2) if verbose else None
        
    residuals = y - pred_y
    res_df = pd.DataFrame({'res':residuals, 'DATE':dates})
    res_df['group'] = res_df['res'].map(lambda x: 0 if x > 0 else 1).diff().map(lambda x: abs(x)).cumsum().fillna(0) # index the group
    res_df = res_df[res_df['group'].isin(res_df['group'].value_counts().reset_index().query('group > 5')['index'].values)].copy() # Drop group with little data
    res_df['group'] = res_df['res'].map(lambda x: 0 if x > 0 else 1).diff().map(lambda x: abs(x)).cumsum().fillna(0) # Reindex after dropping
    res_df['sign'] = res_df['res'].map(lambda x: 'positive' if x > 0 else 'negative')
    min_max_df = res_df.groupby(['sign','group']).agg({'res':['min','max']}).reset_index().sort_values('group')
    min_max_df.columns = [j if j else i for i,j in min_max_df.columns]
    min_max_df['res'] = min_max_df.apply(lambda x: x['min'] if x['sign'] == 'negative' else x['max'], 1)
    res_df = res_df.merge(min_max_df[['group','res']], on=['group', 'res']).merge(df[['DATE','px_last']], on='DATE')
    res_df = res_df[~((res_df['group']==0) & (res_df['sign']=='positive'))].copy()
    res_df['line'] = res_df.reset_index(drop=True).index//2
    
    if verbose:
        plot_lr(dates, y, pred_y, res_df)
    return res_df
