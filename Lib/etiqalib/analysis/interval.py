from matplotlib import pyplot as plt
import matplotlib.dates as mdates
from datetime import timedelta
import pandas as pd

def get_start_end(df, col='bear'):
    '''
    Compare previous *state* with current *state* (Boolean Boolean)
    (True, True) or (False, False) -> no change in state
    (False, True) & (True, False) -> start & end of an inversion
    '''
    
    df['col_lag'] = df[col].shift()
    df['state'] = df\
            .apply(lambda x: 'start' if x[col] == True and x['col_lag'] == False else \
                         ('end' if x[col] == False and x['col_lag'] == True  else 'stay'), 1) 
    return df.drop('col_lag',1)

def pivot_start_end(df, 
                    col='state', 
                    date_col='DATE',
                    start_col = 'start',
                    end_col = 'end'):
    '''
    | date | state |      | start |  end |
    |------|-------|      |-------|------|  
    |  a   | stay  |      |   b   |   e  |
    |  b   | start | ---> 
    | ..   |  ...  |
    |  e   | end   |
    
    '''
    df = df[(df[col]==end_col) | (df[col] == start_col)][[col,date_col]].copy().reset_index(drop=True)
    df['index'] = df.index//2
    df = pd.pivot_table(df, values=date_col,columns=col,index='index', aggfunc='first')[[start_col,end_col]]
    return df

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
    df['index'] = df.groupby('ID')[date_col].apply(lambda x: (x.rank()-1)//2)
    df = pd.pivot_table(df, values= date_col,columns=col,index=['index','ID'], aggfunc='first')[[start_col,end_col]].reset_index()
    return df.drop('index',1).sort_values('ID')

def grouping_short_intervals(df, 
                             col ='start', 
                             col_shift='end', 
                             days_diff_threshold = 30, 
                             agg = True):
    '''
    Points/Short intervals where the next occurrence is less than threshold are grouped together
    '''
    
    df['from_last'] = df[col] - df[col_shift].shift()
    df['mark'] = df['from_last'].map(lambda x: x>timedelta(days_diff_threshold)).cumsum()
    if not agg:
        return df
    else:
        if col != col_shift:
            return df.groupby('mark').agg({col:'first',col_shift:'last'})[[col, col_shift]]
        else:
            df = df.groupby('mark').agg({col:['first','last']}).reset_index()
            df.columns = [j if j else i for i,j in df.columns]
            df = df.rename(columns={'first':col,'last':col_shift})
            return df.set_index('mark')[[col, col_shift]]
        
def enveloping_period(df, pre_win_length=365, post_win_length=365):
    '''
    input: start | end|
    
    impute enlarged window on start & date of df_span
    if win_length = 30 days
    
    BEFORE:       (30 days) <------------------> (30+1 days)
    AFTER :     <----------------------------------------->
           2000-01-01 _ 2000-03-01 _____ 2000-04-12 _ 2000-05-13 
            win_start      start             end        win_end   
            
    return: start | end | win_start | win_end
    
    '''
    df['win_start'] = df['start'].map(lambda x: x-timedelta(pre_win_length))
    df['win_end'] = df['end'].map(lambda x: x+timedelta(post_win_length))
    return df

def span_background_area(df_start_end, start_col='start', end_col='end', start_date=None, end_date=None, ax = None, alpha=0.5, color='red'):
    '''
    |    start   |     end    |
    | 1978-08-17 | 1980-05-02 |
    | 1980-09-11 | 1981-11-06 |
    '''
    df_span = df_start_end.applymap(lambda x: str(x))
    start_date = start_date or df_span[start_col].min()
    end_date = end_date or df_span[end_col].max()
    plot_ax = plt if not ax else ax
    for start, end in df_span[[start_col, end_col]].values:
        if start>=start_date and end <= end_date:
            plot_ax.axvspan(mdates.datestr2num(start), mdates.datestr2num(end), color=color, alpha=alpha)