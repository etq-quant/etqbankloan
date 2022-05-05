import plotly.graph_objects as go
import pandas as pd
import numpy as np

id_col = 'ID'
date_col = 'DATE'
px_close = 'px_last'
px_high = 'px_high'
px_low = 'px_low'
px_open = 'px_open'

def plot_swing(swing_df, title='', fig=None):
    '''
    Parameters
    ----------
    swing_df: pd.DataFrame
        one stock dataframe with swing direction
    
    Returns
    -------
    fig: plotly.go_object
        swing plot
    '''
    rev_down = swing_df[swing_df['direction']=='Rev Down']
    rev_up = swing_df[swing_df['direction']=='Rev Up']
    
    if not fig:
        fig = go.Figure()

    fig.add_trace(go.Scatter(x = swing_df.index, y=swing_df[px_close], mode='lines', name=px_close))
    fig.add_trace(go.Scatter(x = rev_down.index, y=rev_down[px_high], mode='markers',name='swing high'))
    fig.add_trace(go.Scatter(x = rev_up.index, y=rev_up[px_low], mode='markers',name='swing low'))

    fig.update_layout(template='plotly_white', title=title)
    return fig

def add_features_1(swing_df, rolldays=5):
    swing_df['low_diff'] = swing_df[px_low].diff()
    swing_df['high_diff'] = swing_df[px_high].diff()

    swing_df['roll_high'] = swing_df[px_high].rolling(rolldays).max().fillna(swing_df[px_high])
    swing_df['roll_low'] = swing_df[px_low].rolling(rolldays).min().fillna(swing_df[px_low])

    swing_df['new_high - cur_low'] = swing_df[px_high] - swing_df[px_low].shift()
    swing_df['cur_high - new_low'] = swing_df[px_high].shift() - swing_df[px_low]

    swing_df['new_high2 - cur_low'] = swing_df[px_high] - swing_df['roll_low'].shift()
    swing_df['cur_high - new_low2'] = swing_df['roll_high'].shift() - swing_df[px_low]
    return swing_df

def swing_filter_1(swing_df, rowidx, days=10):
    def get_swing_filter(swing_df, rowidx, col):
        return np.mean(swing_df[col].loc[:rowidx].tolist()[-1*days:])
    
    swing_filter1 = get_swing_filter(swing_df, rowidx, 'cur_high - new_low2')
    swing_filter2 = get_swing_filter(swing_df, rowidx, 'new_high2 - cur_low')
    swing_filter3 = get_swing_filter(swing_df, rowidx, 'cur_high - new_low')
    swing_filter4 = get_swing_filter(swing_df, rowidx, 'new_high - cur_low')
            
    return swing_filter1, swing_filter2, swing_filter3, swing_filter4

def swing_filter_2(swing_df, rowidx, days=10):
    def get_swing_filter(swing_df, rowidx, h_col, l_col):
        avg_high = np.mean(h_col.loc[:rowidx].tolist()[-1*days:])
        avg_low = np.mean(l_col.loc[:rowidx].tolist()[-1*days:])
        
        return avg_high - avg_low
    
    swing_filter1 = get_swing_filter(swing_df, rowidx, swing_df[px_high].shift(), swing_df[px_low])
    swing_filter2 = get_swing_filter(swing_df, rowidx, swing_df[px_high],swing_df['roll_low'].shift())
    swing_filter3 = get_swing_filter(swing_df, rowidx, swing_df[px_high].shift(),swing_df[px_low])
    swing_filter4 = get_swing_filter(swing_df, rowidx, swing_df['roll_high'].shift(),swing_df[px_low])
            
    return swing_filter1, swing_filter2, swing_filter3, swing_filter4

def label_swing(df, add_features=None, swing_filter = None):
    '''
    Parameters
    ----------
    df: pd.DataFrame
        sorted date single stock dataframe
    add_features: function
        function adding newhigh - currentlow etc
    swing_filter: int, float, function
        int or float for a fixed threshold number, OR
        function to calculate threshold
        
    Returns
    -------
    swing_df: pd.DataFrame
        dataframe with swing directions added
    '''
    def init_swing(ll, hh):
        if hh > 0  and ll >= 0:
            return 'upswing'
        elif ll < 0 and hh <=0:
            return 'downswing'
        elif ll < 0 and hh >0:
            return 'upswing' #or downswing
        elif ll>=0 and hh<=0:
            return 'inside day'
    
    for col in [px_high, px_open, px_low]:
        df[col] = df[col].fillna(df[px_close])
    
    swing_df = add_features(df).iloc[1:].copy().reset_index(drop=True)
    init_swing_df = swing_df.reset_index(drop=True).copy()

    init_swing_len = swing_df.shape[0]
    direction = []
    for rowidx in range(init_swing_len):

        row = swing_df.loc[rowidx:rowidx+1].to_dict(orient='records')[0]
        
        if isinstance(swing_filter, float) or isinstance(swing_filter, int):
            swing_filter1 = swing_filter2 = swing_filter3 = swing_filter4 = swing_filter
        else:
            swing_filter1, swing_filter2, swing_filter3, swing_filter4 = swing_filter(swing_df, rowidx)
            
        if not direction:
            initialize = init_swing(row['low_diff'], row['high_diff'])
            direction.append((rowidx, initialize))
            current_direction = initialize
            continue
        
        prev_direction = [d for d in direction if d[1]!='inside day']
        
        if current_direction == 'inside day' and len(prev_direction) == 0:
            reinitialize = init_swing(row['low_diff'], row['high_diff'])
            direction.append((rowidx, reinitialize))
            current_direction = reinitialize
            continue
            
        elif current_direction == 'inside day' and len(prev_direction) != 0:
            current_direction = prev_direction[-1][1].replace('Rev Up','upswing').replace('Rev Down','downswing')

        if current_direction == 'upswing' and row['high_diff'] > 0:
            direction.append((rowidx, current_direction))

        elif current_direction == 'upswing' and row['cur_high - new_low2'] >= swing_filter1:
            direction.append((rowidx, 'Rev Down'))
            current_direction = 'downswing'

        elif current_direction == 'downswing' and row['low_diff'] < 0:
            direction.append((rowidx, current_direction))

        elif current_direction == 'downswing' and row['new_high2 - cur_low'] >= swing_filter2:
            direction.append((rowidx, 'Rev Up'))
            current_direction = 'upswing'

        elif row['low_diff'] > 0 and row['high_diff'] < 0 and \
             row['cur_high - new_low'] < swing_filter3 and \
             row['new_high - cur_low'] < swing_filter4:
            swing_df = add_features(swing_df[~swing_df.index.isin([rowidx])].copy())
            current_direction = 'inside day'
            direction.append((rowidx, current_direction))
        
        else:
            direction.append((rowidx, current_direction))

#     print(len(direction), len(init_swing_df)) #, len(swing_df_inside_day)
    direction = pd.DataFrame(direction).set_index(0).rename(columns={1: 'direction'})
    init_swing_df = init_swing_df.merge(direction, left_index=True, right_index=True)
    return init_swing_df

def swing_threader_run(df_list, add_features, swing_filter, threads=10):
    '''
    Label swing direction for each stocks using multithread
    
    Parameters
    ----------
    df_list: pd.DataFrame
        list of single stock/period dataframes
    add_features: function
    
    swing_filter: function
    
    Returns
    -------
    swing_dfs: 
    
    
    '''
    import threading
    from queue import Queue
    import time

    write_lock = threading.Lock()
    q = Queue()
    swing_dfs = []

    def generate_swing_signals(i):
        stock_df = df_list[i].copy()
        try:
            swing_df = label_swing(stock_df, add_features=add_features, swing_filter=swing_filter)

            with write_lock:
                print(threading.current_thread().name, end=' ')
                swing_dfs.append(swing_df[stock_df.columns.tolist() + ['direction']])
        except Exception as err:
            print(stock_df[id_col].unique()[0], err)

    def threader():
        while True:
            index = q.get()
            generate_swing_signals(index)
            q.task_done()

    # how many threads are we going to allow for
    for x in range(threads):
         t = threading.Thread(target=threader)
         t.daemon = True
         t.start()

    start_time = time.time()
    for i in range(len(df_list)):
        q.put(i)

    q.join()

    swing_dfs = pd.concat(swing_dfs).sort_values([id_col, date_col])

    print('\nEntire job took:',time.time() - start_time)
    return swing_dfs