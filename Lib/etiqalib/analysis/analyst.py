from ..bqfields import AnalystUpdateData
from ..preprocessing import *
from ..config import *
from .. import styling
from functools import reduce
import pandas as pd

def get_upgrade_downgrade_count(df):
    '''
    downgrade_to_sell_count = max(0, #num_sell_rec_on_day- #num_sell_rec_on_prev_day);
    upgrade_to_buy_count = max(0, #num_buy_rec_on_day- #num_buy_rec_on_prev_day);
    upgrade_from_sell_to_hold_count = max(0, #num_sell_rec_on_prev_day- #num_sell_rec_on_day);
    downgrade_from_buy_to_hold_count =max(0, #num_buy_rec_on_prev_day- #num_buy_rec_on_day);
    '''

    df['downgrade_to_sell_count'] = (df['sell'] - df['sell'].shift()).map(lambda x: max(x, 0))
    df['upgrade_to_buy_count'] = (df['buy'] - df['buy'].shift()).map(lambda x: max(x, 0))
    df['upgrade_from_sell_to_hold_count'] = (df['sell'].shift() - df['sell']).map(lambda x: max(x, 0))
    df['downgrade_from_buy_to_hold_count'] = (df['buy'].shift() - df['buy']).map(lambda x: max(x, 0))

    df['Upgrade'] = df['upgrade_to_buy_count'] + df['upgrade_from_sell_to_hold_count']
    df['Downgrade'] = df['downgrade_to_sell_count'] + df['downgrade_from_buy_to_hold_count']

#     df['Rating Revision Index'] = ((df['Upgrade']+1) / (df['Downgrade'] + 1)).map(lambda x: math.log(x))
#     df['Benchmark'] = 0
    return df

def process_targetpx(df,col='target_price', prefix='price'):
    targetpx_change = df.groupby('ID').agg({col:['first','last']}).applymap(lambda x: '{:.3f}'.format(x))
    targetpx_change.columns=[j if j else i for i,j in targetpx_change.columns]
    targetpx_change = targetpx_change.rename(columns={'first':'%s_from'%prefix,'last':'%s_to'%prefix})
    return targetpx_change

def process_targetpx2(df,col='target_price', prefix='price'):
    targetpx_change = df.groupby('ID').agg({col:['first','last']}).round(3)
    targetpx_change.columns=[j if j else i for i,j in targetpx_change.columns]
    
    cond = targetpx_change['first']/targetpx_change['last']
    targetpx_change['Upgrade'] =  (cond > 1 ).astype(int)
    targetpx_change['Downgrade'] =  (cond < 1 ).astype(int)
    targetpx_change['Remain'] =  (cond == 1 ).astype(int)

    targetpx_change = targetpx_change.rename(columns={'first':'%s_from'%prefix,'last':'%s_to'%prefix})
    return targetpx_change


def process_analyst_call(df_analyst_call, date_col = 'DATE'):
    '''
    Convert calls into analyst movement (upgrade or downgrade) by day
    Merge the aggregated sums of upgrades and downgrades with the latest calls (buy, hold, sell) 
    '''
    
    updowngrades_daily = df_analyst_call.groupby(['ID',date_col])[['buy','sell']].sum().reset_index()\
                                .groupby(['ID']).apply(get_upgrade_downgrade_count).dropna()\
                                .pipe(styling.formatdate, date_col)
    latest_calls = df_analyst_call.groupby('ID').ffill().groupby('ID').tail(1)[['buy','hold','sell']].copy()
    df_analyst = updowngrades_daily.groupby(['ID'])[['Upgrade','Downgrade']].sum().applymap(int)\
                                    .merge(latest_calls, left_index=True, right_index=True)
    return df_analyst


def process_analyst_call2(df_analyst_call):
    updowngrades_daily = df_analyst_call.groupby(['ID','DATE'])[['buy','sell']].sum().reset_index()\
                                .groupby(['ID']).apply(get_upgrade_downgrade_count).dropna()\
                                .pipe(styling.formatdate, 'DATE')
    updowngrades_daily.columns = [c.replace('_count','').replace('upgrade_','').replace('downgrade_','').replace('from_','') \
                                  for c in updowngrades_daily.columns]
    updowngrades_daily.drop(['Upgrade','Downgrade','buy','sell'],1, inplace=True)
    updowngrades_agg = updowngrades_daily.groupby('ID')[[c for c in updowngrades_daily.columns if c not in ['ID','DATE']]].sum().applymap(int)
    return updowngrades_agg    

def _analyst_updates_data(bq, univ, start, end, curr=None, fillprev=None):
    if not curr:
        curr = {}
    if not fillprev:
        fillprev = {'fill':'prev'}
        
    a = AnalystUpdateData(bq)
    
    review_fields = a.review_fields(dates=bq.func.range(start, end), **fillprev)
    df_analyst_call = a.get_data(univ,review_fields)
    
    targetpx_fields = a.targetpx_fields(dates=bq.func.range(start, end), **curr, **fillprev)
    df_targetpx_change = a.get_data(univ,targetpx_fields)
    
    return df_analyst_call, df_targetpx_change
    
def analyst_updates(bq, univ, start, end, **kwargs):
    ## Review ##
    '''
    ID | price_from|price_to|rating_from|rating_to|to_sell|to_buy|sell_to_hold|buy_to_hold|Upgrade|Downgrade|buy|hold|sell
    													
    AAGB MK Equity|0.528|0.528|1.400|1.400|0|0|0|0|0|0|0|2|18
    
    '''
    df_analyst_call, df_targetpx_change = _analyst_updates_data(bq, univ, start, end, **kwargs)
    
    df_analyst = process_analyst_call(df_analyst_call)
    df_analyst2 = process_analyst_call2(df_analyst_call)
    df_rating = process_targetpx(df_analyst_call, col='cons_rating',prefix='rating')        
    df_targetpx_change = process_targetpx(df_targetpx_change)

    ## Merge All df ##
    dfs = [df_targetpx_change, df_rating, df_analyst2, df_analyst]
    df_results = reduce(lambda left,right: pd.merge(left, right, left_index=True, right_index=True, how='left'), dfs)
    return df_results