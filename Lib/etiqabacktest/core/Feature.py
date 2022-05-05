from datetime import datetime
from functools import wraps
import pandas as pd
import numpy as np
import math
from typing import List

class ApplyRule:
    
    @staticmethod
    def get_action_name(a, b):
        """
        a: signal, 1 buy, -1 sell
        b: signal_diff, 0 same signal, 2 to buy, -2 to sell 
        """
        if a==1 and b==0:
            return 'hold'
        elif a==1 and b==2:
            return 'buy'
        elif a==-1 and b==-2:
            return 'sell'
        else:
            return 'na'
      
    def get_signal(self, df):
        df['signal'] = df['sell_signal'].fillna(df['buy_signal'])
        df['signal'] = df.groupby(['ID'])['signal'].ffill().fillna(-1) ###
        return df

    def get_action(f):
        @wraps(f)  #take the passed-in-func out from the wrapped() .
        def wrapped(inst, *args, **kwargs):
            df = f(inst, *args, **kwargs)
            df['signal_diff'] = df.groupby(['ID'])['signal'].diff(periods=1).fillna(2)
            df['signal_action'] = list(zip(df['signal'], df['signal_diff']))
            df['action'] = df['signal_action'].map(lambda x: ApplyRule.get_action_name(*x))
            df = df.drop(['signal_diff', 'signal_action'], axis=1)
            return df
        return wrapped
    
class Features:
    @staticmethod
    def Lag(df: pd.DataFrame, col: list, day: int, group=None):
        new_col = 'lag_{}_{}'
        if group:
            df[[new_col.format(c.lower().replace('_',''), day) for c in col]] = \
                                                        df.groupby('ID')[col].shift(day)
        else:
            df[[new_col.format(c.lower().replace('_',''), day) for c in col]] = df[col].shift(day)
        return df
    
    @staticmethod
    def MA(df: pd.DataFrame, col: str, day: int, group=None):
        new_col = 'ma_{}_{}'.format(col.lower().replace('_',''), day)
        if group:
            df[new_col] = df.groupby(group)[col].rolling(day).mean()\
                                                    .reset_index().set_index('level_1').drop(group, axis=1)
        else:
            df[new_col] = df[col].rolling(day).mean()
        return df
    
    @staticmethod
    def STDEV(df: pd.DataFrame, col: str, day: int, group=None):
        new_col = 'stdev_{}_{}'.format(col.lower().replace('_',''), day)
        if group:
            df[new_col] = df.groupby(group)[col].rolling(day).std()\
                                                    .reset_index().set_index('level_1').drop(group, axis=1)
        else:
            df[new_col] = df[col].rolling(day).std()
        return df
    
    @staticmethod
    def MMax(df: pd.DataFrame, col: str, day: int, group=None):
        new_col = 'mmax_{}_{}'.format(col.lower().replace('_',''), day)
        if group:
            df[new_col] = df.groupby(group)[col].rolling(day).max().reset_index()\
                             .set_index('level_1').drop(group, axis=1)
        else:
            df[new_col] = df[col].rolling(day).max()
        return df
    
    @staticmethod
    def MSum(df: pd.DataFrame, col: str, day: int, group=None):
        new_col = 'msum_{}_{}'.format(col.lower().replace('_',''), day)
        if group:
            df[new_col] = df.groupby(group)[col].rolling(day).sum().reset_index()\
                            .set_index('level_1').drop(group, axis=1)
        else:
            df[new_col] = df[col].rolling(day).mean()
        return df

    @staticmethod
    def PctChange(df: pd.DataFrame, col: str, day: int, group=None):
        new_col = 'pctchg_{}_{}'
        if group:
            df[[new_col.format(c.lower().replace('_',''), day) for c in col]] = \
                                                        df.groupby('ID')[col].pct_change(periods=day)
        else:
            df[[new_col.format(c.lower().replace('_',''), day) for c in col]] = df[col].pct_change(periods=day)
        return df
