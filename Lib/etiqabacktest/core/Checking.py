import pandas as pd
import numpy as np


def check_buy_sell_count(df_calls):
    buy_sell_count = df_calls[df_calls['action'].isin(['buy','sell'])].groupby('ID')['action'].value_counts().reset_index(name='count')
    buy_sell_count = pd.pivot_table(buy_sell_count, index='ID', columns='action', values='count')
    diff = (buy_sell_count['buy'] - buy_sell_count['sell']).map(lambda x: abs(x))
    assert buy_sell_count[diff > 1].shape[0] == 0, 'buy sell counts ratio > 1'
    
def check_px_adjusted(df_calls):
    null_row_count = df_calls[(df_calls['action']!='na') & (df_calls['px_adjusted'].isnull())].shape[0]
    assert null_row_count == 0, 'px_adjusted contains null during holding period'