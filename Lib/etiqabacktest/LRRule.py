from .core.Backtest import BackTesting
from .core.Feature import ApplyRule
import pandas as pd
import numpy as np

class OneFactor_rule(ApplyRule):
    
    def __init__(self, factor_col, b_score=0, s_score=0):
        self.col = factor_col
        self.b_score = b_score
        self.s_score = s_score
        
    def buy_rule(self, df):
        buy = df[self.col] > self.b_score
        df['buy_signal'] = buy.map(lambda x: 1 if x else None)
        return df
    
    def sell_rule(self, df): 
        sell = df[self.col] < self.s_score
        df['sell_signal'] = sell.map(lambda x: -1 if x else None)
        return df
    
    @ApplyRule.get_action
    def run(self,df):
        df = self.buy_rule(df)
        df = self.sell_rule(df)
        df = self.get_signal(df)
        return df
    
class LR_rule(ApplyRule):
    
    def __init__(self, b_score=0, s_score=0):
        self.b_score = b_score
        self.s_score = s_score
        
    def buy_rule(self, df):
        buy = df['pred'] > self.b_score
        df['buy_signal'] = buy.map(lambda x: 1 if x else None)
        return df
    
    def sell_rule(self, df): 
        sell = df['pred'] < self.s_score
        df['sell_signal'] = sell.map(lambda x: -1 if x else None)
        return df
    
    @ApplyRule.get_action
    def run(self,df):
        df = self.buy_rule(df)
        df = self.sell_rule(df)
        df = self.get_signal(df)
        return df
    
