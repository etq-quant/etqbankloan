from .core.Backtest import BackTesting
from .core.Feature import ApplyRule
import pandas as pd
import numpy as np

class ESG_rule(ApplyRule):
    
    def __init__(self, min_buy_score, roe_score=None, roic_score=None):
        self.min_buy_score = min_buy_score
        self.roe_score = roe_score
        self.roic_score = roic_score
        
     # ROE more than 15
    def buy_rule(self, df):
        buy_esg = df['ESG Combined Score']>= self.min_buy_score
        buy_roe = df['ROE']>=self.roe_score if self.roe_score else True
        buy_roic = df['ROIC']>=self.roic_score if self.roic_score else True
        buy = buy_esg & buy_roe & buy_roic
        df['buy_signal'] = buy.map(lambda x: 1 if x else None)
        return df
    
    def sell_rule(self, df): 
        sell_esg = df['ESG Combined Score']< self.min_buy_score
        sell_roe = df['ROE']<self.roe_score if self.roe_score else False
        sell_roic = df['ROIC']<self.roic_score if self.roic_score else False
        sell = sell_esg | sell_roe | sell_roic
        df['sell_signal'] = sell.map(lambda x: -1 if x else None)
        return df
    
    @ApplyRule.get_action
    def run(self,df):
        df = self.buy_rule(df)
        df = self.sell_rule(df)
        df = self.get_signal(df)
        return df

