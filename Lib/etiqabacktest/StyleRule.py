from .core.Feature import ApplyRule
import pandas as pd
import numpy as np

class OneStyleRule(ApplyRule):
    
    def __init__(self, style, factor_col='value', date_col='year_month'):
        self.style = style
        self.date_col = date_col
        self.col = factor_col
        
    def buy_rule(self, df):
        df['buy_signal'] = df[self.col].map(lambda x: 1 if x else None)
        return df
    
    def sell_rule(self, df): 
        df['sell_signal'] = df[self.col].map(lambda x: -1 if not x else None)
        return df
    
    @ApplyRule.get_action
    def run(self,df):
        df = df.query('Style == "{}"'.format(self.style))
        df = df[['ID']].drop_duplicates().assign(key=1).merge(df[[self.date_col]].drop_duplicates().assign(key=1), on='key').drop('key',1)\
                        .merge(df[['ID',self.date_col,self.col]], how='left').fillna(False).sort_values(['ID',self.date_col])
        df = self.buy_rule(df)
        df = self.sell_rule(df)
        df = self.get_signal(df)
        return df
    
class MultiStyleRule(ApplyRule):
    
    def __init__(self, no_of_styles=3, style_count_col='style_count', date_col='year_month'):
        self.no_of_styles = no_of_styles
        self.date_col = date_col
        self.col = style_count_col
        
    def buy_rule(self, df):
        df['buy_signal'] = df[self.col].map(lambda x: 1 if x >= self.no_of_styles else None)
        return df
    
    def sell_rule(self, df): 
        df['sell_signal'] = df[self.col].map(lambda x: -1 if x < self.no_of_styles else None)
        return df
    
    @ApplyRule.get_action
    def run(self,df):
        df = df[['ID']].drop_duplicates().assign(key=1).merge(df[[self.date_col]].drop_duplicates().assign(key=1), on='key').drop('key',1)\
                        .merge(df[['ID',self.date_col,self.col]], how='left').fillna(False).sort_values(['ID',self.date_col])
        df = self.buy_rule(df)
        df = self.sell_rule(df)
        df = self.get_signal(df)
        return df

class OneStyleRule2(ApplyRule):
    
    def __init__(self, style_col='value', date_col='DATE'):
        self.style_col= style_col
        self.date_col = date_col
        
    def buy_rule(self, df):
        df['buy_signal'] = df[self.style_col].map(lambda x: 1 if x else None)
        return df
    
    def sell_rule(self, df): 
        df['sell_signal'] = df[self.style_col].map(lambda x: -1 if not x else None)
        return df
    
    @ApplyRule.get_action
    def run(self, df, price_df):
        drop_cols = [ i for i in df.columns if '_zscore' in i]
        df.drop(drop_cols, 1, inplace=True)
        
        df = df[['ID']].drop_duplicates().assign(key=1).merge(df[['year_month']].drop_duplicates().assign(key=1), on='key').drop('key',1)\
                        .merge(df[['ID','year_month',self.style_col]], how='left').fillna(False)
        
        price_date_df = price_df.groupby('year_month')['DATE'].min().reset_index()
        price_date_df['DATE'] = price_date_df['DATE'].shift(-1)
        price_date_df.dropna(inplace=True)        
        df = df.merge(price_date_df, on='year_month')
        
        df_px = df[['ID',self.date_col,self.style_col]].merge(price_df, on=['ID',self.date_col], how='right').sort_values(['ID',self.date_col])
        ffill_cols = list(set(df.columns).difference(set(price_df.columns)))
        df_px[ffill_cols] = df_px.groupby('ID')[ffill_cols].ffill().fillna(False)
        df_px.dropna(subset=['px_last'], inplace=True)

        df_px = self.buy_rule(df_px)
        df_px = self.sell_rule(df_px)
        df_px = self.get_signal(df_px)
#         df_px.drop(ffill_cols, 1, inplace=True)
        return df_px

class MultiStyleRule2(ApplyRule):
    
    def __init__(self, no_of_styles=3, style_count_col='style_count', date_col='DATE'):
        self.no_of_styles = no_of_styles
        self.date_col = date_col
        self.col = style_count_col
        
    def buy_rule(self, df):
        df['buy_signal'] = df[self.col].map(lambda x: 1 if x >= self.no_of_styles else None)
        return df
    
    def sell_rule(self, df): 
        df['sell_signal'] = df[self.col].map(lambda x: -1 if x < self.no_of_styles else None)
        return df
    
    @ApplyRule.get_action
    def run(self,df,price_df):
        drop_cols = [ i for i in df.columns if '_zscore' in i]
        df.drop(drop_cols, 1, inplace=True)
        
        df = df[['ID']].drop_duplicates().assign(key=1).merge(df[['year_month']].drop_duplicates().assign(key=1), on='key').drop('key',1)\
                        .merge(df[['ID','year_month',self.col]], how='left').fillna(0)
        
        price_date_df = price_df.groupby('year_month')['DATE'].min().reset_index()
        price_date_df['DATE'] = price_date_df['DATE'].shift(-1)
        price_date_df.dropna(inplace=True)        
        df = df.merge(price_date_df, on='year_month')
        
        df_px = df.merge(price_df.drop('year_month',1), 
                         how='right', on=['ID','DATE']).sort_values(['ID','DATE'])
        df_px[self.col] = df_px.groupby('ID')[self.col].ffill().fillna(0)
        
        df_px = self.buy_rule(df_px)
        df_px = self.sell_rule(df_px)
        df_px = self.get_signal(df_px)
        return df_px