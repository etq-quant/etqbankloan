from datetime import datetime
from functools import wraps
import pandas as pd
import numpy as np
import math
from typing import List

class BackTestingUtility():
    
    @property
    def pdf(self):
        return self.__pdf
    
    @pdf.setter
    def pdf(self, pdf):
        if isinstance(pdf, pd.DataFrame):
            self.__pdf = pdf.copy()
        else:
            self.__pdf = self._create_empty_pdf()
    
    @staticmethod
    def _create_empty_pdf():
        return pd.DataFrame([], columns=['ID', 'unit', 'date', 'px_last', 'value']).set_index('ID')
    
    def _create_empty_bdf(self):
        return pd.DataFrame([], columns=['ID'] + self.buy_cols + ['value_cap', 'leftovers', 'buy_value']).set_index('ID')
    
    @staticmethod
    def get_trading_dates(tdf,
                          start_date='2000-01-01',
                          end_date='2020-01-01',):
        '''
        tdf : DataFrame for trading
        	    date	| cnt
            ------------|-----
        0	1999-01-04	| 335
        1	1999-01-05	| 352
        2	1999-01-06	| 361
            ...
        '''
        trading_dates = tdf[(tdf['date']>start_date)&(tdf['date']<end_date)]['date'].sort_values().tolist()
        return trading_dates
    
    @staticmethod
    def _get_tier(x):
        if x<=10 :
            return 10
        else:
            return x#(x-1)//5*5+5
    
    @staticmethod
    def buy(pdf, bdf, cash):
        for ID, v in bdf.iterrows():
            current_price = v['px_last']
            date = v['date']
            buy_value = v['buy_value']
            buy_unit = buy_value // current_price
            cash = cash - buy_value
            pdf.loc[ID] = [
                           buy_unit, 
                           date,
                           current_price,
                           buy_value,
                          ]
        return pdf, cash
    
    @staticmethod
    def sell(pdf, sdf, cash, **kwargs): 
        if sdf.shape[0]==0:
            return pdf, cash
        
        pdf.drop([i for i in sdf.index.tolist() if i in pdf.index], inplace=True)
        sdf['value'] = sdf['unit'] * sdf['px_last']
        return pdf, cash + sdf['value'].sum()
    
    def update_latest(self, hdf):
        hdf = self.get_avg_5_val(hdf)
        hdf = self.get_px_last(hdf)
        return hdf
    
    def rebalance(self, hdf, bdf, cash): 
        
        hdf = self.update_latest(hdf)

        if  hdf.shape[0] + bdf.shape[0] == 0: 
            return hdf, cash, False
        else:
            pdf = self._create_empty_pdf()
            _, cash = self.sell(pdf, hdf, cash)
            
            hbdf = hdf[self.buy_cols].append(bdf[self.buy_cols])
            hbdf = self.get_buy_value(pdf, hbdf, cash)
            pdf, cash = self.buy(pdf, hbdf, cash)
            return pdf, cash, True
        
    def get_px_last(self, df):
        df = df.drop('px_last', 1, errors='ignore')
        df = df.merge(self.df_day[['px_last']], left_index=True, right_index=True)
        return df
    
    def get_avg_5_val(self, df): 
        df = df.drop('avg_5_value', 1, errors='ignore')
        df = df.merge(self.df_day[['avg_5_value']],left_index=True, right_index=True)
        return df
    
    def get_trans_cost(self, t1, t2):
        t1pdf = self.data[t1]['portfolio']
        t2pdf = self.data[t2]['portfolio']

        tpdf = t1pdf.merge(t2pdf, left_index=True, right_index=True, how='outer')

        tpdf['unit'] = (tpdf['unit_x'].fillna(0) - tpdf['unit_y'].fillna(0)).abs()
        tpdf['px_last'] = tpdf['px_last_x'].fillna(tpdf['px_last_y'])
        tpdf['trans_cost'] = tpdf['px_last']*tpdf['unit']*self.trans_p
        trans_cost = tpdf['trans_cost'].sum()
        return trans_cost
    
    def get_buy_value(self, pdf, bdf, cash):
        no_of_stocks = bdf.shape[0]
        cap_per_stock = (pdf.value.sum() + cash)*(1-self.cash_reserve_ratio)*self.stock_cap_ratio
        invest_value = min(cap_per_stock, cash*(1-self.cash_reserve_ratio)/no_of_stocks)
#         invest_value = cash*(1-self.cash_reserve_ratio)/no_of_stocks

        bdf['buy_value'] = invest_value
        return bdf
    
    def update(self, date, trans_cost):
        self.data[date] = {}
        self.data[date]['portfolio'] = self.pdf.copy()
        self.data[date]['cash'] = self.cash
        self.data[date]['value'] = self.pdf['value'].sum() + self.cash
        self.data[date]['trans_cost'] = trans_cost
    
class BackTesting(BackTestingUtility):
    
    buy_cols = ['date','avg_5_value','px_last']
    
    def __init__(self, 
                 action_df: pd.DataFrame, 
                 stock_index_df: pd.DataFrame,
                 trading_dates: List[str],
                 initial_capital=10*10**6,
                 cash_reserve_ratio=0.00,
                 transaction_charge_pct=0.25/100,
                 stock_cap_ratio=0.10,
                 generation=0,
                 debug=False,
                ):
        
        '''
        action_df : DataFrame with buy and sell signal
        stock_index: DataFrame ( date | price ) for Base Index (e.g. KLCI)
        '''
        self.df = action_df.copy()
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.cash_reserve_ratio = cash_reserve_ratio
        self.trans_p = transaction_charge_pct
        self.trading_date = trading_dates.copy()  
        self.stock_index_df = stock_index_df.copy()
        self.stock_index = stock_index_df.set_index('date')['price']
        self.base_index =  self.stock_index.get(self.trading_date[0])
        self.stock_cap_ratio = stock_cap_ratio
        self.generation = generation
        self.debug = debug
        ### Record Daily Portfolio with cash, value (invested money)  ###
        self.data = {}
        self.leftover_df = {}
        self.buy_data = []
        self.sell_data = []
        ### Current Portfolio ###
        self.pdf = self._create_empty_pdf()
        
        self.stock_count = action_df[(action_df['signal']==1)].groupby('date')['action'].count()
    
    def _get_model_return(self, date, cash):
        value = self.pdf['value'].sum()+ cash
        value_p = round(value / self.initial_capital * 100, 2)
        _index_return = (self.stock_index.get(date, 0)/self.base_index)*100
        return value, value_p, _index_return
    
    def _print_result(self, date, rebal, trans_cost, hdf, bdf, sdf, *args, **kwargs):
        pre_date = sorted(self.data.keys())[-1]
        cash = self.data[pre_date]['cash']
        hold_count = len(hdf)
        buy_count = len(bdf)
        sell_count = len(sdf)

        value, value_p, _index_return = self._get_model_return(date, cash)
        action_count = self.df[self.df['date']==date].groupby(['action']).count()['date']
        hold_count = action_count.get('hold', 0)
        sell_count = action_count.get('sell', 0)
        buy_count = action_count.get('buy', 0)
        is_bull = 'bull' if self.stock_index_df.set_index('date')['is_bull'].get(date, False) else '----'
        is_bear = 'bear' if self.stock_index_df.set_index('date')['is_bear'].get(date, False) else '----'
        rebal = 'rebal' if rebal else '-----'
        return [datetime.now().strftime('%D %X'),date, 
                  '| KLCI {:.2f} vs Rodent \x1b[31m{:.2f}\x1b[0m'.format(_index_return, value_p),
                  '| <{} \x1b[32m{}\x1b[0m> {}, {}, {}'.format(self.stock_count.get(date, 0), self.pdf.shape[0], hold_count, buy_count, sell_count),
                  '| cash \x1b[35m{:.2f}%\x1b[0m {:,.2f}'.format(cash/value*100, cash), 
                  '| nav {:,.2f}'.format(value),
                  '| {} {} {}'.format(is_bull, is_bear, rebal),
#                   '| trans_fee {:.2f}'.format(trans_cost)
               ]
    
    def hold_buy_sell_df(self, k, df, pdf):
        
        buy_call = df['action']=='buy'
        bdf = df[buy_call][self.buy_cols].copy()

        sell_call = df['action']=='sell'
        not_in_today = pdf[~pdf.index.isin(df.index)]
        sdf = pdf[pdf.index.isin(df[sell_call].index)].append(not_in_today)
        sdf = self.get_px_last(sdf)
        hdf = pdf[~pdf.index.isin(df[sell_call].index)]

        if k == 0:
            bdf = bdf.append(self.df_day[self.df_day['action']=='hold'][self.buy_cols])
            
        return hdf, bdf, sdf
    
    def run(self, k, date, pdf, cash, hdf, bdf, sdf, *args):
        if k>=2:
            t = sorted(self.data.keys())
            t1, t2 = t[-1], t[-2]
            trans_cost = self.get_trans_cost(t1, t2) 
        else:
            trans_cost = 0

        cash = cash - trans_cost  
        rebal = False

        if k==0:
            if bdf.shape[0] != 0 :
                bdf = self.get_buy_value(pdf, bdf, cash) #pdf here is empty.
                pdf, cash = self.buy(pdf, bdf, cash) ###!!!
        else:
            if sdf.shape[0]: 
                pdf, cash = self.sell(pdf, sdf, cash)
                
            usable_cash = cash - (self.pdf.value.sum()+cash)*self.cash_reserve_ratio  
            if cash<0 or k+1==len(self.trading_date) or usable_cash<0:
                pdf, cash, rebal = self.rebalance(pdf, bdf, cash)
                
            elif bdf.shape[0]:
                bdf = self.get_buy_value(pdf, bdf, cash)
                if usable_cash > bdf['buy_value'].sum():
                    pdf, cash = self.buy(pdf, bdf, cash)
                else:
                    pdf, cash, rebal = self.rebalance(pdf, bdf, cash)
                                        
        return pdf, cash, trans_cost, rebal
    
    def backtest(self):
        for k, date in enumerate(self.trading_date): 
            pdf, cash = self.pdf, self.cash
            self.df_day = self.df[(self.df['date']==date)].copy().set_index('id') ###!!!
            hbsdfs = self.hold_buy_sell_df(k, self.df_day, pdf) ###!!!
            
            pdf, cash, trans_cost, rebal = self.run(k, date, pdf, cash, *hbsdfs)
            
            #MTM pdf
            pdf = self.get_px_last(pdf)      
            pdf['value'] = pdf['unit']*pdf['px_last']
            
            self.pdf, self.cash = pdf, cash
            self.update(date, trans_cost)
            self.res = self._print_result(date, rebal, trans_cost, *hbsdfs) 
            print(*self.res)
            
class BackTestingReset:
#     accum_return = 0
    accum_return = 1
    
    def _get_model_return(self, date, cash):
        value = self.pdf['value'].sum() + cash
#         value_p = round(value / self.initial_capital * 100, 2) + self.accum_return
        value_p = round((value / self.initial_capital * self.accum_return)*100, 2)
        _index_return = (self.stock_index.get(date, 0)/self.base_index)*100
        return value, value_p, _index_return
    
    def backtest(self):
        trading_dates = [datetime.strptime(x, '%Y-%m-%d') for x in self.trading_date]
        self.first_day = [i.date().isoformat() for i in pd.DataFrame({'date':trading_dates})\
                                                          .assign(year=pd.DataFrame({'date':trading_dates})['date'].map(lambda x: x.year))\
                                                          .groupby('year')['date'].min() 
                                                 if i.month == 1]
            
        for k, date in enumerate(self.trading_date): 
            pdf, cash = self.pdf, self.cash
            self.df_day = self.df[(self.df['date']==date)].copy().set_index('id') ###!!!
            hbsdfs = self.hold_buy_sell_df(k, self.df_day, pdf) ###!!!
            
            pdf, cash, trans_cost, rebal = self.run(k, date, pdf, cash, *hbsdfs)

            pdf = self.get_px_last(pdf)
            pdf['value'] = pdf['unit']*pdf['px_last']
            
            if date in self.first_day:
#                 self.accum_return = self.accum_return + round(((pdf['value'].sum()+cash)/self.initial_capital - 1) * 100, 2)
                self.accum_return = self.accum_return * round(((pdf['value'].sum()+cash)/self.initial_capital) , 2)

                reset_amount = self.initial_capital * (1-self.cash_reserve_ratio)
                if pdf['value'].sum() > reset_amount:
                    cash_reserve = self.initial_capital * self.cash_reserve_ratio
                    cash = cash_reserve if cash > cash_reserve else cash
                    
                    pdf['new_val'] = (pdf['value']/pdf['value'].sum()) * reset_amount 
                    pdf['unit'] = (pdf['new_val']/pdf['px_last']).map(lambda x: math.floor(x))
                    pdf['value'] = pdf['unit']*pdf['px_last']
                    pdf = pdf.drop('new_val',1).copy()
                    
                else:
                    cash = self.initial_capital - pdf['value'].sum()
            
            self.pdf, self.cash = pdf, cash    
            self.update(date, trans_cost)
            self.res = self._print_result(date, rebal, trans_cost, *hbsdfs)
            print(*self.res)
            
class BackTestingLeftover:
    
    cap_col = 'volume'
    cap_amt = 0.2
    ldf_days = 3
    trade_min_limit = 5000
    ldf_columns = ['date','invest_value_allocated','value_cap','leftovers', 'days']
    
    def rebalance(self, date, pdf, hdf, bdf, ldf, cash): 
        if  hdf.shape[0]  + bdf.shape[0] == 0:  #buy call when empty pdf
            return pdf, cash, ldf, False 
        else:
            hdf = self.update_latest(hdf)
            pdf_selling = pdf[~pdf.index.isin(hdf.index)].copy()
            
            empty_pdf, nav = self.sell(self._create_empty_pdf(), hdf, cash, rebal=True)
            invest_value = self.calculate_invest_value(empty_pdf, nav, hdf.shape[0]+bdf.shape[0]-(hdf[hdf.index.isin(bdf.index)].shape[0]))

            bdf = bdf[~bdf.index.isin(pdf.index)].copy()
            bdf2 = hdf[(hdf['value']<invest_value)&
                       (~hdf.index.isin(ldf.index))].copy() #Top up stocks in holding if value less than new rebalance value
            bdf2['leftovers'] = invest_value - bdf2['value']
            ldf = ldf.append(bdf2[['date','leftovers']])
            
            hdf['value'] = hdf['value'].map(lambda x: min(x, invest_value))
            
            if bdf.shape[0] + ldf.shape[0] == 0 :
                hdf['unit'] = hdf['value']//hdf['px_last']
                hdf = hdf.append(pdf_selling, sort=True)
                return hdf, nav-hdf['value'].sum(), ldf, True
            
            bdf, ldf = self.get_buy_value(invest_value, bdf, ldf)
            hbdf = hdf[['date','px_last','value']].rename(columns={'value':'buy_value'}).append(bdf, sort=True)
            hbdf = hbdf.reset_index().groupby('index').agg({'date':'max','px_last':'max','buy_value':'sum'})
            pdf, cash = self.buy(empty_pdf, hbdf, nav)
            pdf = pdf.append(pdf_selling) if pdf_selling.shape[0] else pdf
    
            return pdf, cash, ldf, True
    
    @staticmethod
    def buy(pdf, bdf, cash):
        pdf = pdf[['unit','date','px_last','value']].copy()
        for ID, v in bdf.iterrows():
            current_price = v['px_last']
            date = v['date']
            buy_value = v['buy_value']
            buy_unit = buy_value // current_price
            cash = cash - buy_value
            if ID in pdf.index:
                buy_unit = pdf.loc[ID]['unit'] + buy_unit
                buy_value = pdf.loc[ID]['value'] + buy_value
            pdf.loc[ID] = [
                           buy_unit, 
                           date,
                           current_price,
                           buy_value,
                          ]
        return pdf, cash
    
    def calculate_invest_value(self, pdf, cash, no_of_stocks):
        '''
        pdf : to get porfolio mtm value
        cash: available cash
        no_of_stocks: number to buy
        '''
        cap_per_stock = (pdf.value.sum() + cash)**(1-self.cash_reserve_ratio)*0.1
        invest_value = min(cap_per_stock, cash*(1-self.cash_reserve_ratio)/no_of_stocks)
        return invest_value

    def get_buy_value(self, invest_value, bdf, ldf):
        '''
        0. Filter stocks that has liquidity (volume_cap > 0), count stocks
        1. invest_value_allocated = invest_value = min(cap_per_stock, cash_equally_divided_by_stock_count)
        2. invest_value_allocated = min(invest_value_allocated, leftovers)
        3. buy_value = min(volume_cap, invest_value_allocated)
        '''

        ldf = self.update_latest(ldf)
        
        bdf = bdf.append(ldf.drop(['invest_value_allocated','value_cap'],1), sort=True)
        bdf['value_cap'] = bdf[self.cap_col]*self.cap_amt 
        bdf['invest_value_allocated'] = invest_value
        bdf['leftovers'] = bdf['leftovers'].fillna(bdf['invest_value_allocated'])
        bdf['invest_value_allocated'] = bdf.apply(lambda x: min(x['leftovers'],x['invest_value_allocated']), 1)

        bdf['buy_value'] = bdf.apply(lambda x: min(x['invest_value_allocated'] ,x['value_cap']),1)

        ldf = bdf[bdf['invest_value_allocated'] > bdf['value_cap']].copy()
        ldf['days'] = ldf['days'].fillna(self.ldf_days+1) - 1
        ldf['leftovers'] = ldf['invest_value_allocated'] - ldf['value_cap']
        return bdf.drop(['invest_value_allocated'],1).copy(), ldf
        
    def hold_buy_sell_df(self, k, df, pdf):
        date = df['date'].max().date().isoformat()
        
        buy_call = df['action']=='buy'
        bdf = df[buy_call][self.buy_cols].copy()

        sell_call = df['action']=='sell'
        sdf = pdf[pdf.index.isin(df[sell_call].index)]  # Stocks in portfolio that encounter sell call
        
        if k == 0:
            bdf = bdf.append(self.df_day[self.df_day['action']=='hold'][self.buy_cols])
            self.leftover_df[date] = pd.DataFrame([], columns=self.ldf_columns)
#             self.sell_leftover_df[date] = pd.DataFrame([], columns=pdf.columns)
            ldf = pd.DataFrame([], columns=self.ldf_columns)
        else:
            d = sorted(self.leftover_df.keys())
            ldf = self.leftover_df[d[-1]]
            ldf = ldf[~(ldf.index.isin(sdf.index)) & (ldf['days']>0)].copy()
#             sldf = self.sell_leftover_df[d[-1]]
#             sdf = sdf.append(sldf)
        
        sdf = self.get_px_last(sdf)
        hdf = pdf[~pdf.index.isin(df[sell_call].index)]
        
        return hdf, bdf, sdf, ldf
    
    def run(self, k, date, pdf, cash, hdf, bdf, sdf, ldf):
        if k>=2:
            t = sorted(self.data.keys())
            t1, t2 = t[-1], t[-2]
            trans_cost = self.get_trans_cost(t1, t2) 
        else:
            trans_cost = 0

        cash = cash - trans_cost  
        rebal = False

        if k==0:
            if bdf.shape[0] != 0 :
                invest_value = self.calculate_invest_value(pdf, cash, bdf.shape[0]) #pdf here is empty.
                bdf, ldf = self.get_buy_value(invest_value, bdf, ldf) 
                pdf, cash = self.buy(pdf, bdf, cash) 
        else:
            self.date = date
            if sdf.shape[0]: 
                pdf, cash = self.sell(pdf, sdf, cash)
                
            usable_cash = cash - (pdf.value.sum()+cash)*self.cash_reserve_ratio
            if cash<0 or k+1==len(self.trading_date) or usable_cash<0:
                pdf, cash, ldf, rebal = self.rebalance(date, pdf, hdf, bdf, ldf, cash)
                
            elif bdf.shape[0] + ldf.shape[0]:
                invest_value = self.calculate_invest_value(pdf, cash, bdf.shape[0]+ldf.shape[0])
                bdf, ldf = self.get_buy_value(invest_value, bdf, ldf)
                if usable_cash > bdf['buy_value'].sum() :
                    pdf, cash = self.buy(pdf, bdf, cash)
                else:
                    pdf, cash, ldf, rebal = self.rebalance(date, pdf, hdf, bdf, ldf, cash)
                    
        self.leftover_df[date] = ldf[self.ldf_columns]                                
        return pdf, cash, trans_cost, rebal
    
    
class BackTestingSellLeftover:
    sell_leftover_df = {}
    
    def sell(self, pdf, sdf, cash, rebal=False): 
        
        if sdf.shape[0]==0:
            return pdf, cash
        
        if rebal:
            sdf['value'] = sdf['unit'] * sdf['px_last']
            return pdf, cash + sdf['value'].sum()
        
        sdf['unit_cap'] = sdf.apply(lambda x: min(x['value'], x[self.cap_col]*self.cap_amt),1) // sdf['px_last']
        sldf = sdf[sdf['unit']>sdf['unit_cap']].copy()

        if sldf.shape[0] != 0:
            sldf['unit'] = sldf['unit'] - sdf['unit_cap']
            sldf['value'] = sldf['unit']*sldf['px_last']
            self.sell_leftover_df[self.date] = sldf
            pdf = pdf.drop(sdf.index).append(sldf[pdf.columns], sort=True).copy()
        else:
            pdf = pdf.drop(sdf.index)

        return pdf, cash + (sdf['unit_cap']*sdf['px_last']).sum()
    
    def hold_buy_sell_df(self, k, df, pdf):
        date = df['date'].max().date().isoformat()
        buy_call = df['action']=='buy'
        bdf = df[buy_call][self.buy_cols].copy()
        
        sell_call = df['action']=='sell'
        sdf = pdf[pdf.index.isin(df[sell_call].index)]  # Stocks in portfolio that encounter sell call
        
        if k == 0:
            bdf = bdf.append(self.df_day[self.df_day['action']=='hold'][self.buy_cols],sort=True)
            self.leftover_df[date] = pd.DataFrame([], columns=self.ldf_columns)
            ldf = pd.DataFrame([], columns=self.ldf_columns)
        else:
            d = sorted(self.leftover_df.keys())
            ldf = self.leftover_df[d[-1]]
            ldf = ldf[~(ldf.index.isin(sdf.index)) & (ldf['days']>0)].copy()
            
            if d[-1] in self.sell_leftover_df:
                sldf = self.sell_leftover_df[d[-1]]
                sldf = sldf[~sldf.index.isin(bdf.index)]
                sdf = sdf.append(sldf[pdf.columns], sort=True) if sldf.shape[0] else sdf
                
        sdf = self.update_latest(sdf) #Add volume column
        
        hdf = pdf[~pdf.index.isin(sdf.index)]    
        
        return hdf, bdf, sdf, ldf
    