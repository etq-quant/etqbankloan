from .core.Backtest import BackTesting, BackTestingLeftover
import pandas as pd
import numpy as np

class StyleBackTest(BackTesting):
    buy_cols = ['date','px_last'] 
    
    def update_latest(self, hdf):
        hdf = self.get_px_last(hdf)
        return hdf
    
    def _print_result(self, date, rebal, trans_cost, hdf, bdf, sdf, ldf=pd.DataFrame([])):
        pre_date = sorted(self.data.keys())[-1]
        cash = self.data[pre_date]['cash']
        hold_count = len(hdf)
        buy_count = len(bdf)
        sell_count = len(sdf)
        value, value_p, _index_return = self._get_model_return(date, cash)

        _index_return = (self.stock_index.get(date, 0)/self.base_index)*100
        rebal = 'rebal' if rebal else '-----'
        
        action_count = self.df[self.df['date']==date].groupby(['action']).count()['date']
        sell_call = action_count.get('sell', 0)
        buy_call = action_count.get('buy', 0)
        if not ldf.empty:
            leftover_count = len(ldf)
            leftover_count = ' ({})'.format(leftover_count)
        else:
            leftover_count=''
            
        if sell_call + buy_call + len(ldf) == 0: return []
            
        return[date, #datetime.now().strftime('%D %X'),
                  '| FBM100 {:.2f} vs Model \x1b[31m{:.2f}\x1b[0m'.format(_index_return, value_p),
                  '| sell call {}, buy call {}'.format(sell_call, buy_call),
                  '| <{} \x1b[32m{}\x1b[0m> h,b,s: {}, {}, {}{}'.format(self.stock_count.get(date, 0), self.pdf.shape[0], hold_count, buy_count, sell_count, leftover_count),
                  '| cash \x1b[35m{:.2f}%\x1b[0m {:,.2f}'.format(cash/value*100, cash), 
                  '| nav {:,.2f}'.format(value),
                  '| {} '.format(rebal),
                  '| trans_fee {:.2f}'.format(trans_cost)
              ]
    
    def run(self, k, date, pdf, cash, hdf, bdf, sdf):
        '''
        rebalance on every buy call
        '''
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
            
            if cash<0 or k+1==len(self.trading_date) :
                pdf, cash, rebal = self.rebalance(pdf, bdf, cash)
                
            elif bdf.shape[0]:
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
            if self.res: print(*self.res)
    
class StyleBackTestLeftover(BackTestingLeftover, StyleBackTest):
    cap_col = 'volume'
    cap_amt = 0.03
    ldf_days = 9
    trade_min_limit = 5000
    buy_cols = ['date','px_last','volume'] 
    ldf_columns = ['date','invest_value_allocated','value_cap','leftovers', 'days']
    
    def update_latest(self, hdf):
        '''
        contains volume
        '''
        hdf = self.get_vol(hdf)
        hdf = self.get_px_last(hdf)
        return hdf
    
    def get_vol(self, df): 
        df = df.drop('volume',1, errors='ignore')
        df = df.merge(self.df_day[['volume']], left_index=True, right_index=True)
        return df
    
    def run(self, k, date, pdf, cash, hdf, bdf, sdf, ldf):
        '''
        rebalance on every buy call
        '''
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
                pdf, cash = self.buy(pdf, bdf, cash) ###!!!
        else:
            self.date = date
            if sdf.shape[0]: 
                pdf, cash = self.sell(pdf, sdf, cash)
                
            if cash<0 or k+1==len(self.trading_date) or bdf.shape[0]:
                pdf, cash, ldf, rebal = self.rebalance(date, pdf, hdf, bdf, ldf, cash)
                
            elif ldf.shape[0] and not bdf.shape[0]: #Do no rebalance when buying leftovers-only
                invest_value = self.calculate_invest_value(pdf, cash, ldf.shape[0]) #pdf here is empty.
                bdf, ldf = self.get_buy_value(invest_value, bdf, ldf) #update new bdf containing leftovers and new reduced ldf
                pdf, cash = self.buy(pdf, bdf, cash) 
                
        self.leftover_df[date.date().isoformat()] = ldf[self.ldf_columns]
        return pdf, cash, trans_cost, rebal
    
class StyleBackTestNoPrint(StyleBackTestLeftover):
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