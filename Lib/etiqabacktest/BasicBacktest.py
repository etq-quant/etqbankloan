from .core.Backtest import BackTesting
import pandas as pd
import numpy as np

class BasicBackTest(BackTesting):
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
            
        return[date, #datetime.now().strftime('%D %X'),
                  '| KLCI {:.2f} vs Model \x1b[31m{:.2f}\x1b[0m'.format(_index_return, value_p),
                  '| sell call {}, buy call {}'.format(sell_call, buy_call),
                  '| <{} \x1b[32m{}\x1b[0m> h,b,s: {}, {}, {}{}'.format(self.stock_count.get(date, 0), self.pdf.shape[0], hold_count, buy_count, sell_count, leftover_count),
                  '| cash \x1b[35m{:.2f}%\x1b[0m {:,.2f}'.format(cash/value*100, cash), 
                  '| nav {:,.2f}'.format(value),
                  '| {} '.format(rebal),
                  '| trans_fee {:.2f}'.format(trans_cost)
              ]
    
    
class BasicBackTestNoPrint(BasicBackTest):
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
#             print(*self.res)
