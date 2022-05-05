import bql

class BQfields:
    
    def __init__(self, bq):
        self.all_fields = dict(
            ######## Demgraphic ########
            sector=bq.data.gics_sector_name(),
            sub_sector=bq.data.classification_name(classification_scheme='GICS', classification_level='2'),
            name=bq.data.name(),
            syariah=q.data.is_islamic(), 
            ######## Analylst ##########
            buy_calls = bq.data.tot_buy_rec,
            sell_calls = bq.data.tot_sell_rec,
            hold_calls = bq.data.tot_hold_rec,
            analyst_rec = bq.data.tot_analyst_rec,
            target_price = bq.data.best_target_price,
            cons_rating = bq.data.eqy_rec_cons, #consensus rating
            ####### Estimates ###########
            revenue = bq.data.sales_rev_turn,
            net_income = bq.data.net_income,
            ####### Accounting Ratio ###########
            px_to_book = bq.data.PX_TO_BOOK_RATIO,  # Price to Book Ratio
            px_to_sales = bq.data.PX_TO_SALES_RATIO, # Price to Sales Ratio
            ret_com_eqy = bq.data.RETURN_COM_EQY, # Return on Common Equity
            ret_on_inv_cap = bq.data.RETURN_ON_INV_CAPITAL, # Return on Invested Capital
            ret_on_asset = bq.data.RETURN_ON_ASSET, # Return on Assets
            net_debt_to_shrhldr_eqty = bq.data.NET_DEBT_TO_SHRHLDR_EQTY, # Return on Assets
            ev_to_ebit = bq.data.EV_TO_EBIT, # Enterprise Value/EBIT
            ev_to_ebitda = bq.data.EV_TO_EBITDA, # Enterprise Value/EBITDA
            prof_margin = bq.data.PROF_MARGIN, # profit margin
            oper_prof_margin = bq.data.IS_OPER_INC, #Operating Income or Losses
            oper_margin = bq.data.OPER_MARGIN, # Operating Margin
            ebitda_margin = bq.data.EBITDA_MARGIN, # Trailing 12M EBITDA Margin
            ebit_margin = bq.data.EBIT_MARGIN, # Trailing 12M EBIT Margin
            pretax_margin = bq.data.PRETAX_MARGIN, # Pretax Margin
            cur_ratio = bq.data.CUR_RATIO, # Current Ratio
            quick_ratio = bq.data.QUICK_RATIO, # Quick Ratio
            eps_growth = bq.data.EPS_GROWTH, # EPS - 1 Yr Growth
            sales_growth = bq.data.SALES_GROWTH, # Revenue Growth Year over Year
            acct_rcv_turn = bq.data.ACCT_RCV_TURN, # Accounts Receivable Turnover
            asset_turnover = bq.data.ASSET_TURNOVER, # Asset Turnover
            roe = bq.data.ADJUSTED_ROE_AS_REPORTED,  # Adjusted Return on Equity - As Reported
            roic = bq.data.RETURN_ON_INV_CAPITAL, # Return on invested capital
            roa = bq.data.NORMALIZED_ROA, # Normalized Return on Assets
            debt_to_assets = bq.data.TOT_DEBT_TO_TOT_ASSET, #Total Debt to Total Assets
            debt_to_equity = bq.data.TOT_DEBT_TO_TOT_EQY,  #Total Debt to Total Equity
            cf_share_growth = bq.data.free_cash_flow_per_sh_growth,
            cf_firm = bq.data.CF_FREE_CASH_FLOW_FIRM, # Free Cash Flow to Firm
            pe_ratio =  bq.data.PE_RATIO,
        )
        
    def get(self, field: str):
        return self.all_fields.get(field)
        

class BaseGetData:
    def __init__(self, bq):
        self.bq = bq
    
    def get_data(self, security, fields):
        request =  bql.Request(security, fields)
        response = self.bq.execute(request)
        df = bql.combined_df(response)
        return df
    
    def get_data2(bq, security, fields, reset_index=False):
        drop_items = ['ORIG_IDS','ITERATION_DATE','ITERATION_ID', 'CURRENCY','REVISION_DATE']

        request =  bql.Request(security, fields)
        response = bq.execute(request)
        data = [r.df().drop(drop_items, axis='columns', errors='ignore') for r in response]
        if reset_index:
            data = [df.rename(columns={'AS_OF_DATE':'DATE'}).reset_index().set_index(['ID', 'DATE']) for df in data]
        return pd.concat(data, axis=1)
    
    def download_incremental(self, ticker, func, start_date, end_date, steps=5):
        bq=self.bq
        start_year = int(start_date.split('-')[0])
        end_year = int(end_date.split('-')[0])

        if start_year != end_year:
            years = list(range(start_year, end_year+1, steps))  
            df_list = []

            for y in range(len(years)-1):
                if years[y+1] != years[-1]:
                    end = '{}-12-31'.format(years[y+1]-1)
                else:
                    end = end_date

                if years[y] == years[0]:
                    start=start_date
                else:
                    start = '{}-01-01'.format(years[y])

                df = func(bq, ticker, start, end)
                df_list.append(df)

            return pd.concat(df_list)
        else:
            return func(bq, ticker, start_date, end_date)
    
class AnalystUpdateData(BaseGetData):
    def __init__(self, bq):
        super().__init__(bq)
        
    def review_fields(self,**kwargs):
        '''
        Function to get analyst calls 

        fields = get_review_fields(dates=get_dt_range('-3M','0D'), **fillprev)
        '''
        bq=self.bq
        fields = {
            'buy' : bq.data.tot_buy_rec(**kwargs),
            'sell' : bq.data.tot_sell_rec(**kwargs),
            'hold' : bq.data.tot_hold_rec(**kwargs),
            'cons_rating':bq.data.eqy_rec_cons(**kwargs),
        }
        return fields

    def targetpx_fields(self,**kwargs):
        '''
        Function to get analyst target price 

        fields = get_targetpx(dates=get_dt_range('-3M','0D'), **fillprev)
        '''
        bq=self.bq
        fields = {
            'target_price' : bq.data.best_target_price(**kwargs),
        }
        return fields

class FinStatement(BaseGetData):
    def __init__(self, bq):
        super().__init__(bq)
        
    def growth_fields(self,**kwargs):
        '''
        Function to get growth indicators

        fields = get_growth_fields(**fpr_params, **act_params, **adj_params, **quarterly, **curr)
        '''
        bq=self.bq
        fields = {
            'revenue':bq.data.sales_rev_turn(**kwargs),
            'net_income': bq.data.net_income(**kwargs)
        }
        return fields

    def quality_fields(self,**kwargs):
        '''
        Function to get value indicators

        fields = get_quality_fields(**act_params, **adj_params)
        '''
        bq=self.bq
        fields = {
            'roe':bq.data.return_com_eqy(**kwargs),
            'roic':bq.data.return_com_eqy(**kwargs),
        }
        return fields
    
    def revision_fields(self,**kwargs):
        '''
        fields = get_revision(**est_params, **rev_win_params, **adj_params)
        '''
        bq=self.bq
        fields = {
            'Revenue Rev Up': bq.data.sales_rev_turn(**kwargs, fa_stat_revision='NETUP')['value'],
            'Revenue Rev Down': bq.data.sales_rev_turn(**kwargs,fa_stat_revision='NETDN')['value'],
            'Netincome Rev Up': bq.data.sales_rev_turn(**kwargs,fa_stat_revision='NETUP')['value'],
            'Netincome Rev Down': bq.data.sales_rev_turn(**kwargs,fa_stat_revision='NETDN')['value']
        }
        return fields

