from matplotlib import dates as mdates
from matplotlib.dates import DateFormatter
from matplotlib import pyplot as plt
import pandas as pd

from ..bqfields import BaseGetData
from ..preprocessing import get_prev_quarter
from .fa_report import _detect_semiannuals

def get_trail4Q_earnings(bq, memb_list, date, currency):
    d = BaseGetData(bq)
    
    df0 = d.get_data(bq.univ.list(memb_list),{'Revenue':bq.data.sales_rev_turn(fpt='Q',fa_act_est_data='A', adj='Y',
                                                                               fpr=bq.func.range(start=get_prev_quarter(date,3), end=date))})\
            .dropna(subset=['REVISION_DATE'])
    
    semiannual_companies = _detect_semiannuals(df0)
    df1 = pd.DataFrame([], columns=df0.columns)
    if semiannual_companies:
        df1 = d.get_data(bq.univ.list(semiannual_companies),{'Revenue':bq.data.sales_rev_turn(fpt='S',fa_act_est_data='A', adj='Y',
                                                                             fpr=bq.func.range(start=get_prev_quarter(date,3), end=date))})
    
    df = df0[~df0.index.isin(semiannual_companies)].append(df1)
    df['quarter'] = df['PERIOD_END_DATE'].map(lambda x: '{}Q{}'.format(x.year, (x.month-1)//3+1))    
    df_full = pd.pivot_table(df.reset_index().rename(columns={'index':"ID"}), 
                            index=['ID'], columns=['quarter'], values=['Revenue'], 
                            aggfunc=lambda x: x.max()).reset_index()
    
    not_reported = df_full[df_full[('Revenue',df['quarter'].max())].isnull()]['ID'].tolist()
    quarter_not_reported = list(set(not_reported).difference(set(semiannual_companies)))
    quarter_reported = list(set(df0.index.tolist()).difference(set(semiannual_companies)).difference(set(quarter_not_reported)))
    semiannual_not_reported = list(set(not_reported).intersection(set(semiannual_companies)))
    semiannual_reported = list(set(semiannual_companies).difference(semiannual_not_reported))
    
    dff1 = pd.DataFrame([])
    dff2 = pd.DataFrame([])
    dff3 = pd.DataFrame([])
    dff4 = pd.DataFrame([])
    params = {'fa_act_est_data':'A','adj':'Y','currency':currency}
    if quarter_reported:
        dff1 = d.get_data(bq.univ.list(quarter_reported),{'netincome':bq.data.net_income(fpt='Q',**params,
                                                                            fpr=bq.func.range(start=get_prev_quarter(date,3), end=date))})
    if quarter_not_reported:
        dff2 = d.get_data(bq.univ.list(quarter_not_reported),{'netincome':bq.data.net_income(fpt='Q',**params,
                                                                            fpr=bq.func.range(start=get_prev_quarter(date,4), end=get_prev_quarter(date,1)))})
    if semiannual_reported:
        dff3 = d.get_data(bq.univ.list(semiannual_reported),{'netincome':bq.data.net_income(fpt='S',**params,
                                                                            fpr=bq.func.range(start=get_prev_quarter(date,2), end=date))})
    if semiannual_not_reported:
        dff4 = d.get_data(bq.univ.list(semiannual_not_reported),{'netincome':bq.data.net_income(fpt='S',**params,
                                                                            fpr=bq.func.range(start=get_prev_quarter(date,3), end=get_prev_quarter(date,1)))})
    
    dff = dff1.dropna().append(dff2).append(dff3).append(dff4)
    return dff

def _get_earnings_grouped_currency(bq, tickers, date, currency_mapping):
    ticker_curr = pd.DataFrame({'tickers':[[i] for i in tickers], 'currency':[currency_mapping.get(i.split()[1]) for i in tickers]})
    ticker_curr = ticker_curr.groupby('currency')['tickers'].sum().reset_index()
    
    df_list = []
    for cur, stocks in ticker_curr.values:
        df = get_trail4Q_earnings(bq, stocks, date,currency=cur)
        df_list.append(df)
    return pd.concat(df_list)


def get_earnings_df(bq, tickers, this_q_date, currency_mapping):
    d = BaseGetData(bq)
    # 4Q EARNINGS
    earnings_df = _get_earnings_grouped_currency(bq, tickers, this_q_date, currency_mapping)
    return earnings_df.reset_index()
    

def calculate_pe_ratio(bq, index_df, earnings_df, groupby='Index'):
    d = BaseGetData(bq)
    tickers = earnings_df['ID'].unique().tolist()
    # MKT CAP
    eqy_sh_out = d.get_data(bq.univ.list(tickers), {'eqy_sh_out':bq.data.eqy_sh_out()})
    px_last = d.get_data(bq.univ.list(tickers), {'px_last':bq.data.px_last()})
    mkt_cap_df = eqy_sh_out.merge(px_last.drop('DATE',1), left_index=True, right_index=True)
    mkt_cap_df['mkt_cap'] = mkt_cap_df['eqy_sh_out']*mkt_cap_df['px_last']

    #PE Ratio
    earnings_df = earnings_df.reset_index().merge(index_df[['ID',groupby,'DATE']], on='ID')
    mkt_cap_df = mkt_cap_df.reset_index().merge(index_df[['ID',groupby,'DATE']], on='ID')
    pe_df = (mkt_cap_df.groupby(groupby)['mkt_cap'].sum() / earnings_df.groupby(groupby)['netincome'].sum()).reset_index(name='PE ratio')
    return pe_df

def plot_pe_ratio(plot_df, index_name):

    plt.rcParams['figure.figsize'] = (15,5)
    fig, ax=plt.subplots()

    plot_df.set_index('DATE')['PE_RATIO'].plot(ax=ax, color='blue')

    ax2 = ax.twinx()
    plot_df.set_index('DATE')['PE_RATIO'].plot(ax=ax2, color='blue')
    plt.title('{} P/E Ratio'.format(index_name), fontsize=20)
    avg = plot_df['PE_RATIO'].mean()
    std = plot_df['PE_RATIO'].std()
    plt.axhline(avg, color='red')
    plt.axhline(avg-std, color='#ff6200', linestyle='--', dashes=(5, 4))
    plt.axhline(avg+std, color='#ff6200', linestyle='--', dashes=(5, 4))
    ax.axhline(avg-2*std, color='#ff6200', linestyle='--', dashes=(5, 4))
    ax.axhline(avg+2*std, color='#ff6200', linestyle='--', dashes=(5, 4))
    ax2.axhline(avg-2*std, color='#ff6200', linestyle='--', dashes=(5, 4))
    ax2.axhline(avg+2*std, color='#ff6200', linestyle='--', dashes=(5, 4))
    
    space = (plot_df['PE_RATIO'].max() - plot_df['PE_RATIO'].min())*0.02
    midpoint_date = plot_df['DATE'].iloc[len(plot_df)//2]
    ax.annotate('1 std', xy=(midpoint_date,avg+std+space), xytext=(midpoint_date,avg+std+space), fontsize=14, color='#ff6200')
    ax.annotate('2 std', xy=(midpoint_date,avg+2*std+space), xytext=(midpoint_date,avg+2*std+space), fontsize=14, color='#ff6200')

    ax.xaxis.set_major_locator(mdates.MonthLocator((1,4,7,10)))
    ax.xaxis.set_major_formatter(DateFormatter('%Y-%m'))
    for tick in ax.get_xticklabels():
        tick.set_rotation(60)

    plt.grid(True, axis='y', alpha=0.5)
    return fig

'''
from etiqalib.analysis import index_pe 

earnings_df = index_pe.get_earnings_df(bq, tickers, this_quarter, currency_mapping) # Includes non-reported
pe_df = index_pe.calculate_pe_ratio(bq, index_members_df.assign(DATE=today), earnings_df)
'''