import pandas as pd
import numpy as np

def compare_two_pdf(backtest_dat, date1, date2):
    pdf1 = backtest_dat[date1]['portfolio']
    pdf2 = backtest_dat[date2]['portfolio']   
    
    compare_pdf = pdf1.merge(pdf2, left_index=True, right_index=True, how='outer')
    compare_pdf['PnL'] = compare_pdf['px_last_y'] / compare_pdf['px_last_x']-1
    
    print('{:.2f} {:.2f} {:.2f}'.format(compare_pdf['value_x'].sum(), 
                                        compare_pdf['value_y'].sum(),
                                       compare_pdf['value_y'].sum()/compare_pdf['value_x'].sum()-1))
    return compare_pdf

def annualize_rets(r, periods_per_year):
    """
    Annualizes a set of returns
    We should infer the periods per year
    but that is currently left as an exercise
    to the reader :-)
    """ 
    compounded_growth = (1+r).prod()
    n_periods = r.shape[0]
    return compounded_growth**(periods_per_year/n_periods)-1


def annualize_vol(r, periods_per_year):
    """
    Annualizes the vol of a set of returns
    We should infer the periods per year
    but that is currently left as an exercise
    to the reader :-)
    """
    return r.std()*(periods_per_year**0.5)


def sharpe_ratio(r, riskfree_rate, periods_per_year):
    """
    Computes the annualized sharpe ratio of a set of returns
    """
    # convert the annual riskfree rate to per period
    rf_per_period = (1+riskfree_rate)**(1/periods_per_year)-1
    excess_ret = r - rf_per_period
    ann_ex_ret = annualize_rets(excess_ret, periods_per_year)
    ann_vol = annualize_vol(r, periods_per_year)
    return ann_ex_ret/ann_vol

def drawdown(return_series: pd.Series):
    """Takes a time series of asset returns.
       returns a DataFrame with columns for
       the wealth index, 
       the previous peaks, and 
       the percentage drawdown
    """
    wealth_index = 1000*(1+return_series).cumprod()
    previous_peaks = wealth_index.cummax()
    drawdowns = (wealth_index - previous_peaks)/previous_peaks
    return pd.DataFrame({"Wealth": wealth_index, 
                         "Previous Peak": previous_peaks, 
                         "Drawdown": drawdowns})

def backtest_performance_metrics_monthly(rdf, rf_df):

    periods_per_year = 12
    monthly_ret = rdf.set_index('date')['value'].resample('M').last().pct_change().dropna()
    monthly_ret = pd.merge_asof(pd.DataFrame(monthly_ret.dropna()), rf_df, left_index=True, right_index=True)
    
    ret, rf = monthly_ret['value'], monthly_ret['opr']
    
    metrics =  dict(max_drawdown = drawdown(ret)['Drawdown'].min()*100,
                    annualized_ret = annualize_rets(ret, periods_per_year)*100,
                    sharperatio = sharpe_ratio(ret, rf, periods_per_year),
                    total_return = (rdf['value'].iloc[-1] / rdf['value'].iloc[0] -1)*100)
    performance = pd.DataFrame.from_dict(metrics,orient='index').T
    return performance