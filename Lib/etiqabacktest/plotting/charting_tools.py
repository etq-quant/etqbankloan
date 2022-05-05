from matplotlib.dates import DateFormatter
from matplotlib import pyplot as plt
import matplotlib.pylab as pylab
import matplotlib.dates as mdates
import pandas as pd
import numpy as np

params = {'legend.fontsize': 'x-large',
          'figure.figsize': (15, 5),
         'axes.labelsize': '30',
         'axes.titlesize':'30',
         'xtick.labelsize':'25',
         'ytick.labelsize':'20'}
pylab.rcParams.update(params)

def plot_return_timeseries(btobj, indexdf,
                          quant_label='JIN', index_label="KLCI"):
    data = btobj.data
    return_value = sorted([(i, data[i]['value'], data[i]['portfolio'].index.tolist(), data[i]['trans_cost']) for i in data], key=lambda x: x[0])
    rdf = pd.DataFrame(return_value, columns=['date', 'value', 'stock', 'trans_cost'])
    rdf = rdf.merge(indexdf.rename(columns={'price':'index'}), on=['date'], how='left')

    rdf['model_return'] = rdf['value']/rdf['value'][0]*100
    rdf['index_return'] = rdf['index']/rdf['index'][0]*100

    if hasattr(btobj, 'first_day'):
        cum_ret = rdf.iloc[rdf[rdf['date'].isin(chbt.first_day)].index-1].copy()
        if cum_ret.shape[0] != 0:
    #         cum_ret['cum_ret'] = cum_ret['model_return'] - 100
    #         cum_ret['cum_ret'] = cum_ret['cum_ret'].cumsum()
            cum_ret['cum_ret'] = cum_ret['model_return']/100
            cum_ret['cum_ret'] = cum_ret['cum_ret'].cumprod()

            rdf = rdf.merge(cum_ret[['date','cum_ret']], on='date', how='left')
            rdf['cum_ret'] = rdf['cum_ret'].shift().ffill().fillna(1) #0
    #         rdf['model_return'] = rdf['model_return'] + rdf['cum_ret']
            rdf['model_return'] = rdf['model_return'] * rdf['cum_ret']

    rdf['date'] = pd.to_datetime(rdf['date'])
    fig, ax = plt.subplots()
    fig.set_size_inches(30, 10)
    ax.plot(rdf['date'], rdf['model_return'], label=quant_label)
    ax.plot(rdf['date'], rdf['index_return'], label=index_label)
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(DateFormatter('%Y'))
    plt.xlabel('date', fontsize=40)
    plt.ylabel('return (%)', fontsize=40)
    plt.title('%s vs %s'%(quant_label, index_label), fontsize=60, fontweight='bold')
    ax.legend(fontsize=20)
    plt.grid(color='r', linestyle='-', linewidth=0.4)
    plt.yticks(np.arange(0, max(rdf['model_return'].max(), rdf['index_return'].max())+100, 50))
    return ax, rdf

def plot_annual_return(rdf,quant_label='JIN', index_label="KLCI"):
    adf = rdf.copy()
    adf['year'] = adf['date'].dt.year
    adf = adf.groupby(['year']).agg({'value': ['first', 'last'], 'index': ['first', 'last']})
    adf['model_annual_return'] = adf[('value', 'last')]/adf[('value', 'first')]*100-100
    adf['index_annual_return'] = adf[('index', 'last')]/adf[('index', 'first')]*100-100
    adf = adf.reset_index()

    labels = adf['year'].tolist()
    x = np.arange(len(labels))  # the label locations
    barWidth = 0.4  # the width of the bars
    fig, ax = plt.subplots()
    fig.set_size_inches(20, 10)
    r1 = np.arange(len(x))
    r2 = [x + barWidth for x in r1]

    plt.bar(r1, adf['model_annual_return'], width=barWidth, label='JIN')
    plt.bar(r2, adf['index_annual_return'], width=barWidth, label='KLCI')

    ax.set_title('Annual Return of {} and {}'.format(quant_label, index_label), fontweight='bold', fontsize=40)
    ax.set_ylabel('Annual Return (%)', fontweight='bold', fontsize=30)
    plt.xlabel('Year', fontweight='bold', fontsize=30)
    ax.set_xticklabels(labels)
    plt.xticks([r + barWidth for r in range(len(labels))], labels)
    ax.tick_params(labelsize=20, grid_color='r')
    plt.yticks(np.arange(min(adf['model_annual_return'].min(), adf['index_annual_return'].min()//10*10), max(adf['model_annual_return'].max(), adf['index_annual_return'].max())+10, 10))
    plt.grid(color='r', linestyle='-', linewidth=0.4)
    ax.legend(fontsize=30)
    fig.tight_layout()
    plt.show()
    return adf