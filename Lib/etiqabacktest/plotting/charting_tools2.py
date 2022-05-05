from plotly.subplots import make_subplots
import plotly.figure_factory as ff
import plotly.graph_objects as go
import plotly.express as px
from ipywidgets import VBox, HBox, Layout, HTML
import pandas as pd

id_col = 'ID'
date_col = 'DATE'
px_close = 'px_last'
px_high = 'px_high'
px_low = 'px_low'
px_open = 'px_open'

def plot_return_timeseries(btobj, indexdf, 
                           quant_label='ESG', index_label="KLCI" , title="Backtest"):
    data = btobj.data
    return_value = sorted([(i, data[i]['value'], data[i]['portfolio'].index.tolist(), data[i]['trans_cost']) for i in data], key=lambda x: x[0])
    rdf = pd.DataFrame(return_value, columns=['date', 'value', 'stock', 'trans_cost'])
    rdf = rdf.merge(indexdf.rename(columns={'price':'index'}), on=['date'], how='left')

    rdf['model_return'] = rdf['value']/rdf['value'][0]*100
    rdf['index_return'] = rdf['index']/rdf['index'][0]*100

    if hasattr(btobj, 'first_day'):
        cum_ret = rdf.iloc[rdf[rdf['date'].isin(chbt.first_day)].index-1].copy()
        if cum_ret.shape[0] != 0:
            cum_ret['cum_ret'] = cum_ret['model_return'] - 100
            cum_ret['cum_ret'] = cum_ret['cum_ret'].cumsum()
#             cum_ret['cum_ret'] = cum_ret['model_return']/100
#             cum_ret['cum_ret'] = cum_ret['cum_ret'].cumprod()

            rdf = rdf.merge(cum_ret[['date','cum_ret']], on='date', how='left')
#             rdf['cum_ret'] = rdf['cum_ret'].shift().ffill().fillna(1) #0
            rdf['model_return'] = rdf['model_return'] + rdf['cum_ret']
            rdf['model_return'] = rdf['model_return'] * rdf['cum_ret']

    rdf['date'] = pd.to_datetime(rdf['date'])
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=rdf['date'], y=rdf['model_return'], name=quant_label))
    fig.add_trace(go.Scatter(x=rdf['date'], y=rdf['index_return'], name=index_label))
    
    fig.update_layout(title = "<b>"+title+"</b>", 
                      template='plotly_dark', titlefont=dict(size=20),font=dict(color='white'), paper_bgcolor='rgba(0,0,0,0)')
    fig.update_xaxes(title='Date', linewidth=2, linecolor='orange', gridwidth=0.1, gridcolor='grey', titlefont=dict(size=20))
    fig.update_yaxes(title='Return (%)', linewidth=2, linecolor='orange', gridwidth=0.1, gridcolor='grey', titlefont=dict(size=20))
    return rdf, fig

def plot_annual_return(rdf,quant_label='JIN', index_label="KLCI"):
    adf = rdf.copy()
    adf['year'] = adf['date'].dt.year
    adf = adf.groupby(['year']).agg({'value': ['first', 'last'], 'index': ['first', 'last']})
    adf['model_annual_return'] = adf[('value', 'last')]/adf[('value', 'first')]*100-100
    adf['index_annual_return'] = adf[('index', 'last')]/adf[('index', 'first')]*100-100
    adf = adf.reset_index()

    years = adf['year'].tolist()
    
    fig = go.Figure()
    fig.add_trace(go.Bar(x=years, y=adf['model_annual_return'], name=quant_label))
    fig.add_trace(go.Bar(x=years, y=adf['index_annual_return'], name=index_label))
    fig.update_layout(title = '<b>Annual Return of {} and {}<b>'.format(quant_label, index_label), template='plotly_dark',
                      titlefont=dict(size=20),font=dict(color='white'), barmode='group', 
                      paper_bgcolor='rgba(0,0,0,0)')
    fig.update_xaxes(title='Year', gridwidth=0.1, gridcolor='grey', titlefont=dict(size=20))
    fig.update_yaxes(title='Annual Return (%)', gridwidth=0.1, gridcolor='grey', titlefont=dict(size=20),
                     zeroline=True, zerolinewidth=2, zerolinecolor='orange')
    return adf, fig

def highlight_px_graph(df, title='', fig=None):
    df[date_col] = df[date_col].map(lambda x: x.date().isoformat())
    df_hori = df.query('action != "na"').groupby('period').agg({date_col:['first','last']}).reset_index()
    df_hori.columns = [j if j else i for i,j in df_hori.columns]
    
    if not fig:
        fig = go.Figure()

    fig.add_trace(go.Scatter(x=df[date_col], y=df[px_close], line_color='cyan', showlegend=False))
    b_df = df.query('action =="buy"')
    fig.add_trace(go.Scatter(x=b_df[date_col], y=b_df[px_close], name='buy', 
                             mode='markers', marker_symbol='triangle-up', marker_color='lightgreen', marker_size=15))
    s_df = df.query('action =="sell"')
    fig.add_trace(go.Scatter(x=s_df[date_col], y=s_df[px_close], name='sell', 
                             mode='markers', marker_symbol='triangle-down', marker_color='red', marker_size=15))
    
    shapes = [dict(
                    type="rect",
                    xref="x",
                    yref="paper",
                    x0=start,
                    y0=0,
                    x1=end,
                    y1=1,
                    line_width = 0,
                    fillcolor='lightskyblue',
                    opacity=0.5,
                    layer="below",
                ) for start, end in df_hori[['first','last']].values]

    fig.update_layout(title=title, shapes=shapes, xaxis_showgrid = False, template='plotly_dark')
    return fig

def sample_plot_func(stock_name, df_calls, *args):
    stock_call_df = df_calls[df_calls[id_col]==stock_name]
    
    fig = highlight_px_graph(stock_call_df, title=stock_name)
    return VBox([go.FigureWidget(fig)])

def sample_table_func(stock_name, df_calls, *args):
    stock_call_df = df_calls[df_calls[id_col]==stock_name]
    breakdown_df, div_df = get_call_table(stock_call_df)
    return HBox([HTML(breakdown_df.render()), HTML(div_df.render())])

def plot_stock_performance(calls_return_df, plot_func, table_func, ret_col = 'twrr', *args):
    '''
    Example
    -------
    >>> from etiqabacktest.plotting import charting_tools2 as chart
    
    >>> calls_return_df = get_calls_return(df_calls)
    >>> chart.plot_stock_performance(calls_return_df, chart.sample_plot_func, chart.sample_table_func, 'twrr', df_calls)
    '''
    breakdown_chart = VBox()
    breakdown_table = HBox(layout=Layout(display='flex',
                            flex_flow='column',
                            align_items='center',
                            width='100%'))

    top_performer = calls_return_df[calls_return_df[ret_col]>0].tail(20)
    top_loser = calls_return_df[calls_return_df[ret_col]<=0].head(20).sort_values(ret_col, ascending=False)

    fig = make_subplots(rows=1, cols=2)

    fig.add_trace(
        go.Bar(x=top_performer[ret_col], y=top_performer[id_col], text=top_performer[ret_col], orientation='h', showlegend=False),
        row=1, col=1
    )

    fig.add_trace(
        go.Bar(x=top_loser[ret_col], y=top_loser[id_col], text=top_loser[ret_col],orientation='h', showlegend=False),
        row=1, col=2
    )

    fig.update_layout(height=600, width=800, title_text="Side By Side Subplots", template='plotly_dark')

    f = go.FigureWidget(fig)
    selec = f.data[0]
    selec2 = f.data[1]

    def update_point(trace, points, selector):
        if points.ys:
            breakdown_chart.children = [plot_func(points.ys[0], *args)]
            breakdown_table.children = [table_func(points.ys[0], *args)]

    def update_point2(trace, points, selector):
        if points.ys:
            breakdown_chart.children = [plot_func(points.ys[0], *args)]
            breakdown_table.children = [table_func(points.ys[0], *args)]
            
    selec.on_click(update_point)
    selec2.on_click(update_point2)

    return VBox([f, breakdown_chart, breakdown_table])


def get_call_table(df_calls):
    '''
    Examples
    --------
    >>> dff = df_calls[df_calls['ID']=='TOPG MK Equity']
    >>> tbl = HBox(layout=Layout(display='flex',
                flex_flow='column',
                align_items='center',
                width='100%'))
    >>> table, dvd_table = get_call_table(dff)
    >>> tbl.children = [ HBox([HTML(table.render()), HTML(dvd_table.render())])]
    '''
    
    def white_font(s):
        return ['color: white' for v in s]
    
    group = [id_col]

    calls_table = df_calls[group+[date_col,'action','period',px_close,'cash_divs','px_adjusted']].query('action != "na"')
    calls_table[date_col] = calls_table[date_col].map(lambda x: x.date().isoformat())

    dvd_table = calls_table[calls_table['cash_divs']!=0][[date_col,'cash_divs']].copy().reset_index(drop=True)
    calls_table = calls_table.groupby(group+['period']).agg({**{'cash_divs':'sum'},**{i:['first','last'] for i in ['DATE','px_last','px_adjusted']}}).reset_index()
    calls_table = calls_table.drop([('period',''), ('px_adjusted','first')],1).round(3)
    calls_table.columns=group+['total_divs', 'buy_date','sell_date','buy_price','sell_price','sell_price_inc_divs']
#     return calls_table
    calls_table['buy - sell DATE'] = calls_table.apply(lambda x: x['buy_date'] + ' - '+ x['sell_date'], 1)
    calls_table['buy - sell PRICE (last price + divs)'] = calls_table.apply(lambda x: '{:6.3f} - {:6.3f} ({:6.3f}+{:6.3f})'\
                                                                                 .format(x['buy_price'],
                                                                                         x['sell_price_inc_divs'],
                                                                                         x['sell_price'],
                                                                                         x['total_divs']), 1)
    calls_table['Total Return (%)'] = ((calls_table['sell_price_inc_divs']/calls_table['buy_price'] - 1 ) * 100).round(2)
    calls_table = calls_table[group+['buy - sell DATE', 'buy - sell PRICE (last price + divs)','Total Return (%)']].copy()
    return calls_table.style.bar(subset=['Total Return (%)'], align='mid', color=['red','green'])\
                        .apply(white_font).set_table_styles([{'selector': '.row_heading, .blank', 'props': [('display', 'none;')]}]),\
            dvd_table.style.apply(white_font).set_table_styles([{'selector': '.row_heading, .blank', 'props': [('display', 'none;')]}])
