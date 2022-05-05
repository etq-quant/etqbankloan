## define the UI
from ipywidgets import VBox, HBox, Dropdown, HTML
from bqplot import LinearScale, DateScale, OrdinalScale, Bars, Tooltip, Lines, Scatter, Axis, Figure
from bqplot.interacts import IndexSelector
from datetime import datetime
import pandas as pd

def bar_chart_hori(df, col ='twrr', top=20):
    '''
    Return need to be in less than 1 and greater than 1 mode
    '''
    scale_x, scale_x2 = OrdinalScale(), OrdinalScale()
    scale_y, scale_y2 = LinearScale(), LinearScale()
    
    df = df.sort_values(col, ascending=False)
    dff = df[df[col]>1].head(top).copy().sort_values(col)
    dff[col] = (dff[col]-1)*100
    mark_bar = Bars(x=dff.index,
                        y=dff[col],
                        scales={'x': scale_x, 'y': scale_y},
                        orientation='horizontal',
                        padding=0.15,
                        colors=dff[col].map(lambda x: 'red' if x<0 else 'green').tolist(),
                        interactions={'click': 'select'},
                        unselected_style={'opacity': .5},
                        label_display=True,
                        label_font_style={'font-size':'10', 'fill':'white'})
#                         tooltip=Tooltip(fields=['y'], formats=['.2f'], show_labels=False))
    
    df = df.sort_values(col)
    dff = df[df[col]<1].head(top).copy().sort_values(col, ascending=False)
    dff[col] = (dff[col]-1)*100
    mark_bar2 = Bars(x=dff.index,
                        y=dff[col],
                        scales={'x': scale_x2, 'y': scale_y2},
                        orientation='horizontal',
                        padding=0.15,
                        colors=dff[col].map(lambda x: 'red' if x<0 else 'green').tolist(),
                        interactions={'click': 'select'},
                        unselected_style={'opacity': .5},
                        label_display=True,
                        label_font_style={'font-size':'10', 'fill':'white'})
#                         tooltip=Tooltip(fields=['y'], formats=['.2f'], show_labels=False))
    # Start plot with all bars selected
    mark_bar.selected = list(range(len(mark_bar.x)))

    # Create Axes
    axis_x = Axis(scale=scale_x, orientation='vertical')
    axis_y = Axis(scale=scale_y, label='Total return (%)')

    axis_x2 = Axis(scale=scale_x2, orientation='vertical')
    axis_y2 = Axis(scale=scale_y2, label='Total loses (%)')

    # Create Figure
    fig_margin = {'top':60,'bottom':50,'left':200,'right':40}
    
    figure = Figure(marks=[mark_bar], axes=[axis_x, axis_y],
                    title='Top Performers',
                    title_style={'font-size': '22px'},
                    fig_margin=fig_margin,
                    layout={'width': '100%', 'height': '450px'})

    figure2 = Figure(marks=[mark_bar2], axes=[axis_x2, axis_y2],
                     title='Top Losers',
                     title_style={'font-size': '22px'},
                     fig_margin=fig_margin,
                     layout={'width': '100%', 'height': '450px'})
    
    return HBox([figure, figure2]) , (mark_bar, mark_bar2)

def create_line_chart():
    # define the characteristics of the line chart (popup)
    sc_x, sc_y = DateScale(), LinearScale()
    ax_x, ax_y = Axis(scale=sc_x, tick_rotate=-45, tick_style={'text-anchor': 'end'}, label_location='middle', 
                      grid_lines='none', tick_format='%d-%b-%y', num_ticks=10), \
                 Axis(orientation='vertical', scale=sc_y, label_location='middle', grid_lines='none')

    lines = Lines(x=[], y=[], colors=['ivory'],
                  scales={'x': sc_x, 'y': sc_y}, stroke_width=2)

    signal_value = Scatter(x=[],y=[],colors=['green'], marker='triangle-up',
                           scales={'x': sc_x, 'y': sc_y}, stroke_width=2)

    signal_value_2 = Scatter(x=[],y=[],colors=['red'], marker='triangle-down',
                       scales={'x': sc_x, 'y': sc_y}, stroke_width=2)
    # add the index brusher
    highlighted_text = HTML('''<div style="line-height:1.1;font-size:0.9em;color:ivory;padding:8px;">
                                            <i>Click on the chart to check the levels for the chart</i></div>''')
    index_sel = IndexSelector(scale=sc_x, marks=[lines])
    
    def index_change_callback(change):
        if change['new']:
            p_date = pd.to_datetime(change['new'][0]).date()
            try:
                df = pd.DataFrame(lines.y, lines.x)
                value = '{:0.2f}'.format(df.loc[p_date][0])
            except Exception as e:
                value = '-'

            highlighted_text.value = '''<div style="line-height:1.1;font-size:1.1em;color:ivory;padding:8px;">
                                        On the <font color="orange">{}</font> - value is <font color="orange">{}</font></div>
                                        '''.format(datetime.strftime(p_date, '%d-%b-%y'), value)

    index_sel.observe(index_change_callback, names=['selected'])

    fig = Figure(marks=[lines, signal_value, signal_value_2], 
                 axes=[ax_x, ax_y], title='', interaction=index_sel,
                 layout={'width': '100%', 'height': '300px'})

    return VBox([fig, highlighted_text], layout={'width': '100%', 'height': '330px',
                               'overflow_x':'hidden','overflow_y':'hidden'})

def add_signals(chart, stock, df_calls, price_df):
    ticker_px_series = price_df[price_df['ID']==stock].set_index('DATE').ffill()
    # update chart
    chart.children[0].marks[0].x = ticker_px_series.index
    chart.children[0].marks[0].y = ticker_px_series['px_last'].values
    # signal value
    signal_date = df_calls[(df_calls['ID']==stock)&(df_calls['action']=='buy')].set_index('DATE')
    chart.children[0].marks[1].x = signal_date.index
    chart.children[0].marks[1].y = signal_date['px_last'].values

    signal_date = df_calls[(df_calls['ID']==stock)&(df_calls['action']=='sell')].set_index('DATE')
    chart.children[0].marks[2].x = signal_date.index
    chart.children[0].marks[2].y = signal_date['px_last'].values
        

def get_buy_sell_by_stocks(df_calls, price_df, title='', top=15):
    '''
    df_calls: ID, DATE, px_last, action
    price_df: ID, DATe, px_last
    
    e.g. get_buy_sell_by_stocks(df_calls[['ID','DATE','px_last','action']], price_df)
    '''
    def update_chart_data(caller):
        s = ticker_selector.value
        add_signals(chart, s, df_calls, price_df)

    ticker_selector = Dropdown(options=sorted(df_calls[df_calls['action']!='na']['ID'].unique()))
    ticker_selector.observe(update_chart_data, 'value')

    app_title = HTML('<h2>{}</h2>'.format(title))
    chart = create_line_chart()

    return VBox([app_title, ticker_selector, 
              HBox([chart, HTML(df_calls[df_calls['action']!='hold'].head(top).style.render())])])


'''
box1, (mark_bar, mark_bar2) = bar_chart_hori(top_contributor.set_index('ID'), col=ret_col)

def on_bar_click(evt=None):
    if evt is not None and evt['new'] is not None:
        selected = [mark_bar.x[i] for i in evt['new']][0]
        title.value = '<h2>%s</h2>'%selected
        add_signals(chart, selected, signal_df, px_df)

def on_bar_click2(evt=None):
    if evt is not None and evt['new'] is not None:
        selected = [mark_bar2.x[i] for i in evt['new']][0]
        title.value = '<h2>%s</h2>'%selected
        add_signals(chart, selected, signal_df, px_df)

# Attach listeners
mark_bar.observe(on_bar_click, names='selected')
mark_bar2.observe(on_bar_click2, names='selected')

chart = create_line_chart()
title = HTML()
VBox([box1, title, chart], layout={'overflow_x': 'hidden'})

'''