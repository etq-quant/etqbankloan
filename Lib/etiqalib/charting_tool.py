from ipywidgets import VBox, HBox, Dropdown, HTML
from bqplot import LinearScale, DateScale, Lines, Scatter, Axis, Figure, Tooltip
from bqplot.interacts import IndexSelector
import ipydatagrid as ipdg
import ipywidgets as ipw
import bqplot as bqp
import pandas as pd

def create_line_chart1(plot_df, plot_cols:list, title=''):
    '''
    dataframe with date index
    '''
    # define the characteristics of the line chart (popup)
    colors=['#1B84ED', '#CF7DFF']
    sc_x, sc_y = DateScale(), LinearScale()
    ax_x, ax_y = Axis(scale=sc_x, tick_rotate=-45, tick_style={'text-anchor': 'end'}, label_location='middle', 
                              grid_lines='none', tick_format='%d-%b-%y', num_ticks=10), \
                    Axis(orientation='vertical', scale=sc_y, label_location='middle',side='left'),\

    lines = Lines(x=plot_df.index, y=[plot_df[c] for c in plot_cols],scales={'x': sc_x, 'y': sc_y}, \
                  labels=plot_cols, stroke_width=2)
    scatter_df = pd.melt(plot_df.reset_index(),id_vars=['DATE'],var_name='column')
    mark_scatter = Scatter(x=scatter_df['DATE'],y=scatter_df['value'],skew=scatter_df['column'],\
                           scales={'x': sc_x, 'y': sc_y},default_opacities=[0],
                           tooltip=Tooltip(fields=['skew','y', 'x'],formats=['', '.3', '%m/%d/%Y'],show_labels=False))
    marks = [lines,mark_scatter]
    axes = [ax_x, ax_y]
    
    fig = Figure(marks=marks, 
                 axes=axes, title=title,
                 layout={'width': '100%', 'height': '300px'})

    return VBox([fig], layout={'width': '100%', 'height': '330px',
                               'overflow_x':'hidden','overflow_y':'hidden'})

def create_line_chart(plot_df, plot_cols:list, title=''):
    '''
    dataframe with date index
    '''
    # define the characteristics of the line chart (popup)
    colors=['#1B84ED', '#CF7DFF']
    sc_x, sc_y, sc_y2 = DateScale(), LinearScale(), LinearScale()
    ax_x, ax_y, ax_y2 = Axis(scale=sc_x, tick_rotate=-45, tick_style={'text-anchor': 'end'}, label_location='middle', 
                              grid_lines='none', tick_format='%d-%b-%y', num_ticks=10), \
                         Axis(orientation='vertical', scale=sc_y, label_location='middle',side='left', label=plot_cols[0]),\
                         Axis(orientation='vertical', scale=sc_y2, label_location='middle', side='right', label=plot_cols[-1])

    lines = Lines(x=plot_df.index, y=plot_df[plot_cols[0]].values,scales={'x': sc_x, 'y': sc_y}, \
                  colors=[colors[0]], labels=[plot_cols[0]], stroke_width=2)
    mark_scatter = Scatter(x=plot_df.index,y=plot_df[plot_cols[0]].values,skew=[plot_cols[0]]*len(plot_df),\
                           scales={'x': sc_x, 'y': sc_y},default_opacities=[0],
                           tooltip=Tooltip(fields=['skew','y', 'x'],formats=['', '.3', '%m/%d/%Y'],show_labels=False))
    marks = [lines, mark_scatter]
    axes = [ax_x, ax_y]
    
    if len(plot_cols)==2:
        lines2 = Lines(x=plot_df.index, y=plot_df[plot_cols[1]].values,scales={'x': sc_x, 'y': sc_y2},\
                       colors=[colors[1]], labels=[plot_cols[1]], stroke_width=2)
        mark_scatter2 = Scatter(x=plot_df.index,y=plot_df[plot_cols[1]].values,skew=[plot_cols[1]]*len(plot_df), \
                                scales={'x': sc_x, 'y': sc_y2},default_opacities=[0],
                                tooltip=Tooltip(fields=['skew', 'y', 'x'],formats=['', '.3', '%m/%d/%Y'],show_labels=False))
        marks.extend([lines2,mark_scatter2])
        axes.append(ax_y2)

    fig = Figure(marks=marks, 
                 axes=axes, title=title,
                 layout={'width': '100%', 'height': '300px'})

    return VBox([fig], layout={'width': '100%', 'height': '330px',
                               'overflow_x':'hidden','overflow_y':'hidden'})

import plotly.figure_factory as ff
import plotly.graph_objects as go


def correlation_heatmap(df, title='', absolute=False):
    df_corr = df.corr() if not absolute else df.corr().applymap(lambda x: abs(x))
    z = df_corr.round(2).values

    x = df_corr.columns.tolist()

    fig = ff.create_annotated_heatmap(z, x=x, y=x, showscale = True)
    fig.update_xaxes(tickfont=dict(size=18))
    fig.update_yaxes(tickfont=dict(size=18))
    fig.update_layout(title='Correlation Heatmap'+title, font={'size':18})
    fig.show()
    
def correlation_heatmap_go(df, absolute=False, colorscale='Viridis', **kwargs):
    '''
    Parameters
    ----------
    df: DataFrame
        Dataframe formatted in wide-form
    
    Returns
    -------
    fig: go.Figure
        Plotly Heatmap
        
    Examples
    --------
    >>> df
               DATE     KO1     KO3   TSH       TAH  HAPL       SOP  IJMP
    3    2016-01-04  2290.0  2436.0  1.99  4.158333  2.39  4.179079  3.55
    4    2016-01-05  2275.0  2454.0  2.00  4.466667  2.40  4.179079  3.62
    5    2016-01-06  2268.0  2448.0  2.04  4.525000  2.43  4.179079  3.66
    ...         ...     ...     ...   ...       ...   ...       ...   ...
    1826 2020-12-31  3891.0  3600.0  1.15  3.030000  1.80  4.000000  1.82
    1830 2021-01-04  3950.0  3724.0  1.15  2.960000  1.81  4.000000  1.84
    1831 2021-01-05  3966.0  3755.0  1.14  3.000000  1.81  4.000000  1.87
    
    >>> fig = correlation_heatmap_go(df, title='Correlation since 2016')
    
    '''
    df_corr = df.corr() if not absolute else df.corr().applymap(lambda x: abs(x))
    z = df_corr.round(2).values
    x = df_corr.columns.tolist()
    
    fig = go.Figure(data=go.Heatmap(
            z=z,
            x=x,
            y=x,
            colorscale=colorscale))

    fig.update_xaxes(tickfont=dict(size=18))
    fig.update_yaxes(tickfont=dict(size=18))
    fig.update_layout(font={'size':18}, **kwargs)
    fig.show()
    
def line_chart_by(df, x_col, y_col, group_col, **kwargs):
    '''
    Parameters
    ----------
    
    
    Returns
    -------
    
    '''
    fig=go.Figure()
    
    for i in sorted(df[group_col].unique()):
        plot_df = df[df[group_col]==i]
        fig.add_trace(go.Scatter(x=plot_df[x_col], y=plot_df[y_col], name=i))
    
    fig.update_layout(**kwargs)
    return fig