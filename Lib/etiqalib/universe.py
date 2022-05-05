# libraries for visuals
from ipywidgets import Dropdown, Textarea, VBox, HBox, IntSlider, HTML, FloatRangeSlider, SelectMultiple, Checkbox
from bqwidgets import TickerAutoComplete
# standard python libraries
from collections import OrderedDict
# Bloomberg and custom libraries
import bqport
from . import univ_config as config

caption = '<h2>{h}</h2><div style="line-height:1.2;font-size:1em;color:ivory;padding:8px;font-style:italic;">{t}</div>'

class UniversePicker:   
    def __init__(self, layout=None):
        layout = {'layout': layout} if layout else {}
        # load the configuration from config file
        self.cfg = config.universe_config
        # load all portfolios from PRTU
        self.all_portfolios = sorted([(p['name'], p['id']) for p in bqport.list_portfolios()])
        # create a dictionary of widgets to ease display
        self.widgets = dict()
        try:
            # check if valid config.py file
            if len(self.cfg) < 0:
                raise RuntimeError('Check the App configuration in `config.py` file')
            
            # build the UI box
            obj_lst = self.build_ui()
            self.box = VBox(obj_lst, **layout)

        except Exception as e:
            self.box = VBox([HTML(caption.format(h='Error in loading component',t=e))], **layout)
      
    
    def show(self):
        return self.box

    def get_universe(self):
        """
        Retrieve user inputs from universe selection 
        under the form of a dictionary. 
        """
        univ = self.widgets['univ_type'].value
        value = self.widgets['univ_value'].children[0]
        if univ == 'Index':  element = value.value.split(':')[0]
        if univ == 'Portfolio': element = value.value
        if univ == 'List': element = value.value.split('\n')
        if univ == 'Screener':
            element = dict()
            for k,v in self.w.items():
                if v.children[3].value:
                    oper = v.children[1].value
                    value = v.children[2].value
                    value = list(value) if type(value) == tuple else value.split(',') \
                                        if type(value) == str else value
                    # check if selected value is EQS or a portfolio
                    if k == 'Universe':
                        if value[0].startswith('**'): value = 'EQS'
                        else:
                            value = '#N/A Invalid Portfolio name' if value[0] not in [x[0] for x in self.all_portfolios] else value[0]
                            
                    element[k] = (oper, value)
            
        return {'type':univ, 'value':element}

    def build_ui(self):
        """
        Widgets for picking a universe.
        """
        # create a label for universe selection
        self.widgets['label'] = HTML(caption.format(h='Universe selection', t='''Toggle the universe type 
                                                    and adjust the value of universe to run analysis on.'''))
        # dropdown universe type selector
        univ_type_options = [x for x in self.cfg.keys() if self.cfg[x]['active']]
        univ_type_default = [x for x in self.cfg.keys() if self.cfg[x]['startup']][0]
        self.widgets['univ_type'] = Dropdown(options=univ_type_options, value=univ_type_default , layout={'width':'150px'})
        self.widgets['univ_type'].observe(self._on_univ_type_change, 'value')
        
        # objects for the universe selectors
        widget_layout = {'min_width':'260px'}
        self.widgets['univ_value'] = VBox(layout=widget_layout)

        # Call the event handler to show the default widget.
        self._on_univ_type_change()
        
        # final UI for universe picker
        output = VBox([self.widgets['label'], 
                      HBox([self.widgets['univ_type'], self.widgets['univ_value']])])
        return [output]

    def _on_univ_type_change(self, *args, **kwargs):
        # Show different widgets according to the universe type user selected.
        univ = self.widgets['univ_type'].value
        # fetch the associated configuration
        cfg_ = self.cfg[univ]
        
        # assign the relevant widget output to univ_value
        if univ == 'Index':
            element = TickerAutoComplete(value=cfg_['value'], yellow_keys=['Index'])

        elif univ == 'Portfolio':
            custom_port = self.all_portfolios[0][1]
            try:
                custom_port = [x[1] for x in self.all_portfolios if cfg_['value'] == x[0]][0]
            except:
                pass
            element = Dropdown(options=self.all_portfolios, value=custom_port)

        elif univ == 'Screener':
            element = self.build_screener_ui(cfg_['value'])
            
        elif univ == 'List':
            element = Textarea(value=cfg_['value'], rows=4, placeholder='Place one ticker per line')
            
        self.widgets['univ_value'].children = [element]
    
    def build_screener_ui(self, filters_definition):
        s_ = {'description_width':'initial'}
        sw_ = {'width':'50px'}
        lw_ = {'width':'200px'}
        self.w = OrderedDict()
        
        try:
            for f,v in filters_definition.items():
                if v['type'] == 'slider':
                    self.w[f] = HBox([ HTML(value=v['description'], layout=lw_), 
                                       HTML(value=v['oper'], layout=sw_), 
                                       IntSlider(value=v['value'],min=v['min'], max=v['max'], step=v['step'], style=s_),
                                       Checkbox(value=v['active'], indent=False, layout=sw_) ])
                if v['type'] == 'double-slider':
                    self.w[f] = HBox([ HTML(value=v['description'], layout=lw_), 
                                       HTML(value=v['oper'], layout=sw_), 
                                       FloatRangeSlider(value=v['value'],min=v['min'], max=v['max'], step=v['step'], 
                                                        readout_format='.2f', style=s_),
                                       Checkbox(value=v['active'], indent=False, layout=sw_) ])
                elif v['type'] == 'dropdown':
                    self.w[f] = HBox([ HTML(value=v['description'], layout=lw_), 
                                       HTML(value=v['oper'], layout=sw_), 
                                       Dropdown(options=v['options'], value=v['value'], style=s_),
                                       Checkbox(value=v['active'], indent=False, layout=sw_) ])
                elif v['type'] == 'textarea':
                    self.w[f] = HBox([ HTML(value=v['description'], layout=lw_), 
                                       HTML(value=v['oper'], layout=sw_), 
                                       Textarea(value=v['value'], style=s_),
                                       Checkbox(value=v['active'], indent=False, layout=sw_) ])
                elif v['type'] == 'multi-select':
                    self.w[f] = HBox([ HTML(value=v['description'], layout=lw_), 
                                       HTML(value=v['oper'], layout=sw_), 
                                       SelectMultiple(options=v['options'], value=v['value'], style=s_),
                                       Checkbox(value=v['active'], indent=False, layout=sw_) ])
                    
            # render the final UI for screening
            filters_ui = [w for w in self.w.values()]
        except Exception as e:
            txt = caption.format(h='',t='Check the App configuration in `config.py` file<br>More info: {} on {}'.format(e, f))
            raise RuntimeError(txt)
            
        return VBox(filters_ui)