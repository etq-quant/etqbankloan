from collections import OrderedDict

universe_config = {
    'Index': {
        'active':True,
        'startup':True,
        'value':'FBMKLCI Index',
    },
    'Portfolio': {
        'active':True,
        'startup':False,
        'value':'EQ_WATCHLIST',
    },
    'Screener': {
        'active':True,
        'startup':False,
        'value': OrderedDict([
            ('Universe', {
                'type':'dropdown',
                'description':'Screen from',
                'oper':'in',
                'value':'**All Active Securities',
                'options':['**All Active Securities'],
                'active': True,
            }), # end criteria
            ('Exchange code', {
                'type':'textarea',
                'description':'List of Exchange codes',
                'oper':'in',
                'value':'AU,NZ,SP,IN,MK,TB,ID,TT,KS,HK,PM,CH',
                'active': True,
            }), # end criteria
            ('Sector', {
                'type':'multi-select',
                'description':'Sectors to include',
                'oper':'in',
                'value':['Communications','Consumer Discretionary','Energy'],
                'options':['Communications','Consumer Discretionary','Consumer Staples','Energy','Financials','Health Care','Industrials','Materials','Technology','Utilities'],
                'active': False,
            }), # end criteria
#             ('Current / 5Y Avg ROE', {
#                 'type':'slider',
#                 'description':'Current / 5Y Avg ROE',
#                 'oper':'>',
#                 'value':-2,
#                 'min':-10,
#                 'max':10,
#                 'step':1,
#                 'active': False,
#             }), # end criteria
#             ('PE Ratio', {
#                 'type':'double-slider',
#                 'description':'Price/Earnings',
#                 'oper':'between',
#                 'value':(5,18),
#                 'min':0,
#                 'max':50,
#                 'step':1,
#                 'active': False,
#             }), # end criteria
        ]), # end screener section
    },
    'List': {
        'active':True,
        'startup':False,
        'value':'AAPL US Equity\nIBM US Equity\nMSFT US Equity',
    },
}