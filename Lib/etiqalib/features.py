import numpy as np
import math

def _degree_to_y(degree, x=20):
    """
    """
    return math.tan(math.radians(degree))*x/100

def _y_to_degree(radian, x=20):
    """
    radian: price change percentage
    x: duration, default 20 days
    get_y(radian_to_degree(0.05))
    = 0.05
    """
    return np.rad2deg(math.atan(radian*100/x))

def get_gradient(df, price_col='px_last'):
    df['price_degree1'] = df[price_col].pct_change().map(lambda x: _y_to_degree(x, 1))
    df['price_degree4'] = df[price_col].pct_change(4).map(lambda x: _y_to_degree(x, 4))
    df['price_degree10'] = df[price_col].pct_change(10).map(lambda x: _y_to_degree(x, 10))
    df['price_degree20'] = df[price_col].pct_change(20).map(lambda x: _y_to_degree(x, 20))
    df['price_degree60'] = df[price_col].pct_change(60).map(lambda x: _y_to_degree(x, 60))
    return df

def add_avat(df, volume_col='volume',rolling=20):
    df['volume'] = df['volume'].fillna(0)
    df['ma_volume_%d'%rolling] = df.groupby(['ID'])['volume'].rolling(rolling).mean().reset_index().set_index('level_1').drop('ID', axis=1)
    df['avat'] = df['volume']/df['ma_volume_%d'%rolling]    
    return df
    
def add_accel(df, price_col='px_last'):
    df['price_velocity'] = df.groupby(['ID'])[price_col].diff()
    df['price_acceleration'] = df.groupby(['ID'])['price_velocity'].diff()
    return df

def add_year(df, date_col='DATE'):
    df['year'] = df[date_col].map(lambda x: x.year)    
    return df

def add_quarter(df, date_col='PERIOD_END_DATE', with_year=True):
    if df[date_col].dtypes.name == 'object':
        date_col = pd.to_datetime(df[date_col])
    else:
        date_col = df[date_col]
        
    if with_year:
        df['quarter'] = date_col.map(lambda x: '{}Q{}'.format(x.year, (x.month-1)//3+1))   
    else:
        df['quarter'] = date_col.map(lambda x: 'Q{}'.format((x.month-1)//3+1))   
    return df

def add_year_month(df, date_col='DATE'):
    df['year_month'] = df[date_col].map(lambda x: '{}-{:02}'.format(x.year, x.month))
    return df

def add_halfyear(df, date_col='PERIOD_END_DATE'):
    df['1H'] = df[date_col].map(lambda x: (x.year,(x.month-1)//3+1)).map(lambda x: '{} 1/2Q'.format(x[0]) if x[1] <= 2 else '{} 3/4Q'.format(x[0]))
    return df