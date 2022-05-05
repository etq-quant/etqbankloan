def reorder(df, col_order:list):
    return df[col_order]

def rename(df, in_col:str, out_col:str):
    return df.rename(columns={in_col:out_col})

def map_col_with(df, in_col:str, out_col:str, rename_dict:dict):
    df[out_col] = df[in_col].map(rename_dict)
    return df

def drop_cols(df, cols:list):
    return df.drop([c for c in cols if c in df.columns],1, errors='ignore')

def get_quarter(x):
    return (x.month-1)//3+1
    
def get_prev_quarter(date, quarter_step=4):
    '''
    step = 4
    In: 2019-09-30
    Out: 2018-12-31
    '''
    year = int(date.split('-')[0])
    tail = date.replace('{}'.format(year),'')
    quarters = ['-03-31','-06-30','-09-30','-12-31']
    quarter_trail_12M = quarters.index(tail)-quarter_step
    if quarter_trail_12M < 0:
        year = year -1
    return str(year)+quarters[quarter_trail_12M]

def multidivision(df, cols:list, divcol:str):
    df[cols] = df[cols].div(df[divcol], axis=0) 
    return df

def percent_change(df, cols:list, shift=1, suffix='_chg'):
    outcols = [c + suffix for c in cols] 
    df[outcols] = (df[cols]/df[cols].shift(shift) - 1)*100
    return df

def merge_multiple_dfs(df_list:list, on:list, how='left'):
    from functools import reduce
    merged_df = reduce(lambda x,y: x.merge(y, on=on, how=how), df_list)
    return merged_df