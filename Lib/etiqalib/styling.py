from datetime import datetime, timedelta

def formatdate(df, col:str):
    df[col] = df[col].map(lambda x: x.date().isoformat())
    return df

def formatdate_multicol(df, cols:list):
    df[cols] = df[cols].applymap(lambda x: x.date().isoformat())
    return df    

def formatdecimal(df, col:str, decimalno=2):
    formatstr = '{:.%df}'%decimalno
    df[col] = df[col].map(lambda x: formatstr.format(x))
    return df        

def formatdecimal_multicol(df, cols:list, decimalno=2):
    formatstr = '{:.%df}'%decimalno
    df[cols] = df[cols].applymap(lambda x: formatstr.format(x))
    return df        

def round_number(df, col:str, roundto=2):
    df[col] = df[col].map(lambda x: round(x, roundto))
    return df

def round_number_multicols(df, cols:list, roundto=2):
    df[cols] = df[cols].applymap(lambda x: round(x, roundto))
    return df

def formatcomma(df, col:str):
    formatstr = '{:,}'
    df[col] = df[col].map(lambda x: formatstr.format(x))
    return df   

def formatcomma_multicol(df, cols:list):
    formatstr = '{:,}'
    df[cols] = df[cols].applymap(lambda x: formatstr.format(x))
    return df   

def formatpercent(df, col:str):
    formatstr = '{}%'
    df[col] = df[col].map(lambda x: formatstr.format(round(x*100,2)))
    return df

def customformat(df, col:str, formatstr:str):
    df[col] = df[col].map(lambda x: formatstr.format(x))
    return df

def customformat_multicol(df, cols:list, formatstr:str):
    df[cols] = df[cols].applymap(lambda x: formatstr.format(x))
    return df