from datetime import datetime, timedelta
import pandas as pd
import numpy as np
try:
    import bql
except:
    ""

def get_data(bq, security, fields):
    request =  bql.Request(security, fields)
    response = bq.execute(request)
    df = bql.combined_df(response)
    return df

def download_incremental(bq, ticker, func, start_date, end_date, steps=5, **kwargs):
    start_year = int(start_date.split('-')[0])
    end_year = int(end_date.split('-')[0])
    
    steps = min(end_year-start_year, steps)
    if start_year != end_year:
        years = list(range(start_year, end_year+1, steps))  
        df_list = []
        for y in range(len(years)-1):
            if years[y+1] != years[-1]:
                end = '{}-12-31'.format(years[y+1]-1)
            else:
                end = end_date

            if years[y] == years[0]:
                start=start_date
            else:
                start = '{}-01-01'.format(years[y])
            df = func(bq, ticker, start, end, **kwargs)
            df_list.append(df)

        return pd.concat(df_list)
    else:
        return func(bq, ticker, start_date, end_date, **kwargs)
        
def get_dt_range(bq, start, end):
    return bq.func.range(start=start, end=end)