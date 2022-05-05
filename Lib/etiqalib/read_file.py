import pandas as pd
import functools
import re

def read_labelled_data(file='Labelling(FBM100).xlsx'):
    label_df = pd.read_excel(file)
    label_df['Sector'] = label_df.apply(lambda x: x['Name'] if str(x['Ticker'])=='nan' else None,1)
    label_df['Sector'] = label_df['Sector'].ffill()
    label_df = label_df.dropna().copy().rename(columns={'Ticker':'ID'})
    label_df['Sector'] = label_df['Sector'].map(lambda x:  re.sub('\(.+\)','',x).strip())
    return label_df

def read_index_members(index_df_file):
    '''
    Parameters
    ----------
    index_df_file: dict 
        The index name and path to the index members excel
    
    Returns
    -------
    index_df: DataFrame
        Index members with weights and ID
    
    Examples
    --------
    >>> index_df =  read_index_members({
    ...             "FBM100":"../QoQ reporting/members/FBM100 as of Apr 01 20211.xlsx",
    ...             "FBMS":"../QoQ reporting/members/FBMS as of Apr 01 20211.xlsx"
    ...             })
    
    >>>                ID                      Name    Weight        Shares       Price   Index  
         0       PBK MK Equity           Public Bank Bhd  9.690017  15462.557036   4.290  FBM100  
         1       MAY MK Equity       Malayan Banking Bhd  7.281666   5991.306506   8.320  FBM100  
    '''
    
    index_df_list = [pd.read_excel(file).assign(Index=idx_name).rename(columns={'Ticker':'ID'}) \
                         for idx_name, file in index_df_file.items()] 
    index_df = functools.reduce( lambda x,y : x.append(y), index_df_list)
    return index_df
    
def read_ssl_members(file_path = '../QoQ reporting/members/SSL_20210324_real.xls'):
    '''
    Parameters
    ----------
    file_path: str
        Path to SSL excel.
    
    Returns
    -------
    ssl_df : DataFrame
        List of stocks in SSL
    '''
    ssl_df = pd.read_excel(file_path, sheet_name='Source', header=None).rename(columns={1:'ID'})
    ssl_df = ssl_df[ssl_df['ID'].map(lambda x: ' MK ' in str(x))].copy()
    return ssl_df
