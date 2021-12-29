"""
(c) 2013 Tsung-Han Yang
This source code is released under the Apache license.  
Created on April 1, 2013
"""
import datetime as dt
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates 
import finpy.utils.fpdateutil as du
import finpy.data.dataaccess as da
import finpy.utils.utils as ut
from .fincommon import FinCommon

def get_tickdata(ls_symbols, ldt_timestamps, csv_col = [], fill=True, df=pd.DataFrame):
    """
        To get all price data of all tickers in ls_symbols within the list of ldt_timestamps
        :param ls_symbols: A list with all tickers
        :param ldt_timestamps: A list with all trading days within the time frame.
        :param fill: Whether to fill invalid data. Default is True.
        :param source: "Yahoo" or "Google"
    """
    c_dataobj = da.DataAccess("Yahoo", cachestalltime=0)
    if csv_col:
        ls_keys = csv_col
    else:    
        ls_keys = ['open', 'high', 'low', 'actual_close', 'close', 'volume']
    ldf_data = c_dataobj.get_data(ldt_timestamps, ls_symbols, ls_keys)
    d_data = dict(list(zip(ls_symbols, ldf_data)))
    if fill == True:
        for s_key in ls_symbols:
            d_data[s_key] = d_data[s_key].fillna(method = 'ffill')
            d_data[s_key] = d_data[s_key].fillna(method = 'bfill')
            d_data[s_key] = d_data[s_key].fillna(1.0)
    stocks = dict()
    for s in ls_symbols:
        stocks[s] = df(index=ldt_timestamps, data=d_data[s])
        stocks[s]['shares'] = np.nan
        stocks[s].loc[ldt_timestamps[0],'shares'] = 0
    return stocks 
