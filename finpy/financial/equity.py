"""
(c) 2013 Tsung-Han Yang
This source code is released under the Apache license.  
Created on April 1, 2013
"""
import datetime as dt
import pandas as pd
import numpy as np
import finpy.utils.fpdateutil as du
import finpy.data.dataaccess as da
import finpy.utils.utils as ut
from .fincommon import FinCommon
from finpy.edgar.company import company
import os
import sqlite3

def get_tickdata(ls_symbols, ldt_timestamps, csv_col = [], fill=True, df=pd.DataFrame, actions=True, concepts=[]):
    """
        To get all price data of all tickers in ls_symbols within the list of ldt_timestamps
        :param ls_symbols: A list with all tickers
        :param ldt_timestamps: A list with all trading days within the time frame.
        :param fill: Whether to fill invalid data. Default is True.
    """
    c_dataobj = da.DataAccess("Yahoo", cachestalltime=0)
    if csv_col:
        ls_keys = csv_col
    else:    
        ls_keys = ['open', 'high', 'low', 'actual_close', 'close', 'volume']
    ldf_data = c_dataobj.get_data(ldt_timestamps, ls_symbols, ls_keys, actions)
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
    if len(concepts) != 0:
        company_ticker_json_db = os.path.join(os.path.join(os.environ['FINPYDATA'], "edgar", "files", "company_tickers.db"))
        try:
            conn = sqlite3.connect(company_ticker_json_db)
        except:
            print("Please run downlaod_edgar.py to create company db")
        for s in ls_symbols:
            c = company("NVDA", conn)
            facts =  c.get_concepts(concepts)

    return stocks 
