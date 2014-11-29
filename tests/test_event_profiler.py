import unittest
import pandas as pd
import numpy as np
import math
import copy
import datetime as dt
import finpy.fpdateutil as du
import finpy.eventprofiler as ep
import finpy.dataaccess as da
from finpy.equity import get_tickdata
from finpy.equity import Equity
class TestPortfolioFunctions(unittest.TestCase):
    def setUp(self):
        dt_start = dt.datetime(2008, 1, 1)
        dt_end = dt.datetime(2009, 12, 31)
        ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt.timedelta(hours=16))
        
        dataobj = da.DataAccess('Yahoo', cachestalltime=0)
        year = '2012'
        stock_list = 'sp500' + year
        ls_symbols = dataobj.get_symbols_from_list(stock_list)
        ls_symbols.append('SPY')
        ls_keys = ['open', 'high', 'low', 'close', 'volume', 'actual_close']
        all_stocks = get_tickdata(ls_symbols=ls_symbols, ldt_timestamps=ldt_timestamps)
        df_events = self.find_events(ls_symbols, all_stocks)
        self.event_no = ep.eventprofiler(df_events, all_stocks, i_lookback=20, i_lookforward=20,
                    b_market_neutral=True, b_errorbars=True,
                    s_market_sym='SPY')

    def testEvent(self):
        print("test the number of events ...")
        self.assertEqual(self.event_no, 176)
        
    def find_events(self, ls_symbols, all_stocks):
        ''' Finding the event dataframe '''
        df_tmp = {}
        for x in all_stocks:
            df_tmp[x] = all_stocks[x]['actual_close']
        df_close = pd.DataFrame(df_tmp)
        ts_market = all_stocks['SPY']['actual_close']
        print("Finding Events")
        # Creating an empty dataframe
        df_events = copy.deepcopy(df_close)
        df_events = df_events * np.NAN
        # Time stamps for the event range
        ldt_timestamps = df_close.index
        for s_sym in ls_symbols:
            for i in range(1, len(ldt_timestamps)):
                # Calculating the returns for this timestamp
                f_symprice_today = df_close[s_sym].ix[ldt_timestamps[i]]
                f_symprice_yest = df_close[s_sym].ix[ldt_timestamps[i - 1]]
                f_marketprice_today = ts_market.ix[ldt_timestamps[i]]
                f_marketprice_yest = ts_market.ix[ldt_timestamps[i - 1]]
                f_symreturn_today = (f_symprice_today / f_symprice_yest) - 1
                f_marketreturn_today = (f_marketprice_today / f_marketprice_yest) - 1
                # Event is found if the symbol is smaller than 5.0 today and 
                # greater or equal to 5.0 yesterday.
                if (f_symprice_today < 5.0 and f_symprice_yest >= 5.0):
                    print(s_sym, i, ldt_timestamps[i], f_symprice_yest, f_symprice_today)
                    df_events[s_sym].ix[ldt_timestamps[i]] = 1
        return df_events
if __name__ == '__main__':
    unittest.main()
