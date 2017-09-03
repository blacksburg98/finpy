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

def get_tickdata(ls_symbols, ldt_timestamps, fill=True, df=pd.DataFrame, source="Yahoo"):
    """
        To get all price data of all tickers in ls_symbols within the list of ldt_timestamps
        :param ls_symbols: A list with all tickers
        :param ldt_timestamps: A list with all trading days within the time frame.
        :param fill: Whether to fill invalid data. Default is True.
        :param source: "Yahoo" or "Google"
    """
    c_dataobj = da.DataAccess(source, cachestalltime=0)
    if source == "Yahoo":
        ls_keys = ['open', 'high', 'low', 'actual_close', 'volume', 'close']
    elif source == "Google":
        ls_keys = ['open', 'high', 'low', 'close', 'volume']
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
class Equity(pd.DataFrame, FinCommon):
    """
    Equity is something that will be/was/is in the Portfolio.
    buy is either a NaN, or a floating number. 
    If buy is a floating number, then we buy the number of shares of the equity.
    sell is either a NaN, or a floating number.
    shares is the daily balance of the equities.
    This class is deprecated because of performance degrading.
    """
    def __init__(self, data=None, index=None, columns=None, dtype=None, copy=False, init_share=0.0):
        if columns == None:
            cols = ['open', 'high', 'low', 'close', 'volume', 'actual_close', 'buy',
            'sell', 'shares']
        else:
            cols = columns
        pd.DataFrame.__init__(self, data=data, index=index, columns=cols, dtype=dtype, copy=copy)
        self['shares'][0] = init_share
        self['buy'] = 0.0
        self['sell'] = 0.0

    def buy(self, date, shares, price):
        """
        Buy stocks.
        """
        self.fillna_shares(date)
        self.loc[date, 'buy'] = shares
        self.loc[date, 'shares'] += shares

    def sell(self, date, shares, price):
        self.fillna_shares(date)
        self.loc[date, 'sell'] = shares
        self.loc[date, 'shares'] -= shares
        return price*shares

    def fillna_shares(self, date):
        last_valid = self['shares'].last_valid_index()
        self.loc[last_valid:date, 'shares'] = self.loc[last_valid, 'shares']

    def daily_return(self, data=None):
        """
        Return the return of each day, a list.
        """
        if type(data) == type(None):
            daily_close = self['close']
            ldt_timestamps = self.ldt_timestamps()
        else:
            daily_close = data
            ldt_timestamps = data.index
        daily_rtn = []
        for date in range(len(ldt_timestamps)):
            if date == 0:
                daily_rtn.append(0)
            else:
                daily_rtn.append((daily_close[date]/daily_close[date-1])-1)
        return np.array(daily_rtn)

    def std(self):
        " Standard Deviation of the daily_return "
        return np.std(self.daily_return())

    def normalized(self):
        return self['close']/self['close'].ix[0]

    def normalized_all(self):
        if 'actual_close' in self:
            self['open'] = self['open'] * self['close']/self['actual_close']
            self['high'] = self['high'] * self['close']/self['actual_close']
            self['low'] = self['low'] * self['close']/self['actual_close']
    def sortino(self, k=252):
        """
        Return Sortino Ratio. 
        You can overwirte the coefficient with k.
        The default is 252.
        """
        daily_rtn = self.daily_return()
        negative_daily_rtn = daily_rtn[daily_rtn < 0]
        sortino_dev = np.std( negative_daily_rtn)
        sortino = (self.avg_daily_return() / sortino_dev) * np.sqrt(k)
        return sortino

    def total_return(self):
        return self['close'][-1]/self['close'][0]

    def moving_average(self, tick, window=20):
        """
        Return an array of moving average. Window specified how many days in
        a window.
        """
        mi = self.bollinger_band(tick=tick, window=window, mi_only=True)
        return mi

    def bollinger_band(self, tick, window=20, k=2, mi_only=False):
        """
        Return four arrays for Bollinger Band.
        The first one is the moving average.
        The second one is the upper band.
        The thrid one is the lower band.
        The fourth one is the Bollinger value.
        If mi_only, then return the moving average only.
        """
        ldt_timestamps = self.ldt_timestamps()
        pre_timestamps = ut.pre_timestamps(ldt_timestamps, window)
        # ldf_data has the data prior to our current interest.
        # This is used to calculate moving average for the first window.
        ldf_data = get_tickdata([tick], pre_timestamps)
        merged_data = pd.concat([ldf_data[tick]['close'], self['close']])
        bo = dict()
        bo['mi'] = pd.rolling_mean(merged_data, window=window)[ldt_timestamps] 
        if mi_only:
            return bo['mi']
        else:
            sigma = pd.rolling_std(merged_data, window=window)
            bo['hi'] = bo['mi'] + k * sigma[ldt_timestamps] 
            bo['lo'] = bo['mi'] - k * sigma[ldt_timestamps] 
            bo['ba'] = (merged_data[ldt_timestamps] - bo['mi']) / (k * sigma[ldt_timestamps])
            return bo

    def drawdown(self, tick, window=10):
        """
        Find the peak within the retrospective window.
        Drawdown is the difference between the peak and the current value.
        """
        ldt_timestamps = self.index
        pre_timestamps = ut.pre_timestamps(ldt_timestamps, window)
        # ldf_data has the data prior to our current interest.
        # This is used to calculate moving average for the first window.
        ldf_data = get_tickdata([tick], pre_timestamps)
        merged_data = pd.concat([ldf_data[tick]['close'], self['close']])
        total_timestamps = merged_data.index
        dd = pd.Series(index=ldt_timestamps)
        j = 0
        for i in range(len(pre_timestamps), len(total_timestamps)):
            win_start = total_timestamps[i - window]
            win_end = total_timestamps[i]
            ts_value = merged_data[win_start:win_end]
            current = merged_data[win_end]
            peak = np.amax(ts_value)
            dd[j] = (peak-current)/peak
            j += 1
        return dd

    def return_ratio(self):
        return (self['close'][-1]/self['close'][0])

    def ldt_timestamps(self):
        """
        Return an array of datetime objects.
        """
        ldt_index = self.index
        dt_start = ldt_index[0] 
        dt_end = ldt_index[-1] 
        dt_timeofday = dt.timedelta(hours=16)
        ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt_timeofday)
        return ldt_timestamps

    def up_ratio(self, date, days=10):
        """
        Return the ratio of the past up days.
        """
        ldt_index = self.index
        last = date
        first = date-days
        up = 0.0
        dn = 0.0
        for i in range(first, last+1):
            if self['close'][i] < self['close'][i-1]:
                dn += 1
            else:
                up += 1
        ratio = up / (dn + up)
        return ratio

    def dn_ratio(self, date, days=10):
        """
        Return the ratio of the past down days. 
        """
        ratio = 1.0 - self.up_ratio(date, days)
        return ratio

    def rolling_normalized_stdev(self, tick, window=50):
        """
        Return the rolling standard deviation of normalized price
        """
        ldt_timestamps = self.ldt_timestamps()
        pre_timestamps = ut.pre_timestamps(ldt_timestamps, window)
        # ldf_data has the data prior to our current interest.
        # This is used to calculate moving average for the first window.
        ldf_data = get_tickdata([tick], pre_timestamps)
        merged_data = pd.concat([ldf_data[tick]['close'], self['close']])
        all_timestamps = pre_timestamps + ldt_timestamps
        merged_daily_rtn = pd.Series(self.daily_return(merged_data), index=all_timestamps) 
        sigma = pd.rolling_std(merged_daily_rtn, window=window)
        return sigma[self.index]

    def max_rise(self, tick, date, window=20):
        """
        Find the maximum change percentage between the current date and the bottom of the retrospective window.
        """
        ldt_timestamps = self.index
        pre_timestamps = ut.pre_timestamps(ldt_timestamps, window)
        first = pre_timestamps[0]
        # ldf_data has the data prior to our current interest.
        # This is used to calculate moving average for the first window.
        try:
            self['close'][first]
            merged_data = self['close']
        except:
            ldf_data = get_tickdata([tick], pre_timestamps)
            merged_data = pd.concat([ldf_data[tick]['close'], self['close']])
        if(isinstance(date , int)):
            int_date = ldt_timestamps[date]
        else:
            int_date = date
        c = merged_data.index.get_loc(int_date)
        m = merged_data[c-window:c].min()
        r = (merged_data[c]-m)/m
        return r

#    def RSI(self):
#        """
#        Relative Strength Index
#        http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:relative_strength_index_rsi
#        """
#        retrun False
