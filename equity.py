import datetime as dt
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates 

class Equity(pd.DataFrame):
    """
    Equity is something that will be/was/is in the Portfolio.
    buy is either a NaN, or a floating number. 
    If buy is a floating number, then we buy the number of shares of the equity.
    sell is either a NaN, or a floating number.
    shares is the daily balance of the equities.
    """
    def __init__(self, data=None, index=None, columns=None, dtype=None, copy=False, init_share=0.0):
        if columns == None:
            cols = ['open', 'high', 'low', 'close', 'volume', 'actual_close', 'nml_price', 'buy',
            'sell', 'shares']
        else:
            cols = columns
        pd.DataFrame.__init__(self, data=data, index=index, columns=cols, dtype=dtype, copy=copy)
        self['shares'][0] = init_share
        self['buy'] = 0.0
        self['sell'] = 0.0

    def buy(self, date, shares, price, ldt_timestamps):
        self.fillna_shares(date, ldt_timestamps)
        self['buy'][date] = shares
        self['shares'][date] += shares

    def sell(self, date, shares, price, ldt_timestamps):
        self.fillna_shares(date, ldt_timestamps)
        self['sell'][date] = shares
        self['shares'][date] -= shares
        return price*shares

    def fillna_shares(self, date, ldt_timestamps):
        last_valid = self['shares'].last_valid_index()
        self['shares'][last_valid:date] = self['shares'][last_valid]

    def nml_price(self):
        self['nml_price'] = self['close']/self['close'].ix[0]

    def plot(self, ax, ldt_timestamps, column):
        ax.plot(ldt_timestamps, self[column])

    def dailyrtn(self):
        """
        Return the return of each day.
        """
        daily_rtn = []
        ldt_timestamps = self['close'].index
        for date in range(len(ldt_timestamps)):
            if date == 0:
                daily_rtn.append(0)
            else:
             daily_rtn.append((self['close'][date]/self['close'][date-1])-1)
        return daily_rtn

    def avg_dailyrtn(self): 
        return np.average(self.dailyrtn())

    def std(self):
        return np.std(self.dailyrtn())

    def sharpe(self, k=252):
        return np.sqrt(k) * self.avg_dailyrtn()/self.std()

    def totalrtn(self):
        return self['close'][-1]/self['close'][0]

