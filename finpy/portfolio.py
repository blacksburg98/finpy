"""
(c) 2013 Tsung-Han Yang
This source code is released under the Apache license.  
blacksburg98@yahoo.com
Created on April 1, 2013
"""
import datetime as dt
import pandas as pd
import numpy as np
import random 
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates 
import csv
from .order import Order
from .fincommon import FinCommon
import finpy.fpdateutil as du
from . import utils as ut

class Portfolio(FinCommon):
    def __init__(self, equities, cash, dates, order_list=None):
        """
        Portfolio has three items.
        equities is a dictionay of Equity instances. 
        Reference by ticker. self.equities['AAPL']
        cash is a pandas series with daily cash balance.
        total is the daily balance.
        order_list is a list of Order
        """
        self.equities = pd.Panel(equities)
        if order_list == None:
            self.order = []
        else:
            ol = order_list
            ol.sort(key=lambda x: x.date)
            self.order = ol
            for x in [x for x in order_list if x.price == None]:
                x.price = self.equities[x.tick]['close'][x.date]
        self.cash = pd.Series(index=dates)
        self.cash[0] = cash
        self.total = pd.Series(index=dates)
        self.total[0] = self.dailysum(dates[0])

    def dailysum(self, date):
        " Calculate the total balance of the date."
        equities_total = np.nansum(self.equities.loc[:,date,'shares'] * self.equities.loc[:,date,'close'])
        total = equities_total + self.cash[date]
        return total

    def buy(self, shares, tick, price, date, update_ol=False):
        """
        Portfolio Buy 
        Calculate total, shares and cash upto the date.
        Before we buy, we need to update share numbers. "
        """
        self.cal_total(date)
        last_valid = self.equities.loc[tick,:,'shares'].last_valid_index()
        self.equities.loc[tick, last_valid:date, 'shares'] = self.equities.loc[tick, last_valid, 'shares']
        self.equities.loc[tick, date, 'shares'] += shares
        self.cash[date] -= price*shares
        self.total[date] = self.dailysum(date)
        if update_ol:
            self.order.append(Order(action="buy", date=date, tick=tick, shares=shares, price=self.equities[tick]['close'][date]))

    def sell(self, shares, tick, price, date, update_ol=False):
        """
        Portfolio sell 
        Calculate shares and cash upto the date.
        """
        self.cal_total(date)
        last_valid = self.equities.loc[tick,:,'shares'].last_valid_index()
        self.equities.loc[tick, last_valid:date, 'shares'] = self.equities.loc[tick, last_valid, 'shares']
        self.equities.loc[tick, date, 'shares'] -= shares
        self.cash[date] += price*shares
        self.total[date] = self.dailysum(date)
        if update_ol:
            self.order.append(Order(action="sell", date=date, tick=tick, shares=shares, price=self.equities[tick]['close'][date]))

    def fillna_cash(self, date):
        " fillna on cash up to date "
        update_start = self.cash.last_valid_index()
        update_end = date
        self.cash[update_start:update_end] = self.cash[update_start]
        return update_start, update_end 

    def fillna(self, date):
        """
        fillna cash and all equities.
        return update_start and update_end.
        """
        update_start, update_end = self.fillna_cash(date)
        for tick in self.equities:
            self.equities.loc[tick, update_start:update_end,'shares'] = self.equities.loc[tick, update_start, 'shares']
        return update_start, update_end

    def cal_total(self, date=None):
        """
        Calculate total up to "date".
        """
        if date == None:
            equities_sum = pd.Series(index=self.ldt_timestamps())
            each_total = self.equities.loc[:,:,'close'] * self.equities.loc[:,:,'shares']
            equities_sum = each_total.sum(axis=1)
            self.total = self.cash + equities_sum       
        else:
            start, end = self.fillna(date)
            equities_total_df = self.equities.loc[:,start:end,'shares'] * self.equities.loc[:,start:end,'close']
            equities_total = equities_total_df.sum(axis=1)
            self.total[start:end ] = equities_total + self.cash[start:end]

    def put_orders(self):
        """
        Put the order list to the DataFrame.
        Update shares, cash columns of each Equity
        """
        for o in self.order:
            if o.action.lower() == "buy":
                self.buy(date=o.date, shares=np.float(o.shares), price=np.float(o.price), tick=o.tick)
            elif o.action.lower() == "sell":
                self.sell(shares=np.float(o.shares), tick=o.tick, price=np.float(o.price), date=o.date)

    def sim(self, ldt_timestamps=None):
        """
        Go through each day and calculate total and cash.
        """
        self.put_orders()
        if ldt_timestamps == None:
            ldt_timestamps = self.ldt_timestamps()
        dt_end = ldt_timestamps[-1]
        self.cal_total()

    def csvwriter(self, equity_col=None, csv_file="pf.csv", total=True, cash=True, d=','):
        """
        Write the content of the Portfolio to a csv file.
        If total is True, the total is printed to the csv file.
        If cash is True, the cash is printed to the csv file.
        equity_col specify which columns to print for an equity.
        The specified columns of each equity will be printed.
        """
        lines = []
        l = []
        l.append("Date")
        if total:
            l.append("Total")
        if cash:
            l.append("Cash")
        if equity_col != None:
            for e in self.equities:
                for col in equity_col:
                    label = e + col
                    l.append(label)
        lines.append(l)
        for i in self.ldt_timestamps():
            l = []
            l.append(i.strftime("%Y-%m-%d"))
            if total:
                l.append(round(self.total[i], 2))
            if cash:
                l.append(round(self.cash[i], 2))
            if equity_col != None:
                for e in self.equities:
                    for col in equity_col:
                        l.append(round(self.equities[e][col][i], 2))
            lines.append(l)
        with open(csv_file, 'w') as fp:
            cw = csv.writer(fp, lineterminator='\n', delimiter=d)
            for line in lines:
                cw.writerow(line)

    def write_order_csv(self, csv_file="pf_order.csv", d=','):
        lines = []
        for i in self.order:
            l = []
            l.append(i.date.strftime("%Y-%m-%d"))
            l.append(i.tick)
            l.append(i.action)
            l.append(i.shares)
            lines.append(l)
        with open(csv_file, 'w') as fp:
            cw = csv.writer(fp, lineterminator='\n', delimiter=d)
            for line in lines:
                cw.writerow(line)
        
    def daily_return(self):
        """
        Return the return of each day, a list.
        """
        daily_rtn = []
        for date in range(len(self.ldt_timestamps())):
            if date == 0:
                daily_rtn.append(0)
            else:
             daily_rtn.append((self.total[date]/self.total[date-1])-1)
        return np.array(daily_rtn)

    def std(self):
        " Standard Deviation of the daily_return "
        return np.std(self.daily_return())

    def normalized(self, tick=None):
        if tick == None:
            return self.total/self.total[0]
        else:
            return self.equities[tick]['close']/self.equities[tick]['close'][0]

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

    def return_ratio(self, tick=None):
        " Return the return ratio of the period "
        if tick == None:
            return self.total[-1]/self.total[0]
        else:
            return self.equities.loc[tick, -1, 'close']/self.equities.loc[tick, 0, 'close']

    def moving_average(self, window=20, tick=None):
        """
        Return an array of moving average. Window specified how many days in
        a window.
        """
        if tick == None:
            ma = pd.stats.moments.rolling_mean(self.total, window=window)
        else:
            ma = self.equities[tick].stats.moments.rolling_mean(window=window)
        ma[0:window] = ma[window]
        return ma

    def drawdown(self, window=10):
        """
        Find the peak within the retrospective window.
        Drawdown is the difference between the peak and the current value.
        """
        ldt_timestamps = self.ldt_timestamps()
        pre_timestamps = ut.pre_timestamps(ldt_timestamps, window)
        # ldf_data has the data prior to our current interest.
        # This is used to calculate moving average for the first window.
        merged_data = self.total[pd.Index(pre_timestamps[0]), ldt_timestamps[-1]]
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

    def random_choose_tick(self, exclude=[]):
        """
        Randomly return a ticker in the portfolio.
        The items in exclude list are not in the select pool.
        """
        ex_set = set(exclude)
        pf_set = set([x for x in self.equities])
        sel_ls = [s for s in pf_set - ex_set]
        return random.choice(sel_ls) 

    def equities_long(self, date):
        """
        Return the list of long equities on the date.
        "Long equities" means the number of shares of the equity is greater than 0.
        """
        return [x for x in self.equities if self.equities[x].shares[date] > 0]

    def ldt_timestamps(self):
        """
        Return an array of datetime objects.
        """
        ldt_index = self.total.index
        dt_start = ldt_index[0] 
        dt_end = ldt_index[-1] 
        dt_timeofday = dt.timedelta(hours=16)
        ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt_timeofday)
        return ldt_timestamps

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
        merged_data = pd.concat([ldf_data[tick]['close'], self.equities[tick]['close']])
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
