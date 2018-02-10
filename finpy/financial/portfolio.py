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
import csv
from .order import Order
from .fincommon import FinCommon
import finpy.utils.fpdateutil as du
from finpy.utils import utils as ut
from finpy.financial.equity import get_tickdata

class Portfolio():
    """
    Portfolio has three items.
    equities is a panda Panel of equity data. 
    Reference by ticker. self.equities['AAPL']
    cash is a pandas series with daily cash balance.
    total is the daily balance.
    order_list is a list of Order
    """
    def __init__(self, equities, cash, dates, order_list=None):
        self.equities = pd.Panel(equities)
        """
            :var equities: is a Panel of equities.
        """ 
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
        
    def daily_return(self,tick=None):
        """
        Return the return rate of each day, a list.
            :param tick: The ticker of the equity.
            :type string:
        """
        if tick == None:
            total = self.total
        else:
            total = self.equities.loc[tick,:,'close']
        daily_rtn = total/total.shift(1)-1
        daily_rtn[0] = 0
        return np.array(daily_rtn)

    def avg_daily_return(self, tick=None):
        " Average of the daily_return list "
        return np.average(self.daily_return(tick))

    def std(self, tick=None):
        " Standard Deviation of the daily_return "
        return np.std(self.daily_return(tick))

    def normalized(self, tick=None):
        if tick == None:
            return self.total/self.total[0]
        else:
            return self.equities[tick]['close']/self.equities[tick]['close'][0]
    def normalized_price(self, tick):
        self.equities[tick]['open'] = self.equities[tick]['open'] * self.equities[tick]['close']/self.equities[tick]['actual_close']
        self.equities[tick]['high'] = self.equities[tick]['high'] * self.equities[tick]['close']/self.equities[tick]['actual_close']
        self.equities[tick]['low'] = self.equities[tick]['low'] * self.equities[tick]['close']/self.equities[tick]['actual_close']

    def sortino(self, k=252, tick=None):
        """
        Return Sortino Ratio. 
        You can overwirte the coefficient with k.
        The default is 252.
        """
        daily_rtn = self.daily_return(tick)
        negative_daily_rtn = daily_rtn[daily_rtn < 0]
        sortino_dev = np.std( negative_daily_rtn)
        sortino = (self.avg_daily_return(tick) / sortino_dev) * np.sqrt(k)
        return sortino

    def return_ratio(self, tick=None):
        " Return the return ratio of the period "
        if tick == None:
            return self.total[-1]/self.total[0]
        else:
            return self.equities[tick]['close'][-1]/self.equities.loc[tick]['close'][0]

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

    def excess_return(self, rf_tick="$TNX", tick=None):
        """
        An excess return is the difference between an asset's return and the riskless rate. 
        """
        return self.daily_return(tick=tick) - ut.riskfree_return(self.ldt_timestamps(), rf_tick=rf_tick)

    def mean_excess_return(self, rf_tick="$TNX", tick=None):
        return np.mean(self.excess_return(rf_tick=rf_tick, tick=tick))

    def residual_return(self, benchmark, rf_tick="$TNX", tick=None):
        """
        A residual return is the excess return minus beta times the benchmark excess return.
        """
        beta = self.beta(benchmark, tick)
        return  self.excess_return(rf_tick=rf_tick, tick=tick) - beta * self.excess_return(rf_tick=rf_tick, tick=benchmark)

    def mean_residual_return(self, benchmark, rf_tick="$TNX", tick=None):
        return np.mean(self.residual_return(benchmark=benchmark, rf_tick=rf_tick, tick=tick))

    def residual_risk(self, benchmark, rf_tick="$TNX", tick=None):
        """
        Residual Risk is the standard deviation of the residual return.
        """
        return np.std(self.residual_return(benchmark=benchmark, rf_tick=rf_tick, tick=tick))

    def active_return(self, benchmark, tick=None):
        """
        An active return is the difference between the benchmark and the actual return.
        """
        return self.daily_return(tick=tick) - self.daily_return(tick=benchmark)

    def mean_active_return(self, benchmark, tick=None):
        return np.mean(self.active_return(benchmark, tick))

    def beta_alpha(self, benchmark):
        """
        benchmark is an Equity representing the market. 
        It can be S&P 500, Russel 2000, or your choice of market indicator.
        This function uses polyfit in numpy to find the closest linear equation.
        """
        beta, alpha = np.polyfit(self.daily_return(tick=benchmark), self.daily_return(), 1)
        return beta, alpha

    def beta(self, benchmark, tick=None):
        """
        benchmark is an Equity representing the market. 
        This function uses cov in numpy to calculate beta.
        """
        benchmark_return = self.daily_return(tick=benchmark) 
        C = np.cov(benchmark_return, self.daily_return(tick=tick))/np.var(benchmark_return)
        beta = C[0][1]/C[0][0]
        return beta

    def excess_risk(self, rf_tick="$TNX", tick=None):
        """
        $FVX is another option. Five-Year treasury rate.
        An excess risk is the standard deviation of the excess return.
        """
        return np.std(self.excess_return(rf_tick=rf_tick, tick=tick))

    def active_risk(self, benchmark, tick=None):
        """
        An active risk is the standard deviation of the active return.
        """
        return np.std(self.active_return(benchmark, tick))

    def info_ratio(self, benchmark, rf_tick="$TNX", tick=None):
        """
        Information Ratio
        https://en.wikipedia.org/wiki/Information_ratio
        Information Ratio is defined as active return divided by active risk,
        where active return is the difference between the return of the security
        and the return of a selected benchmark index, and active risk is the
        standard deviation of the active return.
        """
        return self.mean_active_return(benchmark=benchmark, tick=tick)/self.active_risk(benchmark=benchmark, tick=tick)

    def appraisal_ratio(self, benchmark, rf_tick="$TNX", tick=None):
        """
        Appraisal Ratio
        https://en.wikipedia.org/wiki/Appraisal_ratio
        Appraisal Ratio is defined as residual return divided by residual risk,
        where residual return is the difference between the return of the security
        and the return of a selected benchmark index, and residual risk is the
        standard deviation of the residual return.
        """
        return self.mean_residual_return(benchmark, rf_tick, tick)/self.residual_risk(benchmark, rf_tick, tick)

    def sharpe_ratio(self, rf_tick="$TNX", tick=None):
        """
        Return the Original Sharpe Ratio.
        https://en.wikipedia.org/wiki/Sharpe_ratio
        rf_tick is Ten-Year treasury rate ticker at Yahoo.

        """
        return self.mean_excess_return(rf_tick=rf_tick, tick=tick)/self.excess_risk(rf_tick=rf_tick, tick=tick)

    def up_ratio(self, date, tick, days=10):
        """
        Return the ratio of the past up days.
        This function only applies to equities.
        """
        ldt_index = self.ldt_timestamps()
        last = date
        first = date-days
        up = 0.0
        dn = 0.0
        for i in range(first, last+1):
            if self.equities[tick]['close'][i] < self.equities[tick]['close'][i-1]:
                dn += 1
            else:
                up += 1
        ratio = up / (dn + up)
        return ratio

    def dn_ratio(self, date,tick , days=10):
        """
        Return the ratio of the past down days. 
        This function only applies to equities.
        """
        ratio = 1.0 - self.up_ratio(date=date, tick=tick, days=days)
        return ratio

    def rolling_normalized_stdev(self, tick, window=50):
        """
        Return the rolling standard deviation of normalized price.
        This function only applies to equities.
        """
        ldt_timestamps = self.ldt_timestamps()
        pre_timestamps = ut.pre_timestamps(ldt_timestamps, window)
        # ldf_data has the data prior to our current interest.
        # This is used to calculate moving average for the first window.
        ldf_data = get_tickdata([tick], pre_timestamps)
        merged_data = pd.concat([ldf_data[tick]['close'], self.equities[tick]['close']])
        all_timestamps = pre_timestamps.append(ldt_timestamps)
        merged_daily_rtn = (self.equities.loc[tick,:,'close']/self.equities.loc[tick,:,'close'].shift(1)-1)
        merged_daily_rtn[0] = 0
        sigma = pd.rolling_std(merged_daily_rtn, window=window)
        return sigma[self.ldt_timestamps()]

    def max_rise(self, tick, date, window=20):
        """
        Find the maximum change percentage between the current date and the bottom of the retrospective window.

            :param tick: ticker
            :type tick: string
            :param date: date to calculate max_rise
            :type date: datetime
            :param window: The days of window to calculate max_rise.
            :type window: int
        """
        ldt_timestamps = self.ldt_timestamps() 
        pre_timestamps = ut.pre_timestamps(ldt_timestamps, window)
        first = pre_timestamps[0]
        # ldf_data has the data prior to our current interest.
        # This is used to calculate moving average for the first window.
        try:
            self.equties['close'][first]
            merged_data = self.equties['close']
        except:
            ldf_data = get_tickdata([tick], pre_timestamps)
            merged_data = pd.concat([ldf_data[tick]['close'], self.equities.loc[tick,:,'close']])
        if(isinstance(date , int)):
            int_date = ldt_timestamps[date]
        else:
            int_date = date
        c = merged_data.index.get_loc(int_date)
        m = merged_data[c-window:c].min()
        r = (merged_data[c]-m)/merged_data[c]
        return r

    def max_fall(self, tick, date, window=20):
        """
        Find the change percentage between the top and the bottom of the retrospective window.

            :param tick: ticker
            :type tick: string
            :param date: date to calculate max_rise
            :type date: datetime
            :param window: The days of window to calculate max_rise.
            :type window: int
        """
        ldt_timestamps = self.ldt_timestamps() 
        pre_timestamps = ut.pre_timestamps(ldt_timestamps, window)
        first = pre_timestamps[0]
        # ldf_data has the data prior to our current interest.
        # This is used to calculate moving average for the first window.
        try:
            self.equties['close'][first]
            merged_data = self.equties['close']
        except:
            ldf_data = get_tickdata([tick], pre_timestamps)
            merged_data = pd.concat([ldf_data[tick]['close'], self.equities.loc[tick,:,'close']])
        if(isinstance(date , int)):
            int_date = ldt_timestamps[date]
        else:
            int_date = date
        c = merged_data.index.get_loc(int_date)
        mx = merged_data[c-window:c].max()
        mn = merged_data[c-window:c].min()
        r = (mx-mn)/merged_data[c]
        return r

    def moving_average(self, tick, window=20):
        """
        Return an array of moving average. Window specified how many days in
        a window.

            :param tick: ticker
            :type tick: string
            :param window: The days of window to calculate moving average.
            :type window: int
        """
        mi = self.bollinger_band(tick=tick, window=window, mi_only=True)
        return mi

    def bollinger_band(self, tick, window=20, k=2, mi_only=False):
        """
        Return four arrays for Bollinger Band. The upper band at k times an N-period
        standard deviation above the moving average. The lower band at k times an N-period
        below the moving average.

            :param tick: ticker
            :type tick: string
            :param window: The days of window to calculate Bollinger Band.
            :type window: int
            :param k: k * 
            :return bo: bo['mi'] is the moving average. bo['lo'] is the lower band.
               bo['hi'] is the upper band. bo['ba'] is a seris of the position of the current 
               price relative to the bollinger band.
            :type bo: A dictionary of series.
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

    def RSI(self, tick):
        """
        Relative Strength Index
        http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:relative_strength_index_rsi
        This function uses roughly 250 prior points to calculate RS.

            :param tick: The ticker to calculate RSI
            :type tick: string
            :return rsi[ldt_timestamps]: RSI series
        """
        ldt_timestamps = self.ldt_timestamps()
        pre_timestamps = ut.pre_timestamps(ldt_timestamps, 250)
        ldf_data = get_tickdata([tick], pre_timestamps)
        merged_data = pd.concat([ldf_data[tick]['close'], self.equities[tick]['close']])
        delta = merged_data.diff()
        gain = pd.Series(delta[delta > 0], index=delta.index).fillna(0)
        loss = pd.Series(delta[delta < 0], index=delta.index).fillna(0).abs()
        avg_gain = pd.Series(index=delta.index)
        avg_loss = pd.Series(index=delta.index)
        rsi = pd.Series(index=delta.index)
        avg_gain[14] = gain[1:15].mean()
        avg_loss[14] = loss[1:15].mean()
        for i in range(15, len(delta.index)):
            avg_gain[i] = (avg_gain[i-1]*13+gain[i])/14
            avg_loss[i] = (avg_loss[i-1]*13+loss[i])/14
            if avg_loss[i] == 0:
                rsi[i] = 100
            else:
                rs = avg_gain[i]/avg_loss[i]
                rsi[i] = 100 - 100/(1+rs)
        return(rsi[ldt_timestamps])
