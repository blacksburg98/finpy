import sys
import csv
import datetime as dt
from finpy.financial.equity import get_tickdata
from finpy.financial.equity import Equity
from finpy.financial.portfolio import Portfolio
from finpy.financial.order import Order
from dyplot.dygraphs import Dygraphs

import finpy.utils.fpdateutil as du
if __name__ == '__main__':
    """
    python marketsim.py 1000000 orders.csv values.csv
    Where the number represents starting cash and orders.csv is a file of orders organized like this:
    2008, 12, 3, AAPL, BUY, 130
    2008, 12, 8, AAPL, SELL, 130
    2008, 12, 5, IBM, BUY, 50
    values.csv
    2008, 12, 3, 1000000
    2008, 12, 4, 1000010
    2008, 12, 5, 1000250
    """
    cash = sys.argv[1]
    order_file = sys.argv[2]
    value_file = sys.argv[3]
    order_list = []
    dt_timeofday = dt.timedelta(hours=16)
    with open(order_file, 'rU') as csvfile:
        order_reader = csv.reader(csvfile, delimiter=',', skipinitialspace=True)
        for row in order_reader:
            date = dt.datetime(int(row[0]),int(row[1]), int(row[2]), 16)
            o = Order(action=row[4], date=date, tick=row[3], shares=row[5])
            order_list.append(o)
    # order_list needs to be sorted. Otherwise the algorithm won't work.
    date_list = [x.date for x in order_list]
    date_list.sort()
    dt_start = date_list[0]     
    dt_end = date_list[-1] 
    tick_set = set([x.tick for x in order_list])
    ls_symbols = ['$SPX']
    while(tick_set):
        ls_symbols.append(tick_set.pop())
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt_timeofday)
    all_stocks = get_tickdata(ls_symbols=ls_symbols, ldt_timestamps=ldt_timestamps)
    pf = Portfolio(equities=all_stocks, cash=cash, dates=ldt_timestamps, order_list=order_list)
    pf.sim()
    equity_col = ['buy', 'sell', 'close']
    pf.csvwriter(csv_file=value_file, d=',', cash=False)
    print("Details of the Performance of the portfolio :")
    print("Data Range :",    ldt_timestamps[0],    "to",    ldt_timestamps[-1])
    print("Sharpe Ratio of Fund :", pf.sharpe_ratio()) 
    print("Sharpe Ratio of $SPX :", pf.sharpe_ratio('$SPX'))
    print("Total Return of Fund :", pf.return_ratio())
    print(" Total Return of $SPX :", pf.return_ratio('$SPX'))
    print("Standard Deviation of Fund :", pf.std())
    print(" Standard Deviation of $SPX :", pf.std('$SPX'))
    print("Average Daily Return of Fund :", pf.avg_daily_return())
    print("Average Daily Return of $SPX :", pf.avg_daily_return('$SPX'))
    dg = Dygraphs(ldt_timestamps, "date")
    dg.plot(series='S&P 500', mseries=pf.normalized('$SPX'))
    dg.plot(series='Return', mseries=pf.normalized())
    dg.set_options(title="Market Sim")
    dg.savefig(csv_file="marketsim.csv", html_file="marketsim.html")
    
