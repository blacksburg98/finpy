import csv
import datetime as dt
from finpy.financial.equity import get_tickdata
from finpy.financial.equity import Equity
from finpy.financial.portfolio import Portfolio
from finpy.financial.order import Order
import finpy.utils.fpdateutil as du
order_list = []
with open("orders.csv", 'rt', encoding="UTF-8") as csvfile:
    order_reader = csv.reader(csvfile, delimiter=',', skipinitialspace=True)
    for row in order_reader:
        date = dt.datetime(int(row[0]),int(row[1]), int(row[2]), 16)
        o = Order(action=row[4], date=date, tick=row[3], shares=row[5])
        order_list.append(o)
# order_list needs to be sorted. Otherwise the algorithm won't work.
date_list = [x.date for x in order_list]
date_list.sort()
dt_timeofday = dt.timedelta(hours=16)
dt_start = date_list[0]     
dt_end = date_list[-1] 
tick_set = set([x.tick for x in order_list])
ls_symbols = ['$SPX']
while(tick_set):
    ls_symbols.append(tick_set.pop())
ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt_timeofday)
cash = 1000000
all_stocks = get_tickdata(ls_symbols=ls_symbols, ldt_timestamps=ldt_timestamps)
pf = Portfolio(equities=all_stocks, cash=cash, dates=ldt_timestamps, order_list=order_list)
pf.sim()
pf.csvwriter(['close','shares'])
