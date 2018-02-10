import matplotlib
matplotlib.use('Agg') # fix for matplotlib under multiprocessing
import matplotlib.pyplot as plt
import matplotlib.dates as mdates 
import datetime as dt
from finpy.equity import get_tickdata
from finpy.portfolio import Portfolio

import finpy.fpdateutil as du
dt_timeofday = dt.timedelta(hours=16)
dt_start = dt.datetime(2010, 1, 1)
dt_end = dt.datetime(2010, 12, 31)
ls_symbols = ['AAPL','XOM', 'IBM', 'MSFT']
ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt_timeofday)
all_stocks = get_tickdata(ls_symbols=ls_symbols, ldt_timestamps=ldt_timestamps)
p = Portfolio(all_stocks, 0, ldt_timestamps)
p.RSI('AAPL')
