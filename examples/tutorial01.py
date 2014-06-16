"""
Tutorial 1 
Load stock data and print 
"""
import matplotlib
matplotlib.use('Agg') # fix for matplotlib under multiprocessing
import matplotlib.pyplot as plt
import matplotlib.dates as mdates 
import datetime as dt
from finpy.utils import get_tickdata

import finpy.fpdateutil as du
if __name__ == '__main__':
    dt_timeofday = dt.timedelta(hours=16)
    dt_start = dt.datetime(2010, 1, 1)
    dt_end = dt.datetime(2010, 12, 31)
    ls_symbols = ['AAPL','GOOG', 'IBM', 'MSFT']
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt_timeofday)
    all_stocks = get_tickdata(ls_symbols=ls_symbols, ldt_timestamps=ldt_timestamps)
    fig = plt.figure()
    ax = fig.add_subplot(111)
    for tick in ls_symbols:
        ax.plot(ldt_timestamps, all_stocks[tick].normalized())
    legend = ls_symbols
    ax.legend(legend, loc=2)
    fig.autofmt_xdate()
    svg_file = 'tutorial1.pdf'
    fig.savefig(svg_file)
