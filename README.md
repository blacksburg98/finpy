=====
finpy
Financial Python
=====
This is mainly inspired by QSTK and Professor Tucker's Computational Investing I
 at Coursera. I plan to expand the capabilities. Please let me know if you have 
any suggestions.
You can reach me at blacksburg98 (at) yahoo dot com

I've tried to use docstring as much as possible, so you can try these commands
 in python shell to get more information.

::
    from finpy.utils import get_tickdata
    from finpy.equity import Equity
    help(Equity)
    from finpy.portfolio import Portfolio
    help(Portfolio)

Please go to https://github.com/blacksburg98/finpy to file a issue if you have
 any problems.

Recommend:
Copy stock_data to a separate area.
    cp -R stock_data ~/stock_data
    setenv FINPYDATA ~/stock_data

=====
Tutorial 1
=====
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

