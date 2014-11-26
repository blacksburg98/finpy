import sys
import csv
import matplotlib
matplotlib.use('Agg') # fix for matplotlib under multiprocessing
import matplotlib.pyplot as plt
import matplotlib.dates as mdates 
import datetime as dt
import sets
import dateutil
from finpy.utils import get_tickdata
from finpy.equity import Equity
from finpy.portfolio import Portfolio
from finpy.order import Order
import finpy.fpdateutil as du
if __name__ == '__main__':
    """
    python marketsim.py 1000000 orders.csv values.csv
    Where the number represents starting cash and orders.csv is a file of orders organized like this:
    2008-12-3, AAPL, BUY, 130
    2008-12-8, AAPL, SELL, 130
    2008-12-5, IBM, BUY, 50
    values.csv
    2008-12-3, 1000000
    2008-12-4, 1000010
    2008-12-5, 1000250
    """
    cash = sys.argv[1]
    order_file = sys.argv[2]
    value_file = sys.argv[3]
    order_list = []
    dt_timeofday = dt.timedelta(hours=16)
    with open(order_file, 'rU') as csvfile:
        order_reader = csv.reader(csvfile, delimiter=',', skipinitialspace=True)
        for row in order_reader:
            date = dateutil.parser.parse(row[0] + "-16")
            if len(row) == 4:
                o = Order(action=row[2], date=date, tick=row[1], shares=row[3])
            else:
                o = Order(action=row[2], date=date, tick=row[1], shares=row[3], price=row[4])
            order_list.append(o)
    # order_list needs to be sorted. Otherwise the algorithm won't work.
    date_list = [x.date for x in order_list]
    date_list.sort()
    dt_start = date_list[0]     
    dt_end = date_list[-1] 
    tick_set = sets.Set([x.tick for x in order_list])
    ls_symbols = ['$GSPC']
    while(tick_set):
        ls_symbols.append(tick_set.pop())
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt_timeofday)
    all_stocks = get_tickdata(ls_symbols=ls_symbols, ldt_timestamps=ldt_timestamps)
    pf = Portfolio(equities=all_stocks, cash=cash, dates=ldt_timestamps, order_list=order_list)
    pf.sim()
    equity_col = ['buy', 'sell', 'close']
    pf.csvwriter(csv_file=value_file, d=',', cash=False)
    print("The final value of the portfolio using the sample file is -- ", pf.total[-1])
    print("Details of the Performance of the portfolio :")
    print("Data Range :",    ldt_timestamps[0],    "to",    ldt_timestamps[-1])
    print("Sharpe Ratio of Fund :", pf.sharpe_ratio()) 
    print("Sortino Ratio of Fund :", pf.sortino()) 
    print("Sharpe Ratio of $GSPC :", pf.equities['$GSPC'].sharpe_ratio())
    print("Total Return of Fund :", pf.return_ratio())
    print(" Total Return of $GSPC :", pf.equities['$GSPC'].return_ratio())
    print("Standard Deviation of Fund :", pf.std())
    print(" Standard Deviation of $GSPC :", pf.equities['$GSPC'].std())
    print("Average Daily Return of Fund :", pf.avg_daily_return())
    print("Average Daily Return of $GSPC :", pf.equities['$GSPC'].avg_daily_return())
    print("Information Ratio of Fund:", pf.info_ratio(pf.equities['$GSPC']))
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.plot(ldt_timestamps, pf.equities['$GSPC'].normalized())
    ax.plot(ldt_timestamps, pf.total/pf.total[0])
    legend = ['$GSPC', "Portfolio"]
    ax.legend(legend, loc=2)
    fig.autofmt_xdate()
    pdf_file = order_file + '.pdf'
    fig.savefig(pdf_file, format='pdf')
    beta, alpha = pf.beta_alpha(pf.equities['$GSPC'])
    print("Beta of the fund is ", beta, ". Alpha of the fund is ", alpha)
    
