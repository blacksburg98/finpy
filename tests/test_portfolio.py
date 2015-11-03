import os
import unittest
import sys
import csv
import datetime as dt
from finpy.financial.equity import get_tickdata
from finpy.financial.equity import Equity
from finpy.financial.portfolio import Portfolio
from finpy.financial.order import Order
import finpy.utils.fpdateutil as du
class TestPortfolioFunctions(unittest.TestCase):
    """
    _AAPL, _GOOG, _IBM and _XOM are for testing only.
    The real tickers are AAPL, GOOG, IBM and XOM.
    """
    def setUp(self):
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
        self.pf = Portfolio(equities=all_stocks, cash=cash, dates=ldt_timestamps, order_list=order_list)
        self.pf.sim()

    def test_total(self):
        print("test total...")
        self.assertEqual(self.pf.total[-1], 1133860.0)
        
    def test_sharpe_ratio(self):
        print("test sharpe...")
        self.assertEqual(round(self.pf.sharpe_ratio(), 4), 0.066)

    def test_totalReturn(self):
        print("test total return...")
        self.assertEqual(self.pf.return_ratio(), 1.13386)

    def test_std(self):
        print("test Standard Deviation...")
        self.assertEqual(round(self.pf.std(),6), 0.007175)

    def test_avg_daily_return(self):
        print("test daily return...")
        self.assertEqual(round(self.pf.avg_daily_return(),8), 0.00054935)

    def test_random_choose_tick(self):
        print("test random choose tick...")
        exclude_ls = ["_GOOG", "_AAPL"]
        choice = self.pf.random_choose_tick(exclude=exclude_ls)
        print("All equities are ", [x for x in self.pf.equities])
        print("The choice is ", choice)
        self.assertTrue(choice in [x for x in self.pf.equities])
        self.assertFalse(choice in [x for x in exclude_ls])

    def test_equities_own(self):
        print("test equities owned...")
        date = dt.datetime(2011,1,12, 16)
        self.assertEqual(self.pf.equities_long(date), ["_AAPL"])
        date = dt.datetime(2011,3,4, 16)
        self.assertEqual(self.pf.equities_long(date), ["_IBM"])

    def test_beta(self):
        print("test beta...")
        beta = self.pf.beta('$SPX')
        self.assertEqual(round(beta,4), -0.4234)

    def test_info_ratio(self):
        print("test information ratio...")
        self.assertEqual(round(self.pf.info_ratio('$SPX'), 4), 0.0467)

    def test_appraisal_ratio(self):
        print("test appraisal ratio...")
        self.assertEqual(round(self.pf.appraisal_ratio('$SPX'), 4), 0.0363)

    def test_residual_return(self):
        print("test residual return...")
        self.assertEqual(round(self.pf.mean_residual_return('$SPX'), 6), 0.000449)

    def test_equity_total(self):
        print("test equity total...")
        self.assertEqual(round(self.pf.equities['_AAPL']['close'][-1],2), 394.26)
        
    def test_equity_sharpe_ratio(self):
        print("test equity sharpe...")
        self.assertEqual(round(self.pf.sharpe_ratio(tick='_AAPL'), 4), 0.0399)

    def test_equity_return_ratio(self):
        print("test equity total return...")
        self.assertEqual(round(self.pf.return_ratio('_AAPL'), 8), 1.15622159)

    def test_equity_std(self):
        print("test equity Standard Deviation...")
        self.assertEqual(round(self.pf.std('_AAPL'),6), 0.016765)

    def test_equity_avg_daily_return(self):
        print("test equity daily return...")
        self.assertEqual(round(self.pf.avg_daily_return('_AAPL'),8), 0.00074551)

    def test_equity_beta(self):
        print("test equity beta...")
        beta = self.pf.beta('$SPX', '_AAPL')
        self.assertEqual(round(beta,4), -0.8104)

    def test_equity_info_ratio(self):
        print("test equity information ratio...")
        self.assertEqual(round(self.pf.info_ratio('$SPX', tick='_AAPL'), 4), 0.0569)

    def test_equity_appraisal_ratio(self):
        print("test equity appraisal ratio...")
        self.assertEqual(round(self.pf.appraisal_ratio('$SPX', tick='_AAPL'), 4), 0.0235)

    def test_equity_residual_return(self):
        print("test equity residual return...")
        self.assertEqual(round(self.pf.mean_residual_return(benchmark='$SPX', tick='_AAPL'), 6), 0.000622)
 
if __name__ == '__main__':
    if 'FINPYDATA' in os.environ:
        tmp_finpydata = os.environ['FINPYDATA']
    os.environ['FINPYDATA'] = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data')) 
    unittest.main()
    if tmp_finpydata in locals():
        os.environ['FINPYDATA'] = tmp_finpydata
    else:
        os.environ.pop('FINPYDATA', None)
