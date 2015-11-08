import multiprocessing
import datetime as dt
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import argparse
import re
from pylab import *
import csv

from finpy.financial.equity import get_tickdata
from finpy.financial.portfolio import Portfolio
from finpy.financial.transaction import Transaction
import finpy.utils.fpdateutil as du
from dyplot.dygraphs import Dygraphs
from dyplot.c3.pie import Pie
import urllib.parse as urlparse
import os.path

class Sim():
    """ Sim class allows easily setup for backtesting algorithm.
    """
    def __init__(self, benchmark_tick="$RUA"):
        """ 
        :param banchmark_tick: The ticker for the benchmark. 
            $RUA is Russel 3000.
        Instance Variable
        :var args: Command Line Arguement. Use argparse.
        :var symbols: All stock tickers in the simulation.
        :var dt_start: Backtesting start date.
        :var dt_end: Backtesting end date.
        :var ldt_timestamps: The list contains all transaction dates.
        :var pf: Portfolio from finpy.financial.portfolio.
        :var all_order: The list contains all orders. 
        """
        parser = argparse.ArgumentParser( description='My main algorithm.')
        self._default_args(parser)
        self.add_args(parser)
        self.args = parser.parse_args()
        self.symbols = []
        ticks = open(self.args.tick, 'r')
        csv_file = os.path.join(self.args.dir, 'static', 'csv', self.args.subdir,'tick.csv')
        with open(csv_file, 'w') as h:
            for i in ticks:
                h.write(i)
                self.symbols.append(i.rstrip())
        self.dt_start = dt.datetime.strptime(self.args.start, "%Y-%m-%d")
        self.dt_end = dt.datetime.strptime(self.args.end, "%Y-%m-%d")
        dt_timeofday = dt.timedelta(hours=16)
        self.ldt_timestamps = du.getNYSEdays(self.dt_start, self.dt_end, dt_timeofday)
        self.benchmark = self._benchmark(benchmark_tick)
        self.benchmark_tick = benchmark_tick
        self.pf = None
        self.all_order = []
    def _benchmark(self, ticker):
        bm = get_tickdata(ls_symbols=[ticker], ldt_timestamps=self.ldt_timestamps)
        return(Portfolio(bm, 0, self.ldt_timestamps, []))
    def _default_args(self, parser):
        parser.add_argument('-dir', default="app", help="app directory")
        parser.add_argument('-subdir', default="current", help="the subdir under app directory")
        parser.add_argument('-start', default="2000-1-3", help="starting date: default is 2000-1-3")
        now = dt.datetime.now().strftime("%Y-%m-%d")
        parser.add_argument('-end', default=now, help="ending date: default is now")
        parser.add_argument('-tick', default="short.txt", help="The file contains ticker names to be analyzed")
        parser.add_argument('-cash', default=50000, type=int, help="Initical cash")
        parser.add_argument('-thread', default=multiprocessing.cpu_count(), type=int,
            help="The number CPU threads used to run. default is the maximum threads of the computer.")

    def add_args(self, parser):
        """
        The user can use the following syntax to
        overload the method and expand the command line arguments.
            :param parser: 
                parser.add_argument('-max_hold', default=190, type=int,
                help="Maximum holding days")
        """
        pass
    def run_algo(self):
        """
        run_algo has two steps.
        The first is to run algo_wrapper.
        If thread is greater than 1, then multiple threads are fire.
        Each thread runs the algorithm defined in algo() on one stock.
        Then call organize_algo() to post process the results for backtesting.
            :return self.organize_algo(all_res):
        """
        all_res = []
        # all_res is a list of all returned objects of algo_wrapper.
        # The returned object is a set.
        # return (tick, return_ratio, stock_return, pf.order, equities[tick], stat, last)
# Single-Process Code
        if self.args.thread == 1:
            for tick in self.symbols:
                all_res.append(self.algo_wrapper(tick))
# End Single-Process Code
# Multi-Process Code
        else:
            def append_result(x):
                all_res.append(x)
            # Setting up the number of pool equal to the number of CPU counts
            thread_num = self.args.thread
            pool = multiprocessing.Pool(thread_num)
            for tick in self.symbols:
                pool.apply_async(self.algo_wrapper, args=(tick, ), callback=append_result)
            pool.close()
            pool.join()
# End Multi-Process Code
        all_res.sort(key=lambda x: x[1], reverse=True)
        return(self.organize_algo(all_res))
    def algo(self, equities, tick):
        """
        The user can overload the method to define his own algorithm. 
            :param equities: The return value of  get_tickdata.
                get_tickdata(ls_symbols=[tick], ldt_timestamps=self.ldt_timestamps)
            :param tick: The stock ticker.
            :return pf: pf has all the price information by usring the algorithm.
            :return stat: stat is a list of Transaction items.
        The default algorithm is just looking at Bollinger band.
        If the closing price is lower than the lower band, then buy the stock.
        If the closing price is higher than the higher band, then sell the stock.
        """
        ldt_timestamps = self.ldt_timestamps
        cash = self.args.cash
        pf = Portfolio(equities, cash, ldt_timestamps, [])
        mode = "buy"
        last_buy = {}
        last_buy_close = 0
        stat = []
        i = 0
        close = pf.equities[tick]['close']
        ba = pf.bollinger_band(tick=tick)
        while i < len(ldt_timestamps):
            if mode == "buy":
                if close[i] <= ba['lo'][i]:
                    pf.cal_total(ldt_timestamps[i])
                    shares = np.floor(pf.cash[i]/close[i])
                    print("Buy", shares, " of ", tick, " at ", close[i], " on ", ldt_timestamps[i])
                    s = Transaction(buy_date=i, buy_price=close[i])
                    stat.append(s)
                    pf.buy(date=ldt_timestamps[i], shares=shares,
                        tick=tick, price=close[i], update_ol=True)
                    last_buy_close = close[i]
                    mode = "sell"
            else:
                if close[i] > ba['hi'][i]:
                    update_to = ldt_timestamps[i]
                    pf.cal_total(update_to)
                    shares = pf.equities[tick]['shares'][update_to]
                    print("Sell", shares, " of ", tick, " at ", close[i], " on ", ldt_timestamps[i])
                    stat[-1].sell_date = i
                    stat[-1].sell_price = close[i]
                    pf.sell(date=ldt_timestamps[i], tick=tick, shares=shares, price=close[i], update_ol=True)
                    mode = "buy"
            i += 1
        last_date = ldt_timestamps[-1]
        pf.cal_total(last_date)
        return pf, stat
    def algo_wrapper(self, tick):
        """ The wrapper for algo(). Generate various data for backtesting and viewing.
            :param tick: The ticker of the security.
            :return tick: The ticker of the security.
            :return return_ratio: The return ratio of the algorithm.
            :return stock_return: The return ratio of the security in the same preriod.
            :return pf.order: The order of t
        """
        dt_timeofday = dt.timedelta(hours=16)
        equities = get_tickdata(ls_symbols=[tick], ldt_timestamps=self.ldt_timestamps)
        pf, stat = self.algo(equities=equities, tick=tick)
        # Prepare Data for vaiour charts and diagrams
        csvfile = os.path.join(self.args.dir, 'static', 'csv', self.args.subdir,tick + '.csv')
        pf.csvwriter(equity_col=["shares", "close"], csv_file=csvfile, total=True, cash=True, d=',')
        csvfile = os.path.join(self.args.dir, 'static', 'csv', self.args.subdir,tick + '_order.csv')
        pf.write_order_csv(csv_file=csvfile)
        stdev = pf.rolling_normalized_stdev(tick)
        market_nml = self.benchmark.normalized(self.benchmark_tick)
        market_dygraph = ";" + market_nml.map(str) + ";" 
        tick_nml = pf.normalized(tick) 
        total_nml = pf.total/pf.total[0]
        algo = tick + ' ALGO'
        ba = pf.bollinger_band(tick)
        ba_hi_nml = ba['hi']/pf.equities[tick]['close'][0]
        ba_lo_nml = ba['lo']/pf.equities[tick]['close'][0]
        col = ['b', 'r', 'g', 'c', 'y', 'm']
        buy_list = [x.date for x in pf.order if x.action == "buy"]
        buy_price = pf.normalized(tick)[buy_list]
        sell_list = [x.date for x in pf.order if x.action == "sell"]
        sell_price = pf.normalized(tick)[sell_list]
        dg = Dygraphs(tick_nml.index, "date")
        dg.plot(series=tick, mseries = tick_nml, lseries = ba_lo_nml, hseries = ba_hi_nml)
        dg.plot(series=algo, mseries = total_nml)
        dg.plot(series=self.benchmark_tick, mseries = market_nml)
        for o in pf.order:
            text = tick + '@' + str(pf.equities[tick]['close'][o.date]) + ' on ' \
                + o.date.strftime("%Y-%m-%d")
            dg.annotate(tick, o.date.strftime("%Y-%m-%d"), o.action[0].upper(), text)
        csv_file = os.path.join(self.args.dir, 'static', 'csv', self.args.subdir, tick + "_dygraph.csv")
        url_path = "/static/csv/" + self.args.subdir + "/" + tick + "_dygraph.csv"
        div_id = "id0"
        js_vid = 'dyg' 
        dg.set_options(title=tick, ylabel="Ratio")
        dg.set_axis_options("x", drawAxis = False)
        div = dg.savefig(csv_file=csv_file, div_id=div_id, js_vid=js_vid, dt_fmt="%Y-%m-%d", url_path=url_path)
        gstd = Dygraphs(self.ldt_timestamps, "date")
        gstd.plot(series=tick, mseries= stdev)
        csv_file = os.path.join(self.args.dir, 'static', 'csv', self.args.subdir, tick + "_std.csv")
        url_path = "/static/csv/" + self.args.subdir + "/" + tick + "_std.csv"
        div_id = "id1"
        js_vid = 'std' 
        gstd.set_options(ylabel="Volatility", digitsAfterDecimal="4", height=120)
        divstd = gstd.savefig(csv_file=csv_file, div_id=div_id, js_vid=js_vid, dt_fmt="%Y-%m-%d", height="100ptx", url_path=url_path)
        fail = 0
        succ = 0
        pie = ""
        for x in stat:
            if x.sell_date != None:
                if x.sell_price > x.buy_price:
                    succ += 1
                else:
                    fail += 1
        if succ+fail != 0:
            succ_frac = succ*100/(succ+fail)
            fail_frac = fail*100/(succ+fail)
            labels = 'Success', 'Fail'
            gpie = Pie([succ_frac, fail_frac], labels=labels)
            div_id = tick + "_pie"
            js_vid = 'pie' + tick
            pie = gpie.savefig(div_id=div_id, js_vid=js_vid)
        return_ratio = pf.return_ratio()
        stock_return = pf.equities[tick]['close'][-1]/pf.equities[tick]['close'][0]
        return (tick, return_ratio, stock_return, pf.order, equities[tick], stat, div, divstd, pie)
    def organize_algo(self, all_res):
        """ Post Process algo data.
                :param all_res: all_res is a list contains results from algo.
                    Each item is the result of the simulation on a security.
        """
        equities = {}
        stat = {}
        div = {}
        divstd = {}
        pie = {}
        summary = '<table class="table table-bordered">\n'
        summary += "<th>Ticker</th><th>Algo Return</th><th>Stock Return</th>"
        for y in all_res:
            summary += "<tr>\n" 
            summary += "<td><a href=\"%s.html\">%s</a></td><td>%f</td><td>%f</td>\n" %(y[0], y[0],y[1],y[2])
            summary += "</tr>\n"
            self.all_order.extend(y[3])
            equities[y[0]] = y[4]
            stat[y[0]] = y[5]
            div[y[0]] = y[6]
            divstd[y[0]] = y[7]
            pie[y[0]] = y[8]
        summary += '</table>'
        self.all_order.sort(key=lambda x: x.date)
        for x in equities:
            equities[x]['shares'] = np.NaN
            equities[x]['shares'][0] = 0
        equities[self.benchmark_tick] = self.benchmark.equities[self.benchmark_tick]
        self.pf = Portfolio(equities, self.args.cash, self.ldt_timestamps)
        return (stat, div, divstd, pie, summary)
    def backtesting(self):
        market = self.benchmark
        buy_price = {}
        output = '<table class="table table-bordered">\n'
        output += "<th>Date</th><th>Action</th><th>Ticker</th><th>Shares</th>"
        output += "<th>Price</th><th>Cash Before</th><th>Cash After</th><th>Gain</th>"
        for o in self.all_order:
            self.pf.cal_total(o.date)
            sdate = o.date.strftime("%Y-%m-%d")
            if o.action == "buy":
                output += "<tr>\n"
                if self.pf.cash[o.date] >= 200000.0:
                    buy_size = 20000.0
                elif self.pf.cash[o.date] >= 100000.0:
                    buy_size = 10000.0
                else:
                    buy_size = 5000.0
                if self.pf.cash[o.date] >= buy_size:
                    shares = np.floor(buy_size/o.price)
                    print("buy", shares, "of", o.tick, "at", o.price, o.date)
                    ticka = '<a href=\"%s.html\">%s</a>' %(o.tick, o.tick)
                    output += "<td>%s</td><td>buy</td><td>%s</td><td>%d</td><td>%f</td>" %(sdate,ticka,shares,o.price)
                    output += "<td>%d</td>" %(self.pf.cash[o.date])
                    self.pf.buy(shares=shares, tick=o.tick, price=o.price, date=o.date, update_ol=True)
                    output += "<td>%d</td>" %(self.pf.cash[o.date])
                    output += "<td></td>\n"
                    buy_price[o.tick] = o.price
                else:
                    shares = np.floor(buy_size/o.price)
                    print("Not enough cash. Skip buying", shares, "of", o.tick, "at", o.price, "on", o.date)
                    ticka = '<a href=\"%s.html\">%s</a>' %(o.tick, o.tick)
                    output += "<td>%s</td><td>skip buy</td><td>%s</td><td>%d</td><td>%f</td>\n" %(sdate,ticka,shares,o.price)
                    output += "<td></td><td></td><td></td>\n"
            else:
                shares = self.pf.equities[o.tick]['shares'][o.date]
                price = self.pf.equities[o.tick]['close'][o.date]
                if shares > 0:
                    print("sell", shares, " of", o.tick, " at", o.price, o.date, ". A gain of", o.price/buy_price[o.tick])
                    gain = o.price/buy_price[o.tick]
                    if gain > 1:
                        output += "<tr class=\"success\">\n"
                    else:
                        output += "<tr class=\"danger\">\n"
                    ticka = '<a href=\"%s.html\">%s</a>' %(o.tick, o.tick)
                    output += "<td>%s</td><td>sell</td><td>%s</td><td>%d</td><td>%f</td>"  %(sdate,ticka,shares,o.price)
                    output += "<td>%d</td>" %(self.pf.cash[o.date])
                    self.pf.sell(date=o.date, tick=o.tick, shares=shares, price=price, update_ol=True)
                    output += "<td>%d</td>\n" %(self.pf.cash[o.date])
                    output += "<td>%f</td>\n" %(gain)
            output += "</tr>\n"
        output += "</table>\n"
        fund_csv = os.path.join(self.args.dir, 'static', 'csv', self.args.subdir,'fund.csv')
        self.pf.csvwriter(equity_col=["shares"], csv_file=fund_csv, total=True, cash=True, d=',')
        fund_order_csv = os.path.join(self.args.dir, 'static', 'csv', self.args.subdir,'fund_order.csv')
        self.pf.write_order_csv(csv_file=fund_order_csv)
        ldt_timestamps = self.pf.ldt_timestamps()
        self.pf.cal_total(ldt_timestamps[-1])
        dg = Dygraphs(ldt_timestamps, "date")
        dg.plot(series='Market', mseries = market.normalized('$RUA'))
        dg.plot(series="Portfolio", mseries = self.pf.total/self.pf.total[0])
        csv_file = os.path.join(self.args.dir, 'static', 'csv', self.args.subdir,'fund_dygraph.csv')
        div_id = "fund_graphdiv"
        js_vid = 'gfund'
        dg.set_options(title="Fund Performance")
        url_path = "/static/csv/" + self.args.subdir + "/" + "fund_dygraph.csv"
        fund_graph = dg.savefig(csv_file=csv_file, div_id=div_id, js_vid=js_vid, dt_fmt="%Y-%m-%d", url_path=url_path)
        return output, fund_graph
    def sim_output(self, output, fund_graph, stat, div, divstd, pie, summary):
        succ = 0
        fail = 0
        hold_period = []
        for sym in stat:
            for trnx in stat[sym]:
                if trnx.sell_date != None:
                    hold_period.append(trnx.sell_date-trnx.buy_date)
                    if trnx.sell_price > trnx.buy_price:
                        succ += 1
                    else:
                        fail += 1
        hp = pd.Series(hold_period)
        fig = plt.figure()
        ax = fig.add_subplot(111, title="Holding Period Distribution")
        ax.hist(hp, 10)
        svg_file = os.path.join(self.args.dir, 'static', 'img', self.args.subdir,'fund_hist.svg')
        fig.savefig(svg_file, format='svg')
        if succ+fail != 0:
            succ_frac = succ*100/(succ+fail)
            fail_frac = fail*100/(succ+fail)
            fig = plt.figure()
            ax = fig.add_subplot(111)
            labels = 'Success', 'Fail'
            gpie = Pie([succ_frac, fail_frac], labels=labels)
            fundpie = gpie.savefig(div_id="fund_pie", js_vid="piefund")
        index_file = os.path.join(self.args.dir, 'templates', self.args.subdir,'index.html')
        with open(index_file, 'w') as f:
            f.write('{% extends "base.html" %}\n{% block content %}\n')
            f.write(fund_graph)
            f.write('<embed src="/static/img/' + self.args.subdir + '/fund_scatter.svg" type="image/svg+xml" /><br>\n')
            statement = "The final value of the portfolio using the sample file is %s<br>\n" %(self.pf.total[-1])
            statement += "Details of the Performance of the portfolio :<br>\n"
            statement += "Data Range : %s to %s<br>\n" %(self.ldt_timestamps[0], self.ldt_timestamps[-1])
            statement += "Sharpe Ratio of Fund : %s<br>\n" %( self.pf.sharpe_ratio() )
            statement += "Sortino Ratio of Fund : %s<br>\n" %( self.pf.sortino() )
            statement += "Sharpe Ratio of $RUA : %s<br>\n" %( self.pf.sharpe_ratio(tick=self.benchmark_tick))
            statement += "Total Return of Fund : %s<br>\n" %( self.pf.return_ratio())
            statement += " Total Return of $RUA : %s<br>\n" %( self.pf.return_ratio(tick=self.benchmark_tick))
            statement += "Standard Deviation of Fund : %s<br>\n" %( self.pf.std())
            statement += " Standard Deviation of $RUA : %s<br>\n" %( self.pf.std(tick=self.benchmark_tick))
            statement += "Average Daily Return of Fund : %s<br>\n" %( self.pf.avg_daily_return())
            statement += "Average Daily Return of $RUA : %s<br>\n" %( self.pf.avg_daily_return(tick=self.benchmark_tick))
            statement += "Information Ratio of Fund: %s<br>\n" %( self.pf.info_ratio(benchmark=self.benchmark_tick))
            beta, alpha = self.pf.beta_alpha(benchmark=self.benchmark_tick)
            fig = plt.figure()
            ax = fig.add_subplot(111, title="Func vs Benchmark Scatter Plot")
            benchmark_close = self.benchmark.normalized(self.benchmark_tick) 
            ax.scatter(benchmark_close, self.pf.total/self.pf.total[0])
            xmin = round(np.amin(benchmark_close), 2) 
            xmax = round(np.amax(benchmark_close), 2) 
            x = np.arange(xmin, xmax, 0.01)
            y = beta * x + alpha
            ax.plot(x, y)
            svg_file = os.path.join(self.args.dir, 'static', 'img', self.args.subdir, 'fund_scatter.svg')
            fig.savefig(svg_file, format='svg')
            beta = self.pf.beta(benchmark='$RUA')
            statement += "beta of the fund is %s. <br>\n" %(beta)
            statement += "Active Return of the fund is %s<br>\n" %(self.pf.mean_active_return(benchmark=self.benchmark_tick))
            statement += "Residual Return of the fund is %s<br>\n" %(self.pf.mean_residual_return(benchmark=self.benchmark_tick))
            stmt = re.sub("<br>", "", statement)
            print(stmt)
            f.write(statement)
            sum_html = os.path.join(self.args.dir, 'templates', self.args.subdir, 'summary.html')
            with open(sum_html , 'w') as s:
                s.write('{% extends "base.html" %}\n{% block content %}\n')
                s.write(summary)
                s.write("{% endblock %}")
            tran_html = os.path.join(self.args.dir, 'templates', self.args.subdir, 'transactions.html')
            with open(tran_html , 'w') as t:
                t.write('{% extends "base.html" %}\n{% block content %}\n')
                t.write(output)
                t.write("""
                   <script>
                     $(function(){
                       $("tbody").each(function(elem,index){
                         var arr = $.makeArray($("tr",this).detach());
                         arr.reverse();
                         var last = arr.pop()
                         arr.unshift(last)
                         $(this).append(arr);
                       });
                     });
                   </script>
                """)
                t.write("{% endblock %}")
            f.write(fundpie)
            hist_svg = '<embed src="/static/img/' + self.args.subdir + '/fund_hist.svg" type="image/svg+xml" />\n'
            f.write(hist_svg)
            for sym in self.symbols:
                tick_html = os.path.join(self.args.dir, 'templates', self.args.subdir, sym + '.html')
                with open(tick_html , 'w') as h:
                    h.write('{% extends "tick.html" %}\n{% block content %}\n')
                    link_svg = '<a name="' + sym + '"></a><a href="http://finance.yahoo.com/q?s=' + sym + '"><h1>' + sym + '</h1></a>\n'
                    dygraph = ''
                    h.write(link_svg)
                    h.write(div[sym])
                    h.write(divstd[sym])
                    h.write(pie[sym])
                    h.write("<script>ga = [dyg, std];\nsync = Dygraph.synchronize(ga);</script>")
                    h.write("{% endblock %}")
            f.write("{% endblock %}")
