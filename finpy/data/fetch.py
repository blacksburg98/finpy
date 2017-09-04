import datetime as dt
import pandas as pd
import requests
import re
import time
try:
    from io import StringIO
except ImportError:
    from StringIO import StringIO

class Fetcher:
    api_url = "https://query1.finance.yahoo.com/v7/finance/download/%s?period1=%s&period2=%s&interval=%s&events=history&crumb=%s"
    def __init__(self, ticker, start, *args):
        """Initializes class variables and formats api_url string"""
        self.ticker = ticker.upper()
        self.interval = "1d"
        self.cookie, self.crumb = self.init()

        self.start = int(time.mktime(dt.datetime(start[0],start[1],start[2]).timetuple()))

        if args:
            end = args[0]
            self.end = int(time.mktime(dt.datetime(end[0],end[1],end[2]).timetuple()))
        else:
            self.end = int(time.time())

        self.url = self.api_url % (self.ticker, self.start, self.end, self.interval, self.crumb)

    def init(self):
        """Returns a tuple pair of cookie and crumb used in the request"""
        url = 'https://finance.yahoo.com/quote/%s/history' % (self.ticker)
        try:
            r = requests.get(url)
        except requests.exceptions.RequestException:
            raise ValueError
        txt = r.content
        try:
            cookie = r.cookies['B']
            pattern = re.compile('.*"CrumbStore":\{"crumb":"(?P<crumb>[^"]+)"\}')
            for line in txt.splitlines():
                m = pattern.match(line.decode("utf-8"))
                if m is not None:
                    crumb = m.groupdict()['crumb']
                    crumb = crumb.replace(u'\\u002F', '/')
            return cookie, crumb  # return a tuple of crumb and cookie
        except KeyError:
            raise KeyError("{0} is not in Yahoo".format(self.ticker))

    def getHistorical(self):
        """Returns a list of historical data from Yahoo Finance"""
        if self.interval not in ["1d", "1wk", "1mo"]:
            raise ValueError("Incorrect interval: valid intervals are 1d, 1wk, 1mo")

        try:
            data = requests.get(self.url, cookies={'B':self.cookie})
        except requests.exceptions.RequestException:
            raise ValueError
        content = StringIO(data.content.decode("utf-8"))
        return pd.read_csv(content, sep=',')

    def getDatePrice(self):
        """Returns a DataFrame for Date and Price from getHistorical()"""
        return self.getHistorical().ix[:,[0,5]]

    def getDateVolume(self):
        """Returns a DataFrame for Date and Volume from getHistorical()"""
        return self.getHistorical().ix[:,[0,6]]
