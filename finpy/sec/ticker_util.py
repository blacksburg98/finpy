import re
import json
import urllib.request, urllib.error, urllib.parse
import datetime as dt

def get_xbrl_url(final_url):
    try:
        fp = urllib.request.urlopen(final_url)
        next_line_filing_date = False 
        for line in fp:
            filingre = re.search("<div class=\"infoHead\">Filing Date</div>", line)
            datere = re.search("<div class=\"info\">(\d{4}-\d{2}-\d{2})</div>", line)
            if filingre: 
                next_line_filing_date = True
                next
            elif datere and next_line_filing_date : 
                fdate = datere.group(1)
                filing_date = dt.datetime.strptime(fdate, "%Y-%m-%d")
                next_line_filing_date = False
                next
            xmlre = re.search("<a href=\"([^_]*.xml)\">.*.xml</a>", line)
            if xmlre:
                xbrl_url = "http://www.sec.gov" + xmlre.group(1)
        return xbrl_url, filing_date 
    except urllib.error.URLError as e:
        print(final_url)
        return None

def cjk2tick(cjk):
    url = "http://www.sec.gov/cgi-bin/browse-edgar?CIK=" + cjk 
    url += "&Find=Search&owner=exclude&action=getcompany&count=100&type=10-k"
    try:
        fp = urllib.request.urlopen(url)
        for line in fp:
            lre = re.search("<a href=\"(.*)\" id=\"documentsbutton\">.*Interactive Data</a></td>", line)
            if lre:
                final_url = "http://www.sec.gov" + lre.group(1)
                xbrl_url, filing_date = get_xbrl_url(final_url)
                xre = re.search("/([a-z\-]{1,5})-\d*?", xbrl_url)
                if xre:
                    ticker = xre.group(1)
                    if istick(ticker):
                        return ticker
                    else:
                        return 
                else:
                    return 
    except urllib.error.URLError as e:
        print(url)
        return

def istick(t):
    url = "http://www.sec.gov/cgi-bin/browse-edgar?CIK=" + t
    url += "&Find=Search&owner=exclude&action=getcompany&count=100&type=10-k"
    try:
        fp = urllib.request.urlopen(url)
        for line in fp:
            lre = re.search("No matching Ticker Symbol", line)
            if lre:
                return False
        return True
    except urllib.error.URLError as e:
        print(url)
        return False

def get_stock_quote(ticker_symbol):
    """
    If the return object is "quote", then quote['l_cur'] is the current price.
    http://coreygoldberg.blogspot.com/2011/09/python-stock-quotes-from-google-finance.html
    """
    url = 'http://finance.google.com/finance/info?q=%s' % ticker_symbol
    lines = urllib.request.urlopen(url).read().splitlines()
    return json.loads(''.join([x for x in lines if x not in ('// [', ']')]))

