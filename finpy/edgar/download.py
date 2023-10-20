import asyncio
import aiohttp
from aiolimiter import AsyncLimiter
import time
from datetime import date
import pandas as pd
import re
import os
import json
from dyplot.bar import Bar
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

class download():
    def __init__(self, ticker, name, email, debug = True):
        self.ticker = ticker
        self.hdr = self.hdr = {'User-Agent' : name + email}
        self.url = ""
        self.debug = debug
        
    @classmethod
    async def async_create(cls, ticker, name, email, debug, limiter, semaphore, r):
        self = cls(ticker, name, email, debug)
        await self.async_get_cik(limiter, semaphore)
        await self.async_get_cik_json(limiter, semaphore)
        await self.async_get_fact_json(limiter, semaphore)
        r[self.ticker] = self
        return self

    async def async_download_url(self, url, limiter, semaphore):
        s = time.perf_counter()
        async with aiohttp.ClientSession(headers=self.hdr) as session:
            await semaphore.acquire()
            async with limiter:
                if self.debug:
                    print(f"Begin downloading {url} {(time.perf_counter() - s):0.4f} seconds")
                async with session.get(url) as resp:
                    content = await resp.text()
                    if self.debug:
                        print(f"Finished downloading {url}")
                    semaphore.release()
                    return content
    
    async def async_get_cik(self, limiter, semaphore, debug = True):
        url = 'http://www.sec.gov/cgi-bin/browse-edgar?CIK={}&Find=Search&owner=exclude&action=getcompany&count=10&type=10-q'
        self.url_10q = url.format(self.ticker)
        print(self.url_10q)
        content = await self.async_download_url(self.url_10q, limiter, semaphore)
        tables = pd.read_html(content)
        table = tables[2]
        if self.debug:
            print(self.ticker, self.url_10q)
        self.latest_filing_date = date.fromisoformat(list(table.iloc[0])[3])
        url = 'http://www.sec.gov/cgi-bin/browse-edgar?CIK={}&Find=Search&owner=exclude&action=getcompany&count=10&type=10-k'
        self.url_10k = url.format(self.ticker)
        print(self.url_10k)
        content = await self.async_download_url(self.url_10k, limiter, semaphore)
        tables = pd.read_html(content)
        table = tables[2]
        self.latest_filing_date_10k = date.fromisoformat(list(table.iloc[0])[3])
        if self.latest_filing_date_10k > self.latest_filing_date:
            self.latest_filing_date = self.latest_filing_date_10k 
        if self.debug:
            print(self.ticker, self.url_10k)
        if self.debug:
            print(self.ticker, "10k", self.latest_filing_date_10k) 
            print(self.ticker, "final", self.latest_filing_date) 
        match = re.search(r'CIK=\d{10}', content)
        if match:
            self.cik = match.group().split('=')[1]
        else:
            exit("No match found.")
        if self.debug:    
            print(self.cik)    
        
    async def async_get_cik_json(self, limiter, semaphore):
        self.edgar_root = "https://www.sec.gov/"
        self.edgar_data = "Archives/edgar/data/"
        url_str = 'https://data.sec.gov/submissions/CIK{}.json'.format(self.cik)
        if self.debug:
            print(url_str)
        if not os.path.isdir(os.path.join(os.environ['FINPYDATA'], "edgar", "submissions")):
            os.makedirs(os.path.join(os.environ['FINPYDATA'], "edgar", "submissions"))
        self.cik_json_file = os.path.join(os.environ['FINPYDATA'], "edgar", "submissions", self.ticker + '.json')
        if self.debug:
            print(self.cik_json_file)
        if os.path.isfile(self.cik_json_file):
            print(self.latest_filing_date, date.fromtimestamp(os.path.getmtime(self.cik_json_file)))
        if not os.path.isfile(self.cik_json_file) or self.latest_filing_date > date.fromtimestamp(os.path.getmtime(self.cik_json_file)):
            content = await self.async_download_url(url_str, limiter, semaphore)
            with open(self.cik_json_file, 'w') as file:
                file.write(content)
            
    async def async_get_fact_json(self, limiter, semaphore):
        url_str = 'https://data.sec.gov/api/xbrl/companyfacts/CIK{}.json'.format(self.cik)
        if self.debug:
            print(url_str)
        if not os.path.isdir(os.path.join(os.environ['FINPYDATA'], "edgar", "api", "xbrl", "companyfacts")):
            os.makedirs(os.path.join(os.environ['FINPYDATA'], "edgar", "api", "xbrl", "companyfacts"));
        self.fact_json_file = os.path.join(os.path.join(os.environ['FINPYDATA'], "edgar", "api", "xbrl", "companyfacts",'{}.json'.format(self.ticker)))
        if self.debug:
            print(self.fact_json_file)
        if os.path.isfile(self.fact_json_file):
            print(self.latest_filing_date, date.fromtimestamp(os.path.getmtime(self.fact_json_file)))
        if not os.path.isfile(self.fact_json_file) or self.latest_filing_date > date.fromtimestamp(os.path.getmtime(self.fact_json_file)):
            content = await self.async_download_url(url_str, limiter, semaphore)
            with open(self.fact_json_file, 'w') as file:
                file.write(content)
            
