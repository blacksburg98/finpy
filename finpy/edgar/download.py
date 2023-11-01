import asyncio
import aiohttp
from aiolimiter import AsyncLimiter
import time
from datetime import date
import datetime
import pandas as pd
import re
import os
import sqlite3
import json
from contextlib import closing

async def async_download_url(url, hdr, limiter, semaphore):
    s = time.perf_counter()
    async with aiohttp.ClientSession(headers=hdr) as session:
        await semaphore.acquire()
        async with limiter:
            async with session.get(url) as resp:
                content = await resp.text()
                semaphore.release()
                return content
    
class download():
    def __init__(self, ticker_info, name, email, debug = True):
        self.ranking = ticker_info['ranking']
        self.cik = ticker_info['cik']
        self.ticker = ticker_info['ticker']
        self.name = ticker_info['name']
        self.sic = ticker_info['sic']
        self.sicDescription = ticker_info['sicDescription']
        self.latest_filing_date = ticker_info['latest_filing_date']
        self.latest_accessionNumber = ticker_info['latest_accessionNumber']
        self.latest_form = ticker_info['latest_form']
        self.company_ticker_json_file = os.path.join(os.path.join(os.environ['FINPYDATA'], "edgar", "files", "company_tickers.json"))
        self.company_ticker_json_db = os.path.join(os.path.join(os.environ['FINPYDATA'], "edgar", "files", "company_tickers.db"))
        self.hdr = {'User-Agent' : name + email}
        self.url = ""
        self.debug = debug
        
    @classmethod
    async def async_create(cls, ticker, name, email, nodownload, debug, limiter, semaphore, r):
        self = cls(ticker, name, email, debug)
        if (self.latest_filing_date == None) or (date.today() > (self.latest_filing_date + datetime.timedelta(days=90))):
            await self.async_get_cik_json(nodownload, limiter, semaphore)
        await self.async_get_fact_json(limiter, semaphore)
        with closing(sqlite3.connect(self.company_ticker_json_db)) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute("UPDATE COMPANY SET sic = ?, sicDescription = ?, latest_filing_date = ?,  latest_accessionNumber = ?, latest_form = ? where ticker = ?", \
                               (self.sic, self.sicDescription, self.latest_filing_date, self.latest_accessionNumber, self.latest_form , self.ticker))
            conn.commit()
        r[self.ticker] = self
        return self

    async def async_get_cik_json(self, nodownload, limiter, semaphore):
        self.edgar_root = "https://www.sec.gov/"
        self.edgar_data = "Archives/edgar/data/"
        url_str = 'https://data.sec.gov/submissions/CIK{}.json'.format(self.cik)
        if not os.path.isdir(os.path.join(os.environ['FINPYDATA'], "edgar", "submissions")):
            os.makedirs(os.path.join(os.environ['FINPYDATA'], "edgar", "submissions"))
        self.cik_json_file = os.path.join(os.environ['FINPYDATA'], "edgar", "submissions", self.ticker + '.json')
        if not os.path.exists(self.cik_json_file) or (date.fromtimestamp(os.path.getmtime(self.cik_json_file)) != date.today()) or not nodownload:   
            print(url_str)
            content = await async_download_url(url_str, self.hdr, limiter, semaphore)
            with open(self.cik_json_file, 'w') as file:
                file.write(content)
            cik_json = json.loads(content)
        else:
            print(self.cik_json_file)
            with open(self.cik_json_file, 'r') as file:
                cik_json = json.load(file)
        self.sic = cik_json["sic"]
        self.sicDescription = cik_json["sicDescription"] 
        accessionNumber = zip(cik_json['filings']['recent']['form'],
                              cik_json['filings']['recent']['accessionNumber'],
                              cik_json['filings']['recent']['filingDate'])
        fin_forms = { "10-Q", "10-K", "20-K", "20-F", "40-F"}
        for i in accessionNumber:
            if i[0] in fin_forms:
                self.latest_form = i[0]
                self.latest_accessionNumber = i[1]
                self.latest_filing_date = date.fromisoformat(i[2])
                print(self.ticker, "the latest filing date of 10-Q or 10-K", self.latest_filing_date)
                break

    def get_all_forms(self, form):
        form_accessionNumbers = self.find_form_accessionNumbers(form)
        for a in form_accessionNumbers:
            aN = a[0].replace('-', '')
            if not os.path.isdir(os.path.join(self.concept_dir, aN)):
                print("form accession number " + a[0] + "of " + self.cik + " is never processed. fetching it...") 
                os.makedirs(os.path.join(self.concept_dir, aN))
                if form == "13F-HR":
                    f13_html = self.edgar_root + self.edgar_data + self.cik + "/" + aN + "/" + a[0] + "-index.html"
                    if self.debug:
                        print(f13_html)
                    f13_req = requests.get(f13_html, headers = self.hdr)
                    soup = BeautifulSoup(f13_req.text, 'html.parser')
                    match_re = "/xslForm13F_X01/" + "([\-\d]*|form13fInfoTable).xml"
                    pattern = re.compile(match_re)
                    xml_path = ""
                    for link in soup.find_all('a'):
                        href = link.get('href')
            
    async def async_get_fact_json(self, limiter, semaphore):
        url_str = 'https://data.sec.gov/api/xbrl/companyfacts/CIK{}.json'.format(self.cik)
        if not os.path.isdir(os.path.join(os.environ['FINPYDATA'], "edgar", "api", "xbrl", "companyfacts")):
            os.makedirs(os.path.join(os.environ['FINPYDATA'], "edgar", "api", "xbrl", "companyfacts"));
        self.fact_json_file = os.path.join(os.path.join(os.environ['FINPYDATA'], "edgar", "api", "xbrl", "companyfacts",'{}.json'.format(self.ticker)))
        if self.debug:
            print(self.fact_json_file)
        if os.path.isfile(self.fact_json_file):
            print(self.ticker, self.latest_filing_date, date.fromtimestamp(os.path.getmtime(self.fact_json_file)))
        if not os.path.isfile(self.fact_json_file) or self.latest_filing_date > date.fromtimestamp(os.path.getmtime(self.fact_json_file)):
            if self.debug:
                print(url_str)
            content = await async_download_url(url_str, self.hdr, limiter, semaphore)
            with open(self.fact_json_file, 'w') as file:
                file.write(content)
            fact_json = json.loads(content)
