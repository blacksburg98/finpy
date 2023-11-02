from finpy.edgar.download import download
from finpy.edgar.download import async_download_url
from finpy.utils.components import sp500
from finpy.utils.components import custom
from aiolimiter import AsyncLimiter
import asyncio
import time
from datetime import date
import argparse
import os
import json
import sqlite3
from contextlib import closing

async def async_get_company_ticker_json(hdr, tickers, limiter, semaphore):
    url_str = 'https://www.sec.gov/files/company_tickers.json'
    if not os.path.isdir(os.path.join(os.environ['FINPYDATA'], "edgar", "files")):
        os.makedirs(os.path.join(os.environ['FINPYDATA'], "edgar", "files"));
    company_ticker_json_file = os.path.join(os.path.join(os.environ['FINPYDATA'], "edgar", "files", "company_tickers.json"))
    company_ticker_json_db = os.path.join(os.path.join(os.environ['FINPYDATA'], "edgar", "files", "company_tickers.db"))
    content = await async_download_url(url_str, hdr, limiter, semaphore)
    print(company_ticker_json_file)
    with open(company_ticker_json_file, 'w') as file:
        file.write(content)
    company_tickers_json = json.loads(content)
    try:       
        with closing(sqlite3.connect(company_ticker_json_db)) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute('''CREATE TABLE COMPANY (ranking INT NOT NULL,
                                                        cik TEXT NOT NULL,
                                                        ticker TEXT PRIMARY KEY NOT NULL,
                                                        name TEXT NOT NULL,
                                                        sic INT NULL,
                                                        sicDescription TEXT NULL,
                                                        latest_filing_date TEXT NULL,
                                                        latest_report_date TEXT NULL,
                                                        latest_accessionNumber TEXT NULL,
                                                        latest_form TEXT NULL
                                                        );''')
                for i in company_tickers_json:
                    if company_tickers_json[i]["ticker"] in set(tickers):
                        cursor.execute("INSERT INTO COMPANY (ranking, cik, ticker, name) VALUES (?, ?, ?, ?)", \
                                   (i, str(company_tickers_json[i]["cik_str"]).zfill(10), company_tickers_json[i]["ticker"], company_tickers_json[i]["title"]))
            conn.commit()
    except:       
        with closing(sqlite3.connect(company_ticker_json_db)) as conn:
            with closing(conn.cursor()) as cursor:
                for i in company_tickers_json:
                    cursor.execute("""
                                   INSERT OR IGNORE INTO COMPANY (ranking, cik, ticker, name) VALUES (?, ?, ?, ?)
                                   """, \
                                   (i, str(company_tickers_json[i]["cik_str"]).zfill(10), company_tickers_json[i]["ticker"], company_tickers_json[i]["title"]))
            conn.commit()
             
async def main(name, email, nodownload, tickers):
    if nodownload:
        slot = 0.001
    else:
        slot = 0.25
    limiter = AsyncLimiter(1, slot)
    tasks = []
    semaphore = asyncio.Semaphore(value=10)
    hdr = {'User-Agent' : name + email}
    await async_get_company_ticker_json(hdr, tickers, limiter, semaphore)
    company_ticker_json_db = os.path.join(os.path.join(os.environ['FINPYDATA'], "edgar", "files", "company_tickers.db"))
    r = {}
    num = 0

    for ticker in tickers:
        with closing(sqlite3.connect(company_ticker_json_db)) as conn:
            with closing(conn.cursor()) as cursor:
                row = cursor.execute("SELECT * FROM COMPANY WHERE ticker = '{}'".format(ticker)).fetchone()
                ticker_info = {}
                ticker_info['ranking'] = row[0]
                ticker_info['cik'] = row[1]
                ticker_info['ticker'] = row[2]
                ticker_info['name'] = row[3]
                ticker_info['sic'] = row[4]
                ticker_info['sicDescription'] = row[5]
                ticker_info['latest_filing_date'] = date.fromisoformat(row[6]) if isinstance(row[6], str) else row[6]
                ticker_info['latest_report_date'] = date.fromisoformat(row[7]) if isinstance(row[7], str) else row[7]
                ticker_info['latest_accessionNumber'] = row[8]
                ticker_info['latest_form'] = row[9]
                tasks.append(download.async_create(ticker_info, name, email, nodownload, True, limiter, semaphore, r))
    await asyncio.wait(tasks)
#    for i in r:
#        print(i)
    return r
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Name and Email.')
    parser.add_argument('-name', help='Name')
    parser.add_argument('-email', help='E-Mail address')
    parser.add_argument('-tick', help='ticker file list')
    parser.add_argument('-sp500', action="store_true", default=False, help="include all tickers in s&p 500")
    parser.add_argument('-nodownload', action="store_true", default=False, help="only update the database from the exisiting json files")
    args = parser.parse_args()
    tickers = custom(args.tick)
    if args.sp500:
        tickers += sp500();
        tickers = list(set(tickers))
    s = time.perf_counter()
    asyncio.run(main(args.name, args.email, args.nodownload,  tickers)) # Activate this line if the code is to be executed in VS Code
    # , etc. Otherwise deactivate it.
    # r = await main()          # Activate this line if the code is to be executed in Jupyter 
    # Notebook! Otherwise deactivate it.
    elapsed = time.perf_counter() - s
    print(f"Execution time: {elapsed:0.2f} seconds.")
