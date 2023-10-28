from finpy.edgar.company import company
from finpy.utils.components import sp500
from finpy.utils.components import custom
import argparse
import pandas as pd
import os
import sqlite3
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Name and Email.')
    parser.add_argument('-dir', default="app", help="app directory")
    parser.add_argument('-subdir', default="current", help="the subdir under app directory")
    parser.add_argument('-tick', help='ticker file list')
    parser.add_argument('-sp500', action="store_true", default=False, help="include all tickers in s&p 500")
    args = parser.parse_args()
    tickers = custom(args.tick)
    r = []
    e = []
    if args.sp500:
        tickers += sp500();
        tickers = list(set(tickers))
    company_ticker_json_db = os.path.join(os.path.join(os.environ['FINPYDATA'], "edgar", "files", "company_tickers.db"))
    conn = sqlite3.connect(company_ticker_json_db)
    # print(tickers, len(tickers))
    for i in tickers:
        print(i)
        r.append(company(i, conn))
    conn.close()    
    data = []    
    for i in r:
        latest_str = "<a href={}>{}</a>".format(i.latest_filing_url, i.latest_form)
        cik_str = "<a href=https://data.sec.gov/api/xbrl/companyfacts/CIK{}.json>{}</a>".format(i.cik, i.cik)
        df2 = {'Ranking' : i.ranking, 'Ticker' : i.ticker, 'name' : i.name, 'cik' : cik_str, 'sic' : i.sic, 'sicDesciption' : i.sicDescription, 'latest': latest_str}
        data.append(df2)
    df = pd.DataFrame(data)   
    print(df)    
    file_name = os.path.join(args.dir, 'templates', args.subdir, 'edgar.html')
    with open(file_name , 'w') as f:
        f.write('{% extends "base.html" %}\n{% block content %}\n')
        f.write(df.to_html(escape=False,table_id="EdgarMain",index=False))    
        f.write("{% endblock %}")
