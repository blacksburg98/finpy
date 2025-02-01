from finpy.edgar.company import company
from finpy.utils.components import sp500
from finpy.utils.components import russel3000
from finpy.utils.components import custom
import argparse
import pandas as pd
import os
import sqlite3
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Name and Email.')
    parser.add_argument('-dir', default="app", help="app directory")
    parser.add_argument('-tick', help='ticker file list')
    parser.add_argument('-sp500', action="store_true", default=False, help="include all tickers in s&p 500")
    parser.add_argument('-russel3000', action="store_true", default=False, help="include all tickers in russel 3000")
    parser.add_argument('-header', default='{% extends "base.html" %}\n{% block content %}\n', help="header: html code before the table")
    parser.add_argument('-footer', default="{% endblock %}", help="footer: html code before the table")
    args = parser.parse_args()
    tickers = custom(args.tick)
    error_tickers = []
    r = []
    e = []
    if args.sp500:
        tickers += sp500();
    if args.russel3000:
        missing_file = os.path.join(args.dir, "missing.txt");
        r3k_missing = custom(missing_file)
        r3k = russel3000(format="pandas")
        r3k = r3k.drop(columns=['Notional Value', 'Asset Class', 'Location','Exchange','Currency','FX Rate','Market Currency','Accrual Date', 'Market Value', 'Shares'])
        r3k = r3k[~r3k['Ticker'].isin(r3k_missing)]
        tickers += list(r3k['Ticker'])
        r3k.insert(2, 'CJK', "")
    tickers = list(set(tickers))
    company_ticker_json_db = os.path.join(os.path.join(os.environ['FINPYDATA'], "edgar", "files", "company_tickers.db"))
    conn = sqlite3.connect(company_ticker_json_db)
    # print(tickers, len(tickers))
    for i in tickers:
        try:
            r.append(company(i, conn))
        except:
            error_tickers.append(i) 
            print("Error {}".format(i)) 
    conn.close()    
    data = []
    for i in r:
        latest_str = "<a href={}>{}</a>".format(i.latest_inline_xbrl, i.latest_form)
        cik_str = "<a href=https://data.sec.gov/api/xbrl/companyfacts/CIK{}.json>{}</a>".format(i.cik, i.cik)
        tick_str = "<a href={}_edgar.html>{}</a>".format(i.ticker, i.ticker)
        name_str = "<a href=https://www.sec.gov/edgar/browse/?CIK={}&owner=exclude>{}</a>".format(i.cik, i.name)
        r3k.loc[r3k.Ticker == i.ticker, 'CJK'] = cik_str 
        tick_file = os.path.join(args.dir, "{}.html".format(i.ticker))
        r3k.loc[r3k.Ticker == i.ticker, 'Name'] = name_str 
        if os.path.isfile(tick_file):
            r3k.loc[r3k.Ticker == i.ticker, 'Ticker'] = tick_str 
        df2 = {'Ranking' : i.ranking, 'Ticker' : tick_str, 'name' : name_str, 'cik' : cik_str, 'sic' : i.sic, 'sicDesciption' : i.sicDescription, 'latest': latest_str}
        data.append(df2)
    df = pd.DataFrame(data)   
    sics = set(df['sic'])
    for sic in sics:
        sic_file_name = os.path.join(args.dir, "sic_{}.html".format(sic))
        with open(sic_file_name, 'w') as sic_f:
            sic_f.write(args.header)
            sic_f.write(df.loc[df['sic'] == sic].sort_values(by=['Ranking']).to_html(escape=False,table_id="EdgarMain",index=False))    
            sic_f.write(args.footer)
        sic_str = "<a href=sic_{}.html>{}</a>".format(sic, sic)
        df.loc[df['sic'] == sic, 'sic'] = sic_str
    file_name = os.path.join(args.dir, 'edgar.html')
    with open(file_name , 'w') as f:
        f.write(args.header)
        f.write(df.sort_values(by=['Ranking']).to_html(escape=False,table_id="EdgarMain",index=False))    
        f.write(args.footer)
    if args.russel3000:
        russel3000_file = os.path.join(args.dir, "russel3000.html");
        with open(russel3000_file , 'w') as f:
            f.write(args.header)
            f.write(r3k.to_html(escape=False,table_id="Russel 3000",index=False))    
            f.write(args.footer)

