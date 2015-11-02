"""
(c) 2013 Tsung-Han Yang
This source code is released under the Apache license.  
blacksburg98@yahoo.com
Created on April 1, 2013
Pulling Yahoo CSV Data
"""

import urllib.request, urllib.error, urllib.parse
import urllib.request, urllib.parse, urllib.error
import datetime
import os
import csv
import argparse
from BeautifulSoup import BeautifulSoup

def get_data(data_path, ls_symbols):

    # Create path if it doesn't exist
    if not (os.access(data_path, os.F_OK)):
        os.makedirs(data_path)

    for s in ls_symbols:
        print((s["exchange"], s["sym"])) 
        if not (os.access(s["exchange"], os.F_OK)):
            os.makedirs(s["exchange"])
        symbol_data = []
        params= urllib.parse.urlencode ({'exchange':s["exchange"], 'symbol':s["sym"], 'depth': '1'})
        url = "http://www.ofdp.org/continuous_contracts/data?%s" % params
        url_get= urllib.request.urlopen(url)
        html = url_get.read()
        page = BeautifulSoup(html)
        table = page.find('table')
        head = table.find('tr')
        th_row = []
        for th in head.findAll('th'):
            th_row.append(th.find(text=True))
        csv_rows = []
        csv_rows.append(th_row)
        rows = table.findAll('tr')
        rows.pop(0)
        for tr in rows:
            csv_row = []
            cols = tr.findAll('td')
            for td in cols:
                csv_row.append(td.find(text=True))
            csv_rows.append(csv_row)
        csv_file = s["exchange"] + "/" + s["sym"] + ".csv"
        with open(csv_file, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(csv_rows)    


def read_symbols(s_symbols_file):

    ls_symbols=[]
    with open(s_symbols_file, 'rb') as csvfile:
        symreader = csv.reader(csvfile, delimiter='/', quotechar='#')
        for row in symreader:
            sym = {}
            sym["exchange"] = row[0]
            sym["sym"] = row[1]
            ls_symbols.append(sym)
    
    return ls_symbols  

if __name__ == '__main__':
    parser = argparse.ArgumentParser( description='Open Financial Data Project Data Pull.')
    parser.add_argument('-sym', default="symbols.txt", help="symbols file. This contains a ticker per line")
    parser.add_argument('-path', default=".", help="symbols file. This contains a ticker per line")
    args = parser.parse_args()
    path = args.path
    ls_symbols = read_symbols(args.sym)
    get_data(path, ls_symbols)
