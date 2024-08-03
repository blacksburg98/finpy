"""
(c) 2013 Tsung-Han Yang
This source code is released under the Apache license.  
blacksburg98@yahoo.com
Created on April 1, 2013
Pulling Yahoo CSV Data
"""

import urllib.request, urllib.error, urllib.parse
import datetime
import os
import argparse
import yfinance as yf
import re

def get_data(data_path, ls_symbols, src="Yahoo"):

    # Create path if it doesn't exist
    if not (os.access(data_path, os.F_OK)):
        os.makedirs(data_path)

    # utils.clean_paths(data_path)   

    _now =datetime.datetime.now();
    miss_ctr=0; #Counts how many symbols we could not get
    for symbol in ls_symbols:
        print(symbol)
        # Preserve original symbol since it might
        # get manipulated if it starts with a "$"
        symbol_name = symbol
        if symbol[0] == '$':
            symbol = '^' + symbol[1:]

        symbol_data=list()
        dt_start = datetime.datetime(1986, 1, 1, 16)
        file = os.path.join(data_path, symbol_name + ".csv")
        actions_file = os.path.join(data_path, symbol_name + "_actions" + ".csv")
        month = dt_start.month - 1
        if src == "Google":
            try:
                sd = dt_start.strftime("%b %d, %Y")
                ed = _now.strftime("%b %d, %Y")
                params= urllib.parse.urlencode ({'q': symbol,'startdate':sd,'enddate':ed,'output':'csv'})
                url = "http://www.google.com/finance/historical?%s" % params
                header = url_get.readline()
                f.write(header[3:].decode("utf-8"))
                lines = url_get.readlines()
                for l in lines:
                    ls = l.decode("utf-8")[:-2]
                    c = ls.split(',')
                    dt = datetime.datetime.strptime(c[0], "%d-%b-%y")
                    c[0] = dt.strftime("%Y-%m-%d")
                    f.write(','.join(c) + "\n")
            except urllib.error.HTTPError:
                miss_ctr += 1
                print("Unable to fetch data for stock: {0} at {1}".format(symbol_name, url))
            except urllib.error.URLError:
                miss_ctr += 1
                print("URL Error for stock: {0} at {1}".format(symbol_name, url))
        elif src == "Yahoo":
            try:
                data = yf.download(symbol, start="2006-01-01", end=_now.strftime("%Y-%m-%d"))
                data.to_csv(file)
                actions = yf.Ticker(symbol).actions
                actions.to_csv(actions_file)
            except:
                miss_ctr += 1
                print("Unable to fetch data for stock: {0}".format(symbol_name))
            try:    
                stock = data.getHistorical()
            except:
                miss_ctr += 1
                print("Unable to fetch data for stock: {0}".format(symbol_name))
                        
            
    print("All done. Got {0} stocks. Could not get {1}".format(len(ls_symbols) - miss_ctr, miss_ctr))

def latest_local_dt(data_path, symbol_name):
    file_path = os.path.join(data_path, symbol_name + ".csv")
    with open(file_path, "rb") as f:
        f.seek(-2, os.SEEK_END)     # Jump to the second last byte.
        while f.read(1) != b"\n":   # Until EOL is found...
            f.seek(-2, os.SEEK_CUR) # ...jump back the read byte plus one more.
        lastline = f.readline().decode("utf-8")         # Read last line.
        dt_str = lastline.split(',')[0]
        dt_re = re.compile("\d\d\d\d-\d\d-\d\d")
        if dt_re.match(dt_str): 
            dt = datetime.datetime.strptime(dt_str, "%Y-%m-%d")
        else:    
            dt = datetime.datetime.strptime("1950-01-01", "%Y-%m-%d")
        return dt

def read_symbols(s_symbols_file):

    ls_symbols=[]
    file = open(s_symbols_file, 'r')
    for line in file.readlines():
        str_line = str(line)
        if str_line.strip(): 
            ls_symbols.append(str_line.strip())
    file.close()
    
    return ls_symbols  

if __name__ == '__main__':
    parser = argparse.ArgumentParser( description='Yahoo Stock Data Pull.')
    parser.add_argument('-ticks', default="symbols.txt", help="symbols file. This contains a ticker per line")
    args = parser.parse_args()
    path = './'
    ls_symbols = read_symbols(args.ticks)
    get_data(path, ls_symbols)
