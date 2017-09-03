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
from finpy.data.fetch import Fetcher

def get_data(data_path, ls_symbols, src="Yahoo"):

    # Create path if it doesn't exist
    if not (os.access(data_path, os.F_OK)):
        os.makedirs(data_path)

    # utils.clean_paths(data_path)   

    _now =datetime.datetime.now();
    miss_ctr=0; #Counts how many symbols we could not get
    for symbol in ls_symbols:
        # Preserve original symbol since it might
        # get manipulated if it starts with a "$"
        symbol_name = symbol
        if symbol[0] == '$':
            symbol = '^' + symbol[1:]

        symbol_data=list()
        # print "Getting {0}".format(symbol)`
        dt_start=datetime.datetime(1986, 1, 1, 16)
        file = os.path.join(data_path, symbol_name + ".csv")
        f = open (file, 'w')
        try:
            month = dt_start.month - 1
            if src == "Google":
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
            elif src == "Yahoo":
                data = Fetcher(symbol, [1986,1,1], [_now.year,_now.month,_now.day])    
                stock = data.getHistorical()
                stock.to_csv(f, index=False, columns=['Date','Open','High','Low','Close','Volume','Adj Close'])
            f.close();    
                        
        except urllib.error.HTTPError:
            miss_ctr += 1
            print("Unable to fetch data for stock: {0} at {1}".format(symbol_name, url))
        except urllib.error.URLError:
            miss_ctr += 1
            print("URL Error for stock: {0} at {1}".format(symbol_name, url))
            
    print("All done. Got {0} stocks. Could not get {1}".format(len(ls_symbols) - miss_ctr, miss_ctr))

def latest_local_dt(data_path, symbol_name):
    file_path = os.path.join(data_path, symbol_name + ".csv")
    with open(file_path, "rb") as f:
        f.seek(-2, os.SEEK_END)     # Jump to the second last byte.
        while f.read(1) != b"\n":   # Until EOL is found...
            f.seek(-2, os.SEEK_CUR) # ...jump back the read byte plus one more.
        lastline = f.readline().decode("utf-8")         # Read last line.
        dt_str = lastline.split(',')[0]
        dt = datetime.datetime.strptime(dt_str, "%Y-%m-%d")
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
