"""
(c) 2013 Tsung-Han Yang
This source code is released under the Apache license.  
blacksburg98@yahoo.com
Created on April 1, 2013
Pulling Yahoo CSV Data
"""

import urllib2
import urllib
import datetime
import os
import argparse

def get_data(data_path, ls_symbols):

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
        # print "Getting {0}".format(symbol)
        dt_start=datetime.datetime(1986, 1, 1, 16)
        try:
            month = dt_start.month - 1
            params= urllib.urlencode ({'a':month, 'b':dt_start.day, 'c':dt_start.year, 'd':_now.month, 'e':_now.day, 'f':_now.year, 's': symbol})
            # Fix for ubuntu. For some reasons, ubuntu put %0D at the end
            if params[-3:] == "%0D":
                params = params[:-3]
            url = "http://ichart.finance.yahoo.com/table.csv?%s" % params
            url_get= urllib2.urlopen(url)
            
            header= url_get.readline()
            symbol_data.append (url_get.readline())
            while (len(symbol_data[-1]) > 0):
                symbol_data.append(url_get.readline())
            
            symbol_data.pop(-1) #The last element is going to be the string of length zero. We don't want to write that to file.
            #now writing data to file
            f= open (data_path + symbol_name + ".csv", 'w')
            
            #Writing the header
            f.write (header)
            
            while (len(symbol_data) > 0):
                url_line = symbol_data.pop(0)
                f.write (url_line)
               # print url_line
            f.close();    
                        
        except urllib2.HTTPError:
            miss_ctr += 1
            print "Unable to fetch data for stock: {0} at {1}".format(symbol_name, url)
        except urllib2.URLError:
            miss_ctr += 1
            print "URL Error for stock: {0} at {1}".format(symbol_name, url)
            
    print "All done. Got {0} stocks. Could not get {1}".format(len(ls_symbols) - miss_ctr, miss_ctr)

def latest_local(file_path):
    with open(file_path) as f:
        f.next()
        topline = f.next()
        dt_str = topline.split(',')[0]
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
