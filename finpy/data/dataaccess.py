'''
(c) 2011, 2012 Georgia Tech Research Corporation
This source code is released under the New BSD license.  Please see
http://wiki.quantsoftware.org/index.php?title=QSTK_License
for license details.

Created on Jan 15, 2013

@author: Sourabh Bajaj
@contact: sourabhbajaj@gatech.edu
@summary: Data Access python library.

Modified by Tsung-Han Yang.

'''

import numpy as np
import pandas as pd
import os
import re
import csv
import pickle as pkl
import time
import datetime as dt
import tempfile
from . import DataPull 


class Exchange (object):
    AMEX = 1
    NYSE = 2
    NYSE_ARCA = 3
    OTC = 4
    DELISTED = 5
    NASDAQ = 6


class DataItem (object):
    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"
    VOL = "volume"
    VOLUME = "volume"
    ACTUAL_CLOSE = "actual_close"
    ADJUSTED_CLOSE = "adj_close"

class DataSource(object):
    NORGATE = "Norgate"
    YAHOO = "Yahoo"
    GOOGLE = "Google"
    YAHOOold = "YahooOld"
    CUSTOM = "Custom"
    MLT = "ML4Trading"
    #class DataSource ends


class DataAccess(object):
    '''
    @summary: This class is used to access all the symbol data. It readin in pickled numpy arrays converts them into appropriate pandas objects
    and returns that object. The {main} function currently demonstrates use.
    @note: The earliest time for which this works is platform dependent because the python date functionality is platform dependent.
    '''
    def __init__(self, sourcein=DataSource.YAHOO, s_datapath=None,
                 s_scratchpath=None, cachestalltime=12):
        '''
        @param sourcestr: Specifies the source of the data. Initializes paths based on source.
        @note: No data is actually read in the constructor. Only paths for the source are initialized
        @param: Scratch defaults to a directory in /tmp/QSScratch
        '''
        self.folderList = []
        try:
            self.rootdir = os.environ['FINPYDATA']
            try:
                self.scratchdir = os.environ['FINPYSCRATCH']
            except:
                self.scratchdir = os.path.join(tempfile.gettempdir(), 'finpyscratch')
        except:
            if s_datapath != None:
                self.rootdir = s_datapath
                if s_scratchpath != None:
                    self.scratchdir = s_scratchpath
                else:
                    self.scratchdir = os.path.join(tempfile.gettempdir(), 'QSScratch')
            else:
                self.rootdir = os.path.join(os.path.dirname(__file__), 'finpy_data')
                self.scratchdir = os.path.join(tempfile.gettempdir(), 'QSScratch')

        # print(self.rootdir)
        if not os.path.isdir(self.rootdir):
            print("Data path provided is invalid")
            raise

        if not os.path.exists(self.scratchdir):
            os.mkdir(self.scratchdir)

        if (sourcein == DataSource.YAHOO):
            self.source = DataSource.YAHOO
            self.folderList.append(self.rootdir + "/Yahoo/")
            self.fileExtensionToRemove = ".csv"
        elif (sourcein == DataSource.GOOGLE):
            self.source = DataSource.GOOGLE
            self.folderList.append(self.rootdir + "/Google/")
            self.fileExtensionToRemove = ".csv"

        else:
            raise ValueError("Incorrect data source requested.")

        #__init__ ends

    def get_data_hardread(self, ts_list, symbol_list, data_item, verbose=False):
        '''
        Read data into a DataFrame no matter what.
        @param ts_list: List of timestamps for which the data values are needed. Timestamps must be sorted.
        @param symbol_list: The list of symbols for which the data values are needed
        @param data_item: The data_item needed. Like open, close, volume etc.  May be a list, in which case a list of DataFrame is returned.
        @note: If a symbol is not found then a message is printed. All the values in the column for that stock will be NaN. Execution then
        continues as usual. No errors are raised at the moment.
        '''
        #read in data for a stock
        latest_req_dt = ts_list[-1]
        ldmReturn = []
        data_path = os.path.join(self.rootdir, "Yahoo")
        for symbol in symbol_list:
            try:
                file_path = os.path.join(self.rootdir, "Yahoo",  symbol + ".csv")
                dir_name = os.path.dirname(file_path) + os.sep
                data_update = False
                if not os.path.isfile(file_path):
                    data_update = True
                elif os.stat(file_path).st_size <= 6:
                    data_update = True
                else:
                    latest_local_dt = DataPull.latest_local_dt(data_path, symbol)
                    if latest_local_dt < latest_req_dt: 
                        data_update = True
                if data_update:    
                    DataPull.get_data(dir_name, [symbol])
                else:
                    print("Do not pull. Use local file for " + symbol)
                
            except IOError:
                # If unable to read then continue. The value for this stock will be nan
                print("Error:" + symbol)
                continue;
                
            a = pd.DataFrame(index=ts_list)
            b = pd.read_csv(file_path, index_col='Date',parse_dates=True,na_values='null')
            b.columns = ['open', 'high', 'low', 'actual_close', 'volume', 'close']
            del b.index.name
            a = pd.concat([a, b], axis=1, join_axes=[a.index])
            a = a[data_item]
            ldmReturn.append(a)
                
        return ldmReturn            
        
        #get_data_hardread ends

    def get_data (self, ts_list, symbol_list, data_item, verbose=False, bIncDelist=False):
        '''
        Read data into a DataFrame, but check to see if it is in a cache first.
        @param ts_list: List of timestamps for which the data values are needed. Timestamps must be sorted.
        @param symbol_list: The list of symbols for which the data values are needed
        @param data_item: The data_item needed. Like open, close, volume etc.  May be a list, in which case a list of DataFrame is returned.
        @param bIncDelist: If true, delisted securities will be included.
        @note: If a symbol is not found then a message is printed. All the values in the column for that stock will be NaN. Execution then 
        continues as usual. No errors are raised at the moment.
        '''
        retval = self.get_data_hardread(ts_list, symbol_list, data_item, verbose)
        return retval

    def getPathOfFile(self, symbol_name, bDelisted=False):
        '''
        @summary: Since a given pkl file can exist in any of the folders- we need to look for it in each one until we find it. Thats what this function does.
        @return: Complete path to the pkl file including the file name and extension
        '''

        if not bDelisted:
            for path1 in self.folderList:
                if (os.path.exists(str(path1) + str(symbol_name + ".pkl"))):
                    # Yay! We found it!
                    return (str(str(path1) + str(symbol_name) + ".pkl"))
                    #if ends
                elif (os.path.exists(str(path1) + str(symbol_name + ".csv"))):
                    # Yay! We found it!
                    return (str(str(path1) + str(symbol_name) + ".csv"))
                #for ends

        else:
            ''' Special case for delisted securities '''
            lsPaths = []
            for sPath in self.folderList:
                if re.search('Delisted Securities', sPath) == None:
                    continue

                for sFile in os.listdir(sPath):
                    if not re.match( '%s-\d*.pkl'%symbol_name, sFile ) == None:
                        lsPaths.append(sPath + sFile)

            lsPaths.sort()
            return lsPaths

        print("Did not find path to " + str(symbol_name) + ". Looks like this file is missing")

    def getPathOfCSVFile(self, symbol_name):
        for path1 in self.folderList:
            if (os.path.exists(str(path1)+str(symbol_name+".csv"))):
                # Yay! We found it!
                return (str(str(path1)+str(symbol_name)+".csv"))
            #if ends
        #for ends
        print("Did not find path to " + str (symbol_name)+". Looks like this file is missing")    

    def get_all_symbols (self):
        '''
        @summary: Returns a list of all the symbols located at any of the paths for this source. @see: {__init__}
        @attention: This will discard all files that are not of type pkl. ie. Only the files with an extension pkl will be reported.
        '''

        listOfStocks = list()
        #Path does not exist

        if (len(self.folderList) == 0):
            raise ValueError("DataAccess source not set")

        for path in self.folderList:
            stocksAtThisPath = list()
            #print str(path)
            stocksAtThisPath = os.listdir(str(path))
            #Next, throw away everything that is not a .pkl And these are our stocks!
            stocksAtThisPath = [x for x in stocksAtThisPath if (str(x).find(str(self.fileExtensionToRemove)) > -1)]
            #Now, we remove the .pkl to get the name of the stock
            stocksAtThisPath = [(x.partition(str(self.fileExtensionToRemove))[0]) for x in stocksAtThisPath]

            listOfStocks.extend(stocksAtThisPath)
            #for stock in stocksAtThisPath:
                #listOfStocks.append(stock)
        return listOfStocks
        #get_all_symbols ends

    def get_symbols_from_list(self, s_list):
        ''' Reads all symbols from a list '''
        ls_symbols = []
        if (len(self.folderList) == 0):
            raise ValueError("DataAccess source not set")

        for path in self.folderList:
            path_to_look = path + 'Lists/' + s_list + '.txt'
            ffile = open(path_to_look, 'r')
            for f in ffile.readlines():
                j = f[:-1]
                ls_symbols.append(j)
            ffile.close()

        return ls_symbols

    def get_symbols_in_sublist (self, subdir):
        '''
        @summary: Returns all the symbols belonging to that subdir of the data store.
        @param subdir: Specifies which subdir you want.
        @return: A list of symbols belonging to that subdir
        '''

        pathtolook = self.rootdir + self.midPath + subdir
        stocksAtThisPath = os.listdir(pathtolook)

        #Next, throw away everything that is not a .pkl And these are our stocks!
        try:
            stocksAtThisPath = [x for x in stocksAtThisPath if (str(x).find(str(self.fileExtensionToRemove)) > -1)]
            #Now, we remove the .pkl to get the name of the stock
            stocksAtThisPath = [(x.partition(str(self.fileExtensionToRemove))[0]) for x in stocksAtThisPath]
        except:
            print("error: no path to " + subdir)
            stocksAtThisPath = list()

        return stocksAtThisPath
        #get_all_symbols_on_exchange ends

    def get_sublists(self):
        '''
        @summary: Returns a list of all the sublists for a data store.
        @return: A list of the valid sublists for the data store.
        '''

        return self.folderSubList
        #get_sublists

    def get_data_labels(self):
        '''
        @summary: Returns a list of all the data labels available for this type of data access object.
        @return: A list of label strings.
        '''

        print('Function only valid for Compustat objects!')
        return []

    def get_info(self):
        '''
        @summary: Returns and prints a string that describes the datastore.
        @return: A string.
        '''

        if (self.source == DataSource.NORGATE):
            retstr = "Norgate:\n"
            retstr = retstr + "Daily price and volume data from Norgate (premiumdata.net)\n"
            retstr = retstr + "that is valid at the time of NYSE close each trading day.\n"
            retstr = retstr + "\n"
            retstr = retstr + "Valid data items include: \n"
            retstr = retstr + "\topen, high, low, close, volume, actual_close\n"
            retstr = retstr + "\n"
            retstr = retstr + "Valid subdirs include: \n"
            for i in self.folderSubList:
                retstr = retstr + "\t" + i + "\n"
        elif (self.source == DataSource.YAHOO):
            retstr = "Yahoo:\n"
            retstr = retstr + "Attempts to load a custom data set, assuming each stock has\n"
            retstr = retstr + "a csv file with the name and first column as the stock ticker,\ date in second column, and data in following columns.\n"
            retstr = retstr + "everything should be located in QSDATA/Yahoo\n"
            for i in self.folderSubList:
                retstr = retstr + "\t" + i + "\n"
        elif (self.source == DataSource.GOOGLE):
            retstr = "Google:\n"
            retstr = retstr + "Attempts to load a custom data set, assuming each stock has\n"
            retstr = retstr + "a csv file with the name and first column as the stock ticker,\ date in second column, and data in following columns.\n"
            retstr = retstr + "everything should be located in QSDATA/Google\n"
            for i in self.folderSubList:
                retstr = retstr + "\t" + i + "\n"
        elif (self.source == DataSource.CUSTOM):
            retstr = "Custom:\n"
            retstr = retstr + "Attempts to load a custom data set, assuming each stock has\n"
            retstr = retstr + "a csv file with the name and first column as the stock ticker, date in second column, and data in following columns.\n"
            retstr = retstr + "everything should be located in QSDATA/Processed/Custom\n"
        elif (self.source == DataSource.MLT):
            retstr = "ML4Trading:\n"
            retstr = retstr + "Attempts to load a custom data set, assuming each stock has\n"
            retstr = retstr + "a csv file with the name and first column as the stock ticker,\ date in second column, and data in following columns.\n"
            retstr = retstr + "everything should be located in QSDATA/Processed/ML4Trading\n"
        else:
            retstr = "DataAccess internal error\n"

        print(retstr)
        return retstr
        #get_sublists


    #class DataAccess ends
