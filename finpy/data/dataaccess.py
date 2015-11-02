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
import pandas as pa
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

        self.folderList = list()
        self.folderSubList = list()
        self.cachestalltime = cachestalltime
        self.fileExtensionToRemove = ".pkl"

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

#        print "Scratch Directory: ", self.scratchdir
#        print "Data Directory: ", self.rootdir

        if not os.path.isdir(self.rootdir):
            print("Data path provided is invalid")
            raise

        if not os.path.exists(self.scratchdir):
            os.mkdir(self.scratchdir)

        if (sourcein == DataSource.NORGATE):

            self.source = DataSource.NORGATE
            self.midPath = "/Processed/Norgate/Stocks/"

            self.folderSubList.append("/US/AMEX/")
            self.folderSubList.append("/US/NASDAQ/")
            self.folderSubList.append("/US/NYSE/")
            self.folderSubList.append("/US/NYSE Arca/")
            self.folderSubList.append("/US/OTC/")
            self.folderSubList.append("/US/Delisted Securities/")
            self.folderSubList.append("/US/Indices/")

            for i in self.folderSubList:
                self.folderList.append(self.rootdir + self.midPath + i)
        elif (sourcein == DataSource.CUSTOM):
            self.source = DataSource.CUSTOM
            self.folderList.append(self.rootdir + "/Processed/Custom/")
        elif (sourcein == DataSource.MLT):
            self.source = DataSource.MLT
            self.folderList.append(self.rootdir + "/ML4Trading/")
        elif (sourcein == DataSource.YAHOO):
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

    def get_data_hardread(self, ts_list, symbol_list, data_item, verbose=False, bIncDelist=False):
        '''
        Read data into a DataFrame no matter what.
        @param ts_list: List of timestamps for which the data values are needed. Timestamps must be sorted.
        @param symbol_list: The list of symbols for which the data values are needed
        @param data_item: The data_item needed. Like open, close, volume etc.  May be a list, in which case a list of DataFrame is returned.
        @param bIncDelist: If true, delisted securities will be included.
        @note: If a symbol is not found then a message is printed. All the values in the column for that stock will be NaN. Execution then
        continues as usual. No errors are raised at the moment.
        '''

        ''' Now support lists of items, still support old string behaviour '''
        bStr = False
        if( isinstance( data_item, str) ):
            data_item = [data_item]
            bStr = True

        # init data struct - list of arrays, each member is an array corresponding do a different data type
        # arrays contain n rows for the timestamps and m columns for each stock
        all_stocks_data = []
        for i in range( len(symbol_list) ):
            all_stocks_data.append( np.zeros ((len(ts_list), len(data_item))) );
            all_stocks_data[i][:][:] = np.NAN
        
        list_index= []
        
        ''' For each item in the list, add to list_index (later used to delete non-used items) '''
        for sItem in data_item:
            if( self.source == DataSource.CUSTOM ) :
                ''' If custom just load what you can '''
                if (sItem == DataItem.CLOSE):
                    list_index.append(1)
                elif (sItem == DataItem.ACTUAL_CLOSE):
                    list_index.append(2)
            if( self.source == DataSource.NORGATE ):
                if (sItem == DataItem.OPEN):
                    list_index.append(1)
                elif (sItem == DataItem.HIGH):
                    list_index.append (2)
                elif (sItem ==DataItem.LOW):
                    list_index.append(3)
                elif (sItem == DataItem.CLOSE):
                    list_index.append(4)
                elif(sItem == DataItem.VOL):
                    list_index.append(5)
                elif (sItem == DataItem.ACTUAL_CLOSE):
                    list_index.append(6)
                else:
                    #incorrect value
                    raise ValueError ("Incorrect value for data_item %s"%sItem)

            if( self.source == DataSource.MLT or self.source == DataSource.YAHOO):
                if (sItem == DataItem.OPEN):
                    list_index.append(1)
                elif (sItem == DataItem.HIGH):
                    list_index.append (2)
                elif (sItem ==DataItem.LOW):
                    list_index.append(3)
                elif (sItem == DataItem.ACTUAL_CLOSE):
                    list_index.append(4)
                elif(sItem == DataItem.VOL):
                    list_index.append(5)
                elif (sItem == DataItem.CLOSE):
                    list_index.append(6)
                else:
                    #incorrect value
                    raise ValueError ("Incorrect value for data_item %s"%sItem)
            if self.source == DataSource.GOOGLE:
                if (sItem == DataItem.OPEN):
                    list_index.append(1)
                elif (sItem == DataItem.HIGH):
                    list_index.append (2)
                elif (sItem ==DataItem.LOW):
                    list_index.append(3)
                elif(sItem == DataItem.CLOSE):
                    list_index.append(4)
                elif (sItem == DataItem.VOL):
                    list_index.append(5)
                else:
                    #incorrect value
                    raise ValueError ("Incorrect value for data_item %s"%sItem)
                #end elif
        #end data_item loop

        #read in data for a stock
        symbol_ctr=-1
        for symbol in symbol_list:
            _file = None
            symbol_ctr = symbol_ctr + 1
            #print self.getPathOfFile(symbol)
            try:
                if (self.source == DataSource.CUSTOM) or (self.source == DataSource.MLT)or (self.source == DataSource.YAHOO):
                    file_path= self.getPathOfCSVFile(symbol);
                    if file_path != None:
                        dt_latest = DataPull.latest_local(file_path)
                    else:
                        file_path = self.rootdir + "/Yahoo/" + symbol + ".csv"
                        dt_latest = dt.datetime.strptime("1900-1-1", "%Y-%m-%d")
                    dir_name = os.path.dirname(file_path) + os.sep
                    if dt_latest < ts_list[-1]:
                        DataPull.get_data(dir_name, [symbol])
                elif self.source == DataSource.GOOGLE:
                    file_path= self.getPathOfCSVFile(symbol);
                    if file_path != None:
                        dt_latest = DataPull.latest_local(file_path)
                    else:
                        file_path = self.rootdir + "/Google/" + symbol + ".csv"
                        dt_latest = dt.datetime.strptime("1900-1-1", "%Y-%m-%d")
                    dir_name = os.path.dirname(file_path) + os.sep
                    if dt_latest < ts_list[-1]:
                        DataPull.get_data(dir_name, [symbol], src="Google")
                else:
                    file_path= self.getPathOfFile(symbol);
                
                ''' Get list of other files if we also want to include delisted '''
                if bIncDelist:
                    lsDelPaths = self.getPathOfFile( symbol, True )
                    if file_path == None and len(lsDelPaths) > 0:
                        print('Found delisted paths:', lsDelPaths)
                
                ''' If we don't have a file path continue... unless we have delisted paths '''
                if (type (file_path) != type ("random string")):
                    if bIncDelist == False or len(lsDelPaths) == 0:
                        continue; #File not found
                
                if not file_path == None: 
                    _file = open(file_path, "rt", encoding="UTF-8")
            except IOError:
                # If unable to read then continue. The value for this stock will be nan
                print(_file)
                continue;
                
            assert( not _file == None or bIncDelist == True )
            ''' Open the file only if we have a valid name, otherwise we need delisted data '''
            if _file != None:
                if (self.source==DataSource.CUSTOM) or (self.source==DataSource.YAHOO)or (self.source==DataSource.MLT) or (self.source == DataSource.GOOGLE):
                    creader = csv.reader(_file)
                    row=next(creader)
                    row=next(creader)
                    #row.pop(0)
                    for i, item in enumerate(row):
                        if i==0:
                            try:
                                date = dt.datetime.strptime(item, '%Y-%m-%d')
                                date = date.strftime('%Y%m%d')
                                row[i] = float(date)
                            except:
                                date = dt.datetime.strptime(item, '%m/%d/%y')
                                date = date.strftime('%Y%m%d')
                                row[i] = float(date)
                        else:
                            row[i]=float(item)
                    naData=np.array(row)
                    for row in creader:
                        for i, item in enumerate(row):
                            if i==0:
                                try:
                                    date = dt.datetime.strptime(item, '%Y-%m-%d')
                                    date = date.strftime('%Y%m%d')
                                    row[i] = float(date)
                                except:
                                    date = dt.datetime.strptime(item, '%m/%d/%y')
                                    date = date.strftime('%Y%m%d')
                                    row[i] = float(date)
                            else: 
                                row[i]=float(item)
                        naData=np.vstack([np.array(row),naData])
                else:
                    naData = pkl.load (_file)
                _file.close()
            else:
                naData = None
                
            ''' If we have delisted data, prepend to the current data '''
            if bIncDelist == True and len(lsDelPaths) > 0 and naData == None:
                for sFile in lsDelPaths[-1:]:
                    ''' Changed to only use NEWEST data since sometimes there is overlap (JAVA) '''
                    inFile = open( sFile, "rb" )
                    naPrepend = pkl.load( inFile )
                    inFile.close()
                    
                    if naData == None:
                        naData = naPrepend
                    else:
                        naData = np.vstack( (naPrepend, naData) )
                        
            #now remove all the columns except the timestamps and one data column
            if verbose:
                print(self.getPathOfFile(symbol))
            
            ''' Fix 1 row case by reshaping '''
            if( naData.ndim == 1 ):
                naData = naData.reshape(1,-1)
                
            #print naData
            #print list_index
            ''' We open the file once, for each data item we need, fill out the array in all_stocks_data '''
            for lLabelNum, lLabelIndex in enumerate(list_index):
                
                ts_ctr = 0
                b_skip = True
                
                ''' select timestamps and the data column we want '''
                temp_np = naData[:,(0,lLabelIndex)]
                
                #print temp_np
                
                num_rows= temp_np.shape[0]

                
                symbol_ts_list = list(range(num_rows)) # preallocate
                for i in range (0, num_rows):

                    timebase = temp_np[i][0]
                    timeyear = int(timebase/10000)
                    
                    # Quick hack to skip most of the data
                    # Note if we skip ALL the data, we still need to calculate
                    # last time, so we know nothing is valid later in the code
                    if timeyear < ts_list[0].year and i != num_rows - 1:
                        continue
                    elif b_skip == True:
                        ts_ctr = i
                        b_skip = False
                    
                    
                    timemonth = int((timebase-timeyear*10000)/100)
                    timeday = int((timebase-timeyear*10000-timemonth*100))
                    timehour = 16
    
                    #The earliest time it can generate a time for is platform dependent
                    symbol_ts_list[i]=dt.datetime(timeyear,timemonth,timeday,timehour) # To make the time 1600 hrs on the day previous to this midnight
                    
                #for ends
    
    
                #now we have only timestamps and one data column
                
                
                #Skip data from file which is before the first timestamp in ts_list
    
                while (ts_ctr < temp_np.shape[0]) and (symbol_ts_list[ts_ctr] < ts_list[0]):
                    ts_ctr=  ts_ctr+1
                    
                    #print "skipping initial data"
                    #while ends
                
                for time_stamp in ts_list:
                    
                    if (symbol_ts_list[-1] < time_stamp):
                        #The timestamp is after the last timestamp for which we have data. So we give up. Note that we don't have to fill in NaNs because that is 
                        #the default value.
                        break;
                    else:
                        while ((ts_ctr < temp_np.shape[0]) and (symbol_ts_list[ts_ctr]< time_stamp)):
                            ts_ctr = ts_ctr+1
                            #while ends
                        #else ends
                                            
                    #print "at time_stamp: " + str(time_stamp) + " and symbol_ts "  + str(symbol_ts_list[ts_ctr])
                    
                    if (time_stamp == symbol_ts_list[ts_ctr]):
                        #Data is present for this timestamp. So add to numpy array.
                        #print "    adding to numpy array"
                        if (temp_np.ndim > 1): #This if is needed because if a stock has data for 1 day only then the numpy array is 1-D rather than 2-D
                            all_stocks_data[symbol_ctr][ts_list.index(time_stamp)][lLabelNum] = temp_np [ts_ctr][1]
                        else:
                            all_stocks_data[symbol_ctr][ts_list.index(time_stamp)][lLabelNum] = temp_np [1]
                        #if ends
                        
                        ts_ctr = ts_ctr +1
                    
                #inner for ends
            #outer for ends
        #print all_stocks_data
        
        ldmReturn = [] # List of data matrixes to return
        for naDataLabel in all_stocks_data:
            ldmReturn.append( pa.DataFrame( naDataLabel, ts_list, data_item) )            

        
        ''' Contine to support single return type as a non-list '''
        if bStr:
            return ldmReturn[0]
        else:
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

        # Construct hash -- filename where data may be already
        #
        # The idea here is to create a filename from the arguments provided.
        # We then check to see if the filename exists already, meaning that
        # the data has already been created and we can just read that file.

        # Create the hash for the symbols
        hashsyms = 0
        for i in symbol_list:
            hashsyms = (hashsyms + hash(i)) % 10000000

        # Create the hash for the timestamps
        hashts = 0

        for i in ts_list:
            hashts = (hashts + hash(i)) % 10000000
        hashstr = 'qstk-' + str (self.source)+'-' +str(abs(hashsyms)) + '-' + str(abs(hashts)) \
            + '-' + str(hash(str(data_item))) #  + '-' + str(hash(str(os.path.getctime(spyfile))))
        cachefilename = self.scratchdir + '/' + hashstr + '.pkl'
        if verbose:
            print("cachefilename is: " + cachefilename)
        # now eather read the pkl file, or do a hardread
        readfile = False  # indicate that we have not yet read the file

        #check if the cachestall variable is defined.
        # try:
        #     catchstall=dt.timedelta(hours=int(os.environ['CACHESTALLTIME']))
        # except:
        #     catchstall=dt.timedelta(hours=1)
        cachestall = dt.timedelta(hours=self.cachestalltime)

        # Check if the file is older than the cachestalltime
        if os.path.exists(cachefilename):
            if ((dt.datetime.now() - dt.datetime.fromtimestamp(os.path.getmtime(cachefilename))) < cachestall):
                if verbose:
                    print("cache hit")
                try:
                    cachefile = open(cachefilename, "rb")
                    start = time.time() # start timer
                    retval = pkl.load(cachefile)
                    elapsed = time.time() - start # end timer
                    readfile = True # remember success
                    cachefile.close()
                except IOError:
                    if verbose:
                        print("error reading cache: " + cachefilename)
                        print("recovering...")
                except EOFError:
                    if verbose:
                        print("error reading cache: " + cachefilename)
                        print("recovering...")
        if (readfile!=True):
            if verbose:
                print("cache miss")
                print("beginning hardread")
            start = time.time() # start timer
            if verbose:
                print("data_item(s): " + str(data_item))
                print("symbols to read: " + str(symbol_list))
            retval = self.get_data_hardread(ts_list, 
                symbol_list, data_item, verbose, bIncDelist)
            elapsed = time.time() - start # end timer
            if verbose:
                print("end hardread")
                print("saving to cache")
            try:
                cachefile = open(cachefilename,"wb")
                pkl.dump(retval, cachefile, -1)
                os.chmod(cachefilename,0o666)
            except IOError:
                print("error writing cache: " + cachefilename)
            if verbose:
                print("end saving to cache")
            if verbose:
                print("reading took " + str(elapsed) + " seconds")
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
