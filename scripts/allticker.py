from __future__ import print_function
import csv
import re
import sets
import sys
import finpy.sec.ticker_util as ticker_util
if __name__ == '__main__':
    cjk = sets.Set()
    comp_file = sys.argv[1]
    csvfile_name = sys.argv[2]
    cjk_done_name = sys.argv[3]
    with open(comp_file, 'rU') as fp:
        for line in fp:
#            lre = re.search('\s(\d*)\s*\d{4}-\d{2}-\d{2}', line)
            lre = re.search('edgar/data/(\d*)', line)
            if lre:
                cjk.add(lre.group(1))
#    with open("allcjk.txt", 'w') as allcjkfile:
#        for i in cjk:
#            print(i, file=allcjkfile);
    cjk_done = sets.Set()
    try:
        with open(cjk_done_name, 'r') as cjk_donefile:
            for line in cjk_donefile:
                cjk_done.add(line.rstrip('\n'))
        cjk_donefile.close()
    except:
        pass
    tick_done = []
    try:
        with open(csvfile_name, 'r') as csvfile:
            cr = csv.reader(csvfile, delimiter=',')
            for row in cr:
                tick_done.append(row)
        csvfile.close()
    except:
        pass
    with open(csvfile_name, 'w') as csvfile, open(cjk_done_name, 'w') as cjk_donefile:
        cw = csv.writer(csvfile, lineterminator='\n', delimiter=',')
        for ck in tick_done:
            cw.writerow(ck)
        for l in cjk_done:
            print(l, file=cjk_donefile)
        for cjk in cjk:
            if cjk not in cjk_done:
                print(cjk, file=cjk_donefile)
                cjk_donefile.flush()
            if not(cjk in cjk_done):
                l = []
                tick = ticker_util.cjk2tick(cjk)
                if tick != None:
                    l.append(cjk)
                    l.append(tick)
                    cw.writerow(l)
                    csvfile.flush()     
