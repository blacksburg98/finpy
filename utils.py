import QSTK.qstkutil.DataAccess as da
from equity import Equity

def get_tickdata(ls_symbols, ldt_timestamps):
    c_dataobj = da.DataAccess('Yahoo', cachestalltime=0)
    ls_keys = ['open', 'high', 'low', 'close', 'volume', 'actual_close']
    ldf_data = c_dataobj.get_data(ldt_timestamps, ls_symbols, ls_keys)
    d_data = dict(zip(ls_keys, ldf_data))
    for s_key in ls_keys:
        d_data[s_key] = d_data[s_key].fillna(method = 'ffill')
        d_data[s_key] = d_data[s_key].fillna(method = 'bfill')
        d_data[s_key] = d_data[s_key].fillna(1.0)
    stocks = dict()
    for s in ls_symbols:
        stocks[s] = Equity(index=ldt_timestamps)
        for k in ls_keys:
            stocks[s][k] = d_data[k][s]
        stocks[s].nml_price()
    return stocks 

