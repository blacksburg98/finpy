'''
(c) 2011, 2012 Georgia Tech Research Corporation
This source code is released under the New BSD license.  Please see
http://wiki.quantsoftware.org/index.php?title=QSTK_License
for license details.

Created on Jan 16, 2013

@author: Sourabh Bajaj
@contact: sourabhbajaj90@gmail.com
@summary: EventProfiler

'''

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import finpy.dataaccess as da
import finpy.fpdateutil as du


def eventprofiler(df_events, all_stocks, i_lookback=20, i_lookforward=20,
                s_filename='study', b_market_neutral=True, b_errorbars=True,
                s_market_sym='SPY', out_pict=False):
    ''' Event Profiler for an event matix'''
    df_tmpclose = {}
    df_tmprets = {}
    for x in all_stocks:
        df_tmpclose[x] = all_stocks[x]['close'].copy()
        df_tmprets[x] = pd.Series(all_stocks[x].daily_return(), index=df_tmpclose[x].index)
    df_close = pd.DataFrame(df_tmpclose)
    df_rets = pd.DataFrame(df_tmprets)
    if b_market_neutral == True:
        df_rets = df_rets - df_rets[s_market_sym]
        del df_rets[s_market_sym]
        del df_events[s_market_sym]

    df_close = df_close.reindex(columns=df_events.columns)

    # Removing the starting and the end events
    df_events.values[0:i_lookback, :] = np.nan
    df_events.values[-i_lookforward:, :] = np.nan
    # Number of events
    i_no_events = np.nansum(df_events.values)
#    i_no_events = 0
#    for i in df_events:
#        for j in df_events[i]:
#            if j == 1:
#                i_no_events += 1
    na_event_rets = "False"

    # Looking for the events and pushing them to a matrix
    for i, s_sym in enumerate(df_events.columns):
        for j, dt_date in enumerate(df_events.index):
            if df_events[s_sym][dt_date] == 1:
                na_ret = df_rets[s_sym][j - i_lookback:j + 1 + i_lookforward]
                if type(na_event_rets) == type(""):
                    na_event_rets = na_ret
                else:
                    na_event_rets = np.vstack((na_event_rets, na_ret))

    # Computing daily rets and retuns
    na_event_rets = np.cumprod(na_event_rets + 1, axis=1)
    na_event_rets = (na_event_rets.T / na_event_rets[:, i_lookback]).T

    # Study Params
    na_mean = np.mean(na_event_rets, axis=0)
    na_std = np.std(na_event_rets, axis=0)
    li_time = list(range(-i_lookback, i_lookforward + 1))

    # Plotting the chart
    if out_pict:
        plt.clf()
        plt.axhline(y=1.0, xmin=-i_lookback, xmax=i_lookforward, color='k')
        if b_errorbars == True:
            plt.errorbar(li_time[i_lookback:], na_mean[i_lookback:],
                        yerr=na_std[i_lookback:], ecolor='#AAAAFF',
                        alpha=0.1)
        plt.plot(li_time, na_mean, linewidth=3, label='mean', color='b')
        plt.xlim(-i_lookback - 1, i_lookforward + 1)
        if b_market_neutral == True:
            plt.title('Market Relative mean return of ' +\
                    str(i_no_events) + ' events')
        else:
            plt.title('Mean return of ' + str(i_no_events) + ' events')
        plt.xlabel('Days')
        plt.ylabel('Cumulative Returns')
        plt.savefig(s_filename, format='pdf')
    return i_no_events
