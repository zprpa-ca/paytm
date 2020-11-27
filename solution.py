#!/usr/bin/env python

''' <docs>
'''

#-- standard libs
import sqlite3, os, logging

#-- 3rd party libs
import numpy  as np
import pandas as pd
import dask.dataframe as dd

#-- my custom libs

#==============================================================================#
#------------------------- setup and such -------------------------------------#
#==============================================================================#

def _logger(fname):
    logger = logging.getLogger()

    file_log = logging.FileHandler(fname + '.log')
    fmt      = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    file_log.setFormatter(fmt)
    logger.addHandler(file_log)

    tty_log = logging.StreamHandler()
    tty_log.setFormatter(fmt)
    logger.addHandler(tty_log)

    logger.setLevel(logging.INFO)
    logger.info('\n'.join(('*','*','*','*')))
    return logger
#------------------------------------------------------------------------------#

logger = _logger(__file__)

#==============================================================================#
#------------------------- main -----------------------------------------------#
#==============================================================================#

#---------------------------------#
#-- load data
#---------------------------------#

with open('data/countrylist.csv','r') as f:
    lines = f.readlines()
ccd   = [l[:2] for l in lines[1:]]
cname = [l[3:-1] for l in lines[1:]]    #-- strip end-of-line
countries = pd.DataFrame({'country_code':ccd,'country_name':cname})
countries = dd.from_pandas(countries, npartitions=1)
stations  = pd.read_csv('data/stationlist.csv')
stations  = dd.from_pandas(stations, npartitions=1)

gzips = os.listdir('data/2019')
for fi_name in [_fn for _fn in gzips if _fn[-3:]=='.gz']:
    fi = gzip.open(f"data/2019/{fi_name}",'rb')
    fo = open(f"data/2019/{fi_name[:-3]}",'wb')
    fo.write(fi.read())
    fo.close()
    fi.close()

ddf = dd.read_csv(  'data/2019/*.csv',
                    parse_dates = ['YEARMODA'],
                    dtype={ 'STN---'    : np.str    ,
                            'WBAN'      : np.int64  ,
                            'TEMP'      : np.float64,
                            'DEWP'      : np.float64,
                            'SLP'       : np.float64,
                            'STP'       : np.float64,
                            'VISIB'     : np.float64,
                            'WDSP'      : np.float64,
                            'MXSPD'     : np.float64,
                            'GUST'      : np.str    ,
                            'MAX'       : np.str    ,
                            'MIN'       : np.str    ,
                            'PRCP'      : np.str    ,
                            'SNDP'      : np.str    ,
                            'FRSHTT'    : np.str    })

ddf = ddf.merge(stations,  left_on='STN---',       right_on='STN_NO')
ddf = ddf.merge(countries, left_on='COUNTRY_ABBR', right_on='country_code')

#---------------------------------#
#-- compute stats
#---------------------------------#

#1. Which country had the hottest average mean temperature over the year?
#2. Which country had the most consecutive days of tornadoes/funnel cloud formations?
#3. Which country had the second highest average mean wind speed over the year?

#-- #1
avg_temp = ddf.groupby('country_name').TEMP.mean().compute()
avg_temp.loc[avg_temp==avg_temp.max()]

#-- #2
pdf = ddf.loc[ddf.FRSHTT.str[5]=='1', ['country_name','YEARMODA','FRSHTT']].compute()
pdf['date2'] = None
s1 = pdf.YEARMODA.iloc[1:]
s1.reset_index(inplace=True, drop=True)
pdf.date2 = s1
pdf.date2.iloc[-1] = pdf.YEARMODA.iloc[-1]
pdf.date_diff = (pdf.YEARMODA - pdf.date2).abs()
pdf1 = pdf.loc[pdf.date_diff < pd.Timedelta('2 days 00:00:00')]
pdf2 = pdf1.groupby('country_name').count()
pdf2.reset_index(inplace=True)
pdf2.sort_values(by=['date_diff'], ascending=False, inplace=True)
''' surprisingly, it is Italy over USA
>>> pdf2
     country_name  date_diff
6           ITALY          7
8   UNITED STATES          6
1          CANADA          3
2  CAYMAN ISLANDS          3
7           JAPAN          2
0     BAHAMAS THE          1
3           GHANA          1
4           INDIA          1
5          ISRAEL          1
9   WESTERN SAMOA          1
>>>
'''

#-- #3
avg_wdsp = ddf.groupby('country_name').WDSP.mean().compute()
avg_wdsp.sort_values(ascending=False, inplace=True)
avg_wdsp = avg_wdsp.reset_index()
avg_wdsp.iloc[1]        #-- get the second highest value

#==============================================================================#
