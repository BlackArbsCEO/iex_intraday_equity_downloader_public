#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 13 15:24:07 2018

@author: blackarbsceo
"""

from pathlib import PurePath, Path
import sys
import tzlocal # pip install

## get project dir
pdir = PurePath("/YOUR/DIRECTORY/iex_intraday_equity_downloader")
data_dir = pdir/'data'
script_dir = pdir /'src'/'data'
sys.path.append(script_dir.as_posix())
from iex_downloader_utils import split_timestamp, write_to_parquet

import pandas as pd
import pandas_datareader.data as web
pd.options.display.float_format = '{:,.4f}'.format
import numpy as np
import pandas_market_calendars as mcal # pip install

import pyarrow as pa
import pyarrow.parquet as pq

import logzero
from logzero import logger
#=============================================================================
# get current timestamp

now = pd.to_datetime('today')
#=============================================================================
## setup logger

logfile = PurePath(pdir/'logs'/'equity_downloader_logs'/f'iex_downloader_log_{now.date()}.log').as_posix()
log_format = '%(color)s[%(levelname)1.1s %(asctime)s.%(msecs)03d %(module)s:%(lineno)d]%(end_color)s %(message)s'
formatter = logzero.LogFormatter(fmt=log_format, datefmt='%Y-%m-%d %I:%M:%S')
logzero.setup_default_logger(logfile=logfile, formatter=formatter)

#=============================================================================
# confirm market hours

local_tz = tzlocal.get_localzone() # get local timezone via tzlocal package
now_local_tz = now.tz_localize(local_tz) # localize current timestamp
nyse = mcal.get_calendar('NYSE') # get NYSE calendar

nyseToday = nyse.schedule(start_date=now.date(), end_date=now.date())
mktOpen = nyseToday.market_open.iloc[0].tz_convert(local_tz)
mktClose = nyseToday.market_close.iloc[0].tz_convert(local_tz)

if mktOpen <= now_local_tz <= mktClose:  # only run during market hours
    #==========================================================================
    # import symbols

    logger.info('importing symbols...')
    symfp = Path(data_dir/'external'/'ETFList.Options.Nasdaq__M.csv')
    symbols = (pd.read_csv(symfp).Symbol).tolist()

    #==========================================================================
    # request data

    logger.info('requesting data from iex...')
    data = (web.DataReader(symbols,'iex-tops')
            .assign(lastSaleTime=lambda df:pd.to_datetime(df.lastSaleTime,unit='ms'))
            .assign(lastUpdated=lambda df:pd.to_datetime(df.lastUpdated,unit='ms'))
            .pipe(split_timestamp, timestamp=now)
            .dropna())
    # force float conversion for the following columns
    # this is due to a problem reading in the data when schema changes
    # for example when these columns are populated the data is float, when not,
    # value is 0, then int64 dtypes causes schema change and read error
    to_float = ['askPrice','bidPrice','lastSalePrice','marketPercent']
    data.loc[:,to_float] = data.loc[:,to_float].astype(float)

    if data.empty: logger.warn('data df is empty!')
    #==========================================================================
    # store data

    logger.info('storing data to interim intraday_store')
    outfp = PurePath(data_dir/'interim'/'intraday_store').as_posix()
    write_to_parquet(data, outfp, logger=logger)
else:
    logger.warn('system outside of market hours, no data queried')


