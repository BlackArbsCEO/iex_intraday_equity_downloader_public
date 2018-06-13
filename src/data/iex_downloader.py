#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 13 15:24:07 2018

@author: bcr
"""

from pathlib import PurePath, Path
import sys

## get project dir
pdir = PurePath("/media/bcr/HDD/Code_Backups_Sync/iex_intraday_equity_downloader")
data_dir = pdir/'data'
script_dir = pdir /'src'/'data'
sys.path.append(script_dir.as_posix())
from iex_downloader_utils import split_timestamp, write_to_parquet

import pandas as pd
import pandas_datareader.data as web
pd.options.display.float_format = '{:,.4f}'.format
import numpy as np

import pyarrow as pa
import pyarrow.parquet as pq

import logzero
from logzero import logger
#=============================================================================
## setup logger

logfile = PurePath(pdir/'logs'/'equity_downloader_logs'/'iex_downloader_log.log').as_posix()
log_format = '%(color)s[%(levelname)1.1s %(asctime)s.%(msecs)03d %(module)s:%(lineno)d]%(end_color)s %(message)s'
formatter = logzero.LogFormatter(fmt=log_format, datefmt='%Y-%m-%d %I:%M:%S')
logzero.setup_default_logger(logfile=logfile, formatter=formatter)

#=============================================================================
# import symbols

logger.info('importing symbols...')
symfp = Path(data_dir/'external'/'ETFList.Options.Nasdaq__M.csv')
symbols = (pd.read_csv(symfp).Symbol).tolist()

#=============================================================================
# request data

logger.info('requesting data from iex...')
now = pd.to_datetime('today') # get current timestamp

data = (web.DataReader(symbols,'iex-tops')
        .assign(lastSaleTime=lambda df:pd.to_datetime(df.lastSaleTime,unit='ms'))
        .assign(lastUpdated=lambda df:pd.to_datetime(df.lastUpdated,unit='ms'))
        .pipe(split_timestamp, timestamp=now))

if data.empty: logger.warn('data df is empty!')
#=============================================================================
# store data

logger.info('storing data to interim intraday_store')
outfp = PurePath(data_dir/'interim'/'intraday_store').as_posix()
write_to_parquet(data, outfp, logger=logger)
