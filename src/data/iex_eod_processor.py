#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 14 16:00:45 2018

@author: blackarbsceo
"""

from pathlib import PurePath, Path
import sys
import shutil

## get project dir
pdir = PurePath("/YOUR/DIRECTORY/iex_intraday_equity_downloader")
data_dir = pdir/'data'
script_dir = pdir /'src'/'data'
sys.path.append(script_dir.as_posix())

import pandas as pd
pd.options.display.float_format = '{:,.4f}'.format

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
# read intraday data into one dataframe

logger.info('reading all intraday data for today as dataframe...')
infp = PurePath(data_dir/'interim'/'intraday_store').as_posix()

try:
    df = pd.read_parquet(infp).drop_duplicates().dropna().reset_index(drop=True)
    if df.empty: logger.warn('empty dataframe for eod processing')
    #==========================================================================
    # store intraday data into one compressed dataframe

    logger.info('storing all intraday data for today as compressed parquet file...')
    outfp = PurePath(data_dir/'processed'/'intraday_store'/f'etf_intraday_data_{now.date()}.parq')
    df.to_parquet(outfp, engine='fastparquet')

    #==========================================================================
    # delete interim store

    logger.info('deleting all interim intraday data.')
    rmfp = Path(data_dir/'interim'/'intraday_store'/f'year={now.year}')
    shutil.rmtree(rmfp)

except Exception as e:
    logger.error(f'{e}\tlikely no data today: {now.date()}')
