#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 13 16:00:07 2018

@author: blackarbsceo
"""
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

def split_timestamp(df, timestamp):
    # use current timestamp
    df = df.assign(queryTime=lambda df: timestamp,
                   year=lambda df: timestamp.year, # make year
                   month=lambda df: timestamp.month, # make month
                   day=lambda df: timestamp.day, # make day
                   time=lambda df: timestamp.strftime('%H:%M:%S')) # make time
    return df

def write_to_parquet(df, root_path,
                     partition_cols=['year','month','day','time'],
                     logger=None):
    """
    fn: wrapper for pyarrow write_to_dataset

    Params
    ------
    df : pd.DataFrame
        formatted dataframe data
    root_path : str, data store directory
    partition_cols : list of columns (as str dtype) to partition parquet storage directory
    logger : logger object
    """
    if not logger: raise ValueError('must use logger object')
    try:
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(table, root_path=root_path,
                            partition_cols=partition_cols)
    except Exception as e:
        logger.exception(e)
