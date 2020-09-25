#!/usr/bin/env python

import pandas as pd
import os
import sys
sys.path.append('grain-price-data/')
import datasources as ds

IMPORT_FLOW_CODE = 1
EXPORT_FLOW_CODE = 2

partners_to_drop = ['EU_EXTRA','EU_INTRA','WORLD','EA19_EXTRA','EA19_INTRA',
                    'EU27_2020_EXTRA','EU27_2020_INTRA','EU28_EXTRA','EU28_INTRA',
                    'EUROZONE_EXTRA','EUROZONE_INTRA']

years_to_drop = list(map(str, range(ds.wheat_trade_data['start_year'],
                                    ds.wheat_trade_data['end_year'])))

#path_to_data_directory = os.fsencode(ds.wheat_trade_data["destination"])
path_to_data_directory = '/home/martin/gp/grain-price-forcasting/src/data/process/test/'
raw_data_directory = os.fsencode(path_to_data_directory)

for f in os.listdir(raw_data_directory):
    filename = os.fsdecode(f)
    df = pd.read_csv(path_to_data_directory + filename)
    df = df[df['value'].notna()]

    
    # Drop partners that are aggregate and rows with yearly sums
    # and set value column to numeric
    df = df[~df['PARTNER'].isin(partners_to_drop)]
    df = df[~df['TIME_PERIOD'].isin(years_to_drop)]
    df['value'] = pd.to_numeric(df['value'])
    df = df.drop_duplicates()

    
    # Split into import and export
    df_import = df[df['FLOW'] == IMPORT_FLOW_CODE]
    df_export = df[df['FLOW'] == EXPORT_FLOW_CODE]
    

    # Group periods
    df_import = df_import.groupby('TIME_PERIOD')['value'].sum() 
    df_export = df_export.groupby('TIME_PERIOD')['value'].sum() 
    
    print(df_import)
    

