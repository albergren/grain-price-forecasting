#!/usr/bin/env python

import pandas as pd
import os
import sys
sys.path.append('grain-price-data/')
sys.path.append('tools/')
import datasources as ds
import DatasetBuilder as db

IMPORT_FLOW_CODE = 1
EXPORT_FLOW_CODE = 2
PRODUCTION_STRUCPRO_CODE = 'PR'

start_year = ds.wheat_trade_data['start_year']
end_year = ds.wheat_trade_data['end_year']
reporters = ds.wheat_trade_data['reporters']
trade_filename= ds.wheat_trade_data['filename']
prod_filename = ds.wheat_production_data['filename']



partners_to_drop = ['EU_EXTRA','EU_INTRA','WORLD','EA19_EXTRA','EA19_INTRA',
                    'EU27_2020_EXTRA','EU27_2020_INTRA','EU28_EXTRA','EU28_INTRA',
                    'EUROZONE_EXTRA','EUROZONE_INTRA']

years_to_drop = list(map(str, range(int(start_year),
                                    int(end_year))))

#path_to_data_directory = os.fsencode(ds.wheat_trade_data["destination"])
path_to_importExport_data = '/home/martin/gp/grain-price-forcasting/src/data/process/test/importExport_data/'
path_to_production_data = '/home/martin/gp/grain-price-forcasting/src/data/process/test/production_data/'

def calculate_feature(reporter, data):
    feature = [0]
    for index, row in data.iterrows():
        result  = (feature[-1] + row['production'] + row['import']) - row['export']
        feature.append(result)
    feature = pd.DataFrame({reporter : feature})
    return feature

df_features = pd.date_range(ds.wheat_production_data['start_year'],ds.wheat_production_data['end_year'])
reporters = ['DK','FR']

for reporter in reporters:
    filename = (trade_filename[0] + reporter + trade_filename[1])
    df = pd.read_csv(path_to_importExport_data + filename)
    df = df[df['value'].notna()]
    
    production_data_filename = (prod_filename[0] + reporter + prod_filename[1])

    # Drop partners that are aggregate and rows with yearly sums
    # and set value column to numeric
    df = df[~df['PARTNER'].isin(partners_to_drop)]
    df = df[~df['TIME_PERIOD'].isin(years_to_drop)]
    df['value'] = pd.to_numeric(df['value'])
    df = df.drop_duplicates()
    df['TIME_PERIOD'] = pd.to_datetime(df['TIME_PERIOD'])
    
    
    # Split into import and export
    df_import = df[df['FLOW'] == IMPORT_FLOW_CODE]
    df_export = df[df['FLOW'] == EXPORT_FLOW_CODE]
   

    # Group periods
    df_import = df_import.groupby('TIME_PERIOD')['value'].sum() 
    df_export = df_export.groupby('TIME_PERIOD')['value'].sum() 
    
    
    df_production = pd.read_csv(path_to_production_data + production_data_filename)
    df_production = df_production[df_production['STRUCPRO'] == PRODUCTION_STRUCPRO_CODE]
    
    df_production = db.DatasetBuilder(df_production)
    df_production.resample_year_to_month('TIME_PERIOD')
    df_production = df_production.get_set()
    df_production =  df_production.fillna(0)
    df_production['value'] = df_production['value'] * 10000

    new_df = pd.concat([df_production['value'], df_export, df_import], axis=1)   # time_period has to be index 
    column_names = ['production','export','import']
    new_df.columns = column_names
    feature = calculate_feature(reporter, new_df)
    df_features = pd.concat([df_features,feature], axis=1) # concat on df with datetime index instead
    
print(df_features)
