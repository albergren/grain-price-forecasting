#!/usr/bin/env python

import pandas as pd
import os
import sys
sys.path.append('grain-price-data/')
sys.path.append('tools/')
import datasources as ds
from DatasetBuilder import DatasetBuilder

IMPORT_FLOW_CODE = 1
EXPORT_FLOW_CODE = 2
PRODUCTION_STRUCPRO_CODE = 'PR'

start_year = ds.wheat_trade_data['start_year']
end_year = ds.wheat_trade_data['end_year']
reporters = ds.wheat_trade_data['reporters']
trade_filename= ds.wheat_trade_data['filename']
prod_filename = ds.wheat_production_data['filename']
dest = '/home/martin/gp/grain-price-forcasting/grain-price-data/processed/'


partners_to_drop = ['EU_EXTRA','EU_INTRA','WORLD','EA19_EXTRA','EA19_INTRA',
                    'EU27_2020_EXTRA','EU27_2020_INTRA','EU28_EXTRA','EU28_INTRA',
                    'EUROZONE_EXTRA','EUROZONE_INTRA']

years_to_drop = list(map(str, range(int(start_year),
                                    int(end_year))))


path_to_importExport_data = ds.wheat_trade_data["destination"]
path_to_production_data =  ds.wheat_production_data["destination"]
df_features = pd.DataFrame()

def calculate_feature(reporter, data, mean):

    feature = [0]
    time_idx = [0]

    for index, row in data.iterrows():
        result  = (feature[-1] + (row['production'] * mean) + row['import']) - row['export']
        time_idx.append(index)
        feature.append(result)
        
    feature = pd.DataFrame(feature[1:],index=time_idx[1:], columns=[reporter])
    return feature

def process_production_data(data):
    processed_data_df = data[data['STRUCPRO'] == PRODUCTION_STRUCPRO_CODE]    
    processed_data_df['value'] = processed_data_df['value'] * 10000
    processed_data_df = DatasetBuilder(processed_data_df)
    processed_data_df_yearly = processed_data_df.get_set()
    processed_data_df.resample_year_to_month('TIME_PERIOD')
    processed_data_df.shift_columns(processed_data_df.get_set().columns, 12)
    processed_data_df_monthly = processed_data_df.get_set()
    
    return processed_data_df_monthly, processed_data_df_yearly

def process_importExport_data(data):
    df = data[data['value'].notna()]
    
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

    return df_import, df_export

for reporter in reporters:

    trade_data_filename = (trade_filename[0] + reporter + trade_filename[1])
    try:
        df_trade_data = pd.read_csv(path_to_importExport_data + trade_data_filename)
    except IOError:
        print("Error:\tFile " + trade_data_filename +
              " not found!\n\tOmitting processing of reporter: " + reporter)
        break

    df_import_processed, df_export_processed = process_importExport_data(df_trade_data)

    
    production_data_filename = (prod_filename[0] + reporter + prod_filename[1])
    try:
        df_production = pd.read_csv(path_to_production_data + production_data_filename)
    
    except IOError:
        print("Warning: File " + trade_data_filename +
              " not found! Omitting processing of reporter: " + reporter)
        break

    df_production_processed, df_production_processed_yearly  = process_production_data(df_production)
    df_production_processed_yearly = df_production_processed_yearly.reset_index()
    total_exports = (df_export_processed.groupby(by=[df_export_processed.index.year]).sum())
    total_exports = total_exports.reset_index()


    export_percentages  =  (total_exports['value'].T /  (df_production_processed_yearly['value']) ).T
    exports_mean = export_percentages.mean()

    df = pd.concat([df_production_processed['value'],
                        df_export_processed,
                        df_import_processed], axis=1)
    df = df.fillna(0)

    column_names = ['production','export','import']
    df.columns = column_names
    feature = calculate_feature(reporter, df, exports_mean)
    df_features = pd.concat([df_features,feature], axis=1)


dataset_name = 'tradeProductionBalance_monthly_processed.csv'
print("Writing data to file...")
try:
    df_features.to_csv(dest + dataset_name)
    print("File '" + dataset_name + "' created!") 
except IOError:
    print("Error:\t File could not be written!")
          
