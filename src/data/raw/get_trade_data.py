#!/usr/bin/env python

from pandasdmx import Request
import datetime
import pandas as pd
import os
import sys
# need better way to deal with paths to modules
sys.path.append('grain-price-data/')
print(sys.path)

import datasources as ds


start_year = ds.wheat_trade_data['start_year']
end_year = ds.wheat_trade_data['end_year']
estat = Request(ds.wheat_trade_data['provider'])
reporters = ds.wheat_trade_data['reporters']
dest = ds.wheat_trade_data['destination'] + "_"+ str(datetime.date.today()) + '/'

if not os.path.isdir(dest):
    os.mkdir(dest)


for reporter in reporters:
    
    keys = {
        'REPORTER': reporter,
        'PRODUCT':ds.wheat_trade_data['product'],
        'INDICATORS': 'QUANTITY_IN_100KG'
    }


    trade_data = []
    for year in range(start_year, end_year):

        parameters = {
            'startPeriod': year,
            'endPeriod': year + 1
        }

        print("Downloading data for", year)

        resp = estat.data(
            ds.wheat_trade_data['dataset'],
            key=keys,
            params=parameters 
        )

        trade_data.append(resp.to_pandas())

        df = pd.concat(trade_data)

    dataset_name = 'wheat_importExport_' + reporter + '_monthly_raw.csv'
    df.to_csv(dest + dataset_name)


