#!/usr/bin/env python


from pandasdmx import Request
import datetime
import pandas as pd
import os
import sys
sys.path.append('grain-price-data/')
import datasources as ds

estat = Request(ds.wheat_production_data['provider'])
reporters = ds.wheat_production_data['reporter']
dest = ds.wheat_production_data['destination']
parameters  = {
            'startPeriod': ds.wheat_production_data['start_year'],
            'endPeriod':  ds.wheat_production_data['end_year']
        }
 
if not os.path.isdir(dest):
    os.mkdir(dest)

for reporter in reporters:
    data = []
    keys = {
        'GEO' : reporter,
        'CROPS' : ds.wheat_production_data['crop']
    }

    resp = estat.data(
            ds.wheat_production_data['dataset_historical'],
            key=keys,
            params=parameters 
        )
    data.append(resp.to_pandas().to_frame().reset_index())
    
    resp = estat.data(
            ds.wheat_production_data['dataset'],
            key=keys,
            params=parameters 
        )
    # rename dimensions to same as in old data
    df = resp.to_pandas().to_frame().reset_index()
    df.loc[(df['STRUCPRO'] == 'HU_EU'),'STRUCPRO'] = 'HU'   
    df.loc[(df['STRUCPRO'] == 'PR_HU_EU'),'STRUCPRO'] = 'PR'
    df.loc[(df['STRUCPRO'] == 'YI_HU_EU'), 'STRUCPRO'] = 'YI'    
    data.append(df)
    df = pd.concat(data)
    
    dataset_name = (ds.wheat_production_data['filename'][0] 
                    + reporter 
                    + ds.wheat_production_data['filename'][1])
    
    df.to_csv(dest + dataset_name,index=False)
    
