#!/usr/bin/env python

import pandas as pd
import os
import sys
sys.path.append('grain-price-data/')
import datasources as ds

#path_to_data_directory = os.fsencode(ds.wheat_trade_data["destination"])
path_to_data_directory = '/home/martin/gp/grain-price-forcasting/src/data/process/test/'
raw_data_directory = os.fsencode(path_to_data_directory)

for f in os.listdir(raw_data_directory):
    filename = os.fsdecode(f)
    df = pd.read_csv(path_to_data_directory + filename)
    df = df[df['value'].notna()]
    print(df)
    
    

