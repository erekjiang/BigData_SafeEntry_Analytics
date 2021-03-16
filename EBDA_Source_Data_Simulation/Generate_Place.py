# -*- coding: utf-8 -*-


import pandas as pd
import random
import string
from pathlib import Path

# generate users
df_place = pd.DataFrame(columns=["place_id","place_name","url","postal_code", "address", "lat", "lon", "place_category"])

# Table - places (place id, place name, bit.ly url, postal code, 
# address, lat & long, place category(Mall, MRT, Retailer, Shops))

hc_file_path = Path('in/hawker-centres/list-of-government-markets-hawker-centres.csv')
place_file_path = Path('in/mcst/management-corporation-strata-title.csv')
df_hc = pd.read_csv(hc_file_path)
#df_mcst = pd.read_csv(place_file_path, encoding='ANSI')
df_mcst = pd.read_csv(place_file_path)

# process hawker centers
for i in range(0, df_hc.shape[0]):
    place_name = df_hc['name_of_centre'][i]
    
    address = df_hc['location_of_centre'][i]
    list_address = address.split(",")
    
    postal_code = list_address[-1].strip()[2:-1]

    df_place = df_place.append({'place_id': 'pid_' + str(i+1), 
                                'place_name': place_name, 
                                'postal_code': postal_code, 
                                'address': address},
                               ignore_index=True)
    
size = df_place.shape[0]

# process hawker center
# clean in
df_mcst = df_mcst[df_mcst.usr_devtname != 'na']
df_mcst = df_mcst[df_mcst.usr_devtname.isna() != True]
df_mcst = df_mcst[~df_mcst.usr_devtname.str.lower().str.contains('terminate')]
df_mcst = df_mcst[~df_mcst.usr_devtname.str.lower().str.contains('en-bloc')]

df_mcst = df_mcst[df_mcst.mcst_buildingname != 'na']
df_mcst = df_mcst[df_mcst.devt_location != 'na']

df_mcst = df_mcst.reset_index(drop=True)

for j in range(df_mcst.shape[0]):
    place_name = df_mcst['usr_devtname'][j]
    
    address = df_mcst['devt_location'][j]
    list_address = address.split(" ")
    
    postal_code = list_address[-1].strip()

    df_place = df_place.append({'place_id': 'pid_' + str(j + size), 
                                'place_name': place_name, 
                                'postal_code': postal_code, 
                                'address': address},
                               ignore_index=True)

place_file_path = Path('out/place.csv')
df_place.to_csv(place_file_path, index=False)

