# -*- coding: utf-8 -*-
"""
Created on Wed Mar 10 14:05:48 2021

@author: cmbsguser
"""

import pandas as pd
import random
import string

# generate users
df_place = pd.DataFrame(columns=["place_id","place_name","url","postal_code", "address", "lat", "lon", "place category"])

# Table - places (place id, place name, bit.ly url, postal code, 
# address, lat & long, place category(Mall, MRT, Retailer, Shops))


df_hc = pd.read_csv(".\\in\\hawker-centres\\list-of-government-markets-hawker-centres.csv")
df_mcst = pd.read_csv(".\\in\\mcst\\management-corporation-strata-title.csv",encoding='ANSI')

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
# clean data
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
    
    
df_place.to_csv('.\\out\\place.csv', index=False)

