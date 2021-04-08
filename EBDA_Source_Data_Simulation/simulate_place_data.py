# -*- coding: utf-8 -*-


import pandas as pd
import random
import string
from pathlib import Path
import random
import requests
import sys
import time
from datetime import datetime


def pcode_to_data(pcode):
    page = 1
    results = []
    retry_cnt = 0
    while True:
        try:
            response = requests.get(
                'https://developers.onemap.sg/commonapi/search?searchVal=%s&returnGeom=Y&getAddrDetails=Y&pageNum=%d' %
                (pcode, page)).json()
            results = results + response['results']
            if response['totalNumPages'] > page:
                page = page + 1
            else:
                break
        except:
            if retry_cnt > 10:
                print('Fetching %s failed for too many times. Skipping ...' % pcode, file=sys.stderr, flush=True)
                break
            else:
                retry_cnt += 1
                print('Fetching %s failed for %d times. Retrying in 5-10 sec ...' % (pcode, retry_cnt), file=sys.stderr,
                      flush=True)
                time.sleep(5 + 5 * random.random())
                continue

    return results

# generate place
df_place = pd.DataFrame(
    columns=["place_id", "place_name", "url", "postal_code", "address", "lat", "lon", "place_category",
             "last_update_dt"])

# Table - places (place id, place name, bit.ly url, postal code,
# address, lat & long, place category(Mall, MRT, Retailer, Shops))

df_hc = pd.read_csv(Path('in/hawker-centres/list-of-government-markets-hawker-centres.csv'))
#df_mcst = pd.read_csv(Path('in/mcst/management-corporation-strata-title.csv'), encoding='ANSI')
df_pc = pd.read_json(Path('in/one-map/singpostcode.json'))
# df_hc = pd.read_csv(hc_file_path)
# # df_mcst = pd.read_csv(place_file_path, encoding='ANSI')
# df_mcst = pd.read_csv(place_file_path)
# df_pc = pd.read_json("./in/location/singpostcode.json")

# process hawker centers
for i in range(0, df_hc.shape[0]):
    place_name = df_hc['name_of_centre'][i]

    address = df_hc['location_of_centre'][i]
    list_address = address.split(",")

    postal_code = list_address[-1].strip()[2:-1]

    # retrieve lat long
    postal = postal_code[0:6]
    latitude = ''
    longitude = ''
    if postal.isnumeric():
        df_location = df_pc.loc[df_pc['POSTAL'] == postal]
        if df_location.empty:
            location = pcode_to_data('%06d' % int(postal))
            if len(location) > 0:
                latitude = location[0]['LATITUDE']
                longitude = location[0]['LONGITUDE']
        else:
            latitude = df_location.iloc[0]['LATITUDE']
            longitude = df_location.iloc[0]['LONGITUDE']

    df_place = df_place.append({'place_id': 'pid_' + str(i + 1),
                                'place_name': place_name,
                                'postal_code': postal_code,
                                'address': address,
                                'last_update_dt': datetime.now(),
                                'lat': latitude,
                                'lon': longitude
                                },
                               ignore_index=True)

size = df_place.shape[0]

# process mcst
# clean in
'''
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
    
    # retrieve lat long
    postal = postal_code[0:6]
    latitude = ''
    longitude = ''
    if postal.isnumeric():
        df_location = df_pc.loc[df_pc['POSTAL'] == postal_code[0:6]]
        if df_location.empty:
            location = pcode_to_data('%06d' % int(postal_code[0:6]))
            if len(location) > 0:
                latitude = location[0]['LATITUDE']
                longitude = location[0]['LONGITUDE']
        else:
            latitude = df_location.iloc[0]['LATITUDE']
            longitude = df_location.iloc[0]['LONGITUDE']

    df_place = df_place.append({'place_id': 'pid_' + str(j + size),
                                'place_name': place_name,
                                'postal_code': postal_code,
                                'address': address,
                                'last_update_dt': datetime.now(),
                                'lat': latitude,
                                'lon': longitude
                                },
                               ignore_index=True)
'''

place_file_path = Path('out/place.csv')
df_place.to_csv(place_file_path, index=False)