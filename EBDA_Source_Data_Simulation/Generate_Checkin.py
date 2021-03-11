# -*- coding: utf-8 -*-
"""
Created on Thu Mar 11 11:49:24 2021

@author: cmbsguser
"""

import pandas as pd
import random as rd
import numpy as np

df_resident = pd.read_csv('.\\out\\resident.csv')
df_location = pd.read_csv('.\\out\\place.csv')

df_checkin = pd.DataFrame(columns=["resident_id","place_id","entry_time","exit_time"])

resident_size = df_resident.shape[0]
location_size = df_location.shape[0]



for i in range(100000):
    print('index', i)
    date_str = '2021/02/0' + str(rd.randint(1, 7))
    
    resident_index = rd.randint(0, resident_size-1)
    place_index=rd.randint(0, location_size-1)
    
    resident_id = df_resident['resident_id'][resident_index]
    place_id = df_location['place_id'][place_index]

    entry_time_stamp = rd.randint(8*60, 20*60)
    duration = np.random.normal(60, 20)
    
    exit_time_stamp = entry_time_stamp + int(duration)
    
    entry_hr = entry_time_stamp // 60
    entry_min = entry_time_stamp % 60
    
    exit_hr= exit_time_stamp //60
    exit_min = exit_time_stamp % 60
    
    entry_time = date_str + " " +  f"{entry_hr:02d}" +":" + f"{entry_min:02d}"
    exit_time = date_str + " " +  f"{exit_hr:02d}" +":" + f"{exit_min:02d}"
    
    df_checkin = df_checkin.append({'resident_id': resident_id, 
                                    'place_id': place_id, 
                                    'entry_time': entry_time,
                                    'exit_time': exit_time},
                                   ignore_index=True)
    
df_checkin.to_csv('.\\out\\checkin.csv', index=False)  
