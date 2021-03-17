# -*- coding: utf-8 -*-


import pandas as pd
import random as rd
import numpy as np
from pathlib import Path
import uuid
from datetime import datetime

resident_file_path = Path('out/resident.csv')
place_file_path = Path('out/place.csv')
df_resident = pd.read_csv(resident_file_path)
df_location = pd.read_csv(place_file_path)

df_checkin = pd.DataFrame(columns=["record_id","resident_id","place_id","entry_time","exit_time","last_update_dt"])

resident_size = df_resident.shape[0]
location_size = df_location.shape[0]



for i in range(100):
    print('index', i)
    record_id = uuid.uuid4()
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
    
    df_checkin = df_checkin.append({'record_id': record_id,
                                    'resident_id': resident_id,
                                    'place_id': place_id, 
                                    'entry_time': entry_time,
                                    'exit_time': exit_time,
                                    'last_update_dt':datetime.now()},
                                   ignore_index=True)

checkin_file_path = Path('out/entry_records.csv')
df_checkin.to_csv(checkin_file_path, index=False)
