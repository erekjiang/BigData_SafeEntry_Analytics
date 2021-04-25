# -*- coding: utf-8 -*-


import pandas as pd
import random as rd
import numpy as np
from pathlib import Path
import uuid
from datetime import datetime, timedelta

resident_file_path = Path('out/resident.csv')
place_file_path = Path('out/place.csv')
df_resident = pd.read_csv(resident_file_path)
df_location = pd.read_csv(place_file_path)

df_checkin = pd.DataFrame(columns=["record_id","resident_id","place_id","entry_time","exit_time","last_update_dt"])

resident_size = df_resident.shape[0]
location_size = df_location.shape[0]

for i in range(1000*2*25):
    print('index', i)
    record_id = uuid.uuid4()

    end = datetime.strptime('2021/4/21', '%Y/%m/%d')
    start = end + timedelta(days=-25)

    date_str = (start + (end - start) * rd.random()).strftime('%Y/%m/%d')
    
    resident_index = rd.randint(0, resident_size-1)
    place_index=rd.randint(0, location_size-1)
    
    resident_id = df_resident['resident_id'][resident_index]
    place_id = df_location['place_id'][place_index]

    entry_time_stamp = rd.randint(8*60, 20*60)
    duration = np.random.normal(60, 40)
    
    exit_time_stamp = entry_time_stamp + int(duration)
    
    entry_hr = entry_time_stamp // 60
    entry_min = entry_time_stamp % 60
    
    exit_hr= exit_time_stamp //60
    exit_min = exit_time_stamp % 60
    
    entry_time = date_str + " " +  f"{entry_hr:02d}" +":" + f"{entry_min:02d}"
    exit_time = date_str + " " +  f"{exit_hr:02d}" +":" + f"{exit_min:02d}"

    datetime.strptime(entry_time, '%Y/%m/%d %H:%M')

    df_checkin = df_checkin.append({'record_id': record_id,
                                    'resident_id': resident_id,
                                    'place_id': place_id, 
                                    'entry_time': datetime.strptime(entry_time, '%Y/%m/%d %H:%M'),
                                    'exit_time': datetime.strptime(exit_time, '%Y/%m/%d %H:%M'),
                                    'last_update_dt':datetime.now()},
                                   ignore_index=True)

checkin_file_path = Path('out/entry_record.csv')
df_checkin.sort_values(by=['entry_time'], inplace=True)
df_checkin.to_csv(checkin_file_path, index=False)
