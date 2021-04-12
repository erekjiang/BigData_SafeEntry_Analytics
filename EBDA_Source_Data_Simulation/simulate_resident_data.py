# -*- coding: utf-8 -*-


import pandas as pd
import random
import string
from pathlib import Path
from datetime import datetime

# generate users
df_resident = pd.DataFrame(columns=["resident_id","resident_name","nric","phone_number","last_update_dt"])

for i in range(1,3000+1):
    resident_id = str(i)
    resident_name = "resident_name_" + str(i)
    nric = random.choice(["S","F"]) + f"{i:06d}" + random.choice(string.ascii_uppercase)
    phone_number = str(random.randint(8, 9)) + f"{i:07d}"
    
    df_resident = df_resident.append({'resident_id': 'rid_' + str(resident_id), 
                                'resident_name': resident_name, 
                                'nric': nric, 
                                'phone_number': phone_number,
                                'last_update_dt':datetime.now()},
                               ignore_index=True)

resident_file_path = Path('out/resident.csv')
df_resident.to_csv(resident_file_path, index=False)