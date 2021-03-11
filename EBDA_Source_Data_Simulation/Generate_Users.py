# -*- coding: utf-8 -*-
"""
Created on Wed Mar 10 13:27:53 2021

@author: cmbsguser
"""

import pandas as pd
import random
import string

# generate users
df_resident = pd.DataFrame(columns=["resident_id","resident_name","nric","phone_number"])

for i in range(1,10000+1):
    resident_id = str(i)
    resident_name = "resident_name_" + str(i)
    nric = "S" + f"{i:06d}" + random.choice(string.ascii_letters)
    phone_number = str(random.randint(8, 9)) + f"{i:07d}"
    
    df_resident = df_resident.append({'resident_id': 'rid_' + str(resident_id), 
                                'resident_name': resident_name, 
                                'nric': nric, 
                                'phone_number': phone_number},
                               ignore_index=True)
    
df_resident.to_csv('.\\out\\resident.csv', index=False)