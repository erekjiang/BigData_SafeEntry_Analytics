#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path
import uuid
from datetime import datetime
from datetime import timedelta
import random as rd

# find NRIC
df_resident =pd.read_csv(Path('out/resident.csv'))
ls_nric = df_resident.loc[:,'nric'].tolist()

ls_sg = []
ls_fg = []

for i in range(len(ls_nric)):
    if ls_nric[i][0] == 'S':
        ls_sg.append(ls_nric[i])
    elif ls_nric[i][0] == 'F':
        ls_fg.append(ls_nric[i])
        
# Get interim result
case_file_path = Path('out/case_full.csv')
df_case= pd.read_csv(case_file_path)

start_row = 60557

for j in range(start_row, df_case.shape[0]):
    nationality = df_case['nationality'][j]
    if (nationality[0:9] == 'Singapore'):
        nric = ls_sg.pop()
    else:
        nric = ls_fg.pop()
        
    df_case['nric'][j] = nric

place_file_path = Path('out/case_full.csv')
df_case.to_csv(place_file_path, index=False)
    