#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path
import uuid
from datetime import datetime
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

# generate case
#df_case = pd.DataFrame(
#    columns=["caseId", "nric", "passType", "nationality", "race", "name", "birthDt", "age",
#             "gender", "diagnosedDate", "active", "activeStatus", "importedCase", "importedFromCountry",
#             "hospitalizedHospital", "admittedDt", "dischargedDt", "deceased", "deceasedDt", "createdDttm",
#             "lastUpdatedDttm"])

# Get interim result
case_file_path = Path('in/interim/case.csv')
df_case= pd.read_csv(case_file_path)

# Get case summary
case_sum_file_path = Path('in/case/sg_covid_daily_sum.csv')
df_case_sum = pd.read_csv(case_sum_file_path)

# simulate the rest of days, from 2020/04/20

# create lookup table
# lookup table nationality
def lookup_nationality(rand):
    data = [[0.015, 'British'], [0.03, 'Filipino'], [0.048, 'American'], [0.068, 'Myanmarian']
            [0.097, 'Malaysian'], [0.132, 'Chinese'], [0.324, 'Singaporean'], [0.564, 'Indian'], [1, 'Bangladeshi']]
    df_lookup = pd.DataFrame(data, columns=['cum_per', 'nationality'])


case_sum_row_count = df_case_sum.shape[0]
for i in range(90, case_sum_row_count +1):
    case_num = df_case_sum['Daily Confirmed'][i]

    for i in range(case_num):
        print(i)





place_file_path = Path('out/case_full.csv')
df_case.to_csv(place_file_path, index=False)


