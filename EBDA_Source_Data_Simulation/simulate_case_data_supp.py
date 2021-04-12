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
case_file_path = Path('in/interim/case.csv')
df_case= pd.read_csv(case_file_path)

# Get case summary
case_sum_file_path = Path('in/case/sg_covid_daily_sum.csv')
df_case_sum = pd.read_csv(case_sum_file_path)

# simulate the rest of days, from 2020/04/20

# create lookup table
def lookup_nationality(rand):
    nationality = 'Others' 
    
    if rand <=0.015:
        nationality= 'British'
    elif rand <= 0.03:
        nationality = 'Filipino'
    elif rand <= 0.048:
        nationality = 'American'
    elif rand <= 0.048:
        nationality = 'Myanmarian'
    elif rand <= 0.097:
        nationality = 'Malaysian'
    elif rand <= 0.132:
        nationality = 'Chinese'
    elif rand <= 0.324:
        nationality = 'Singaporean'
    elif rand <= 0.564:
        nationality = 'Indian'
    elif rand <= 1:
        nationality = 'Bangladeshi'
    return nationality
        
def lookup_age(rand):
    age = 30
    
    if rand <=0.004:
        age= rd.randint(81, 100)
    elif rand <= 0.01:
        age = rd.randint(1, 10)
    elif rand <= 0.02:
        age = rd.randint(71, 80)
    elif rand <= 0.035:
        age = rd.randint(11, 20)
    elif rand <= 0.066:
        age = rd.randint(61, 70)
    elif rand <= 0.127:
        age = rd.randint(51, 60)
    elif rand <= 0.285:
        age = rd.randint(41, 50)
    elif rand <= 0.63:
        age = rd.randint(21, 30)
    elif rand <= 1:
        age = rd.randint(31, 40)
    return age

def lookup_gender(rand):
    gender = 'male'
    
    if rand <=0.11:
        gender = 'female'
    elif rand <= 1:
        gender = 'male'
    return gender

def lookup_imported(rand):
    imported = 'local'
    
    if rand <=0.12:
        imported = 'imported'
    elif rand <= 1:
        imported = 'local'
    return imported

def choose_hopital():
    hospital_list = ['Singapore General Hospital', 'Changi General Hospital',
                     'National Centre for Infectious Diseases','Sengkang General Hospital',
                     'Khoo Teck Puat Hospital','Ng Teng Fong General Hospital',
                     'TTSH','Others']
    return rd.choice(hospital_list)
    

case_sum_row_count = df_case_sum.shape[0]
for i in range(88, case_sum_row_count):
    case_num = df_case_sum['Daily Confirmed'][i]
    
    for j in range(case_num):    
        rand = rd.random()
        caseId = uuid.uuid4()
        nationality = lookup_nationality(rand)
        age = lookup_age(rand)
        gender =lookup_gender(rand)
        imported = lookup_imported(rand)
        diagnosedDate = df_case_sum['Date'][i]
        hospitalizedHospital = choose_hopital()
        dischargedDt = datetime.strptime(diagnosedDate, "%Y-%m-%d") + timedelta(days=rd.randint(6,20))
        lastUpdatedDttm = datetime.now()
        
        df_case = df_case.append({'caseId': caseId, 
                                  'nationality':nationality,
                                  'age':age,
                                  'gender':gender,
                                  'diagnosedDate':diagnosedDate,
                                  'importedCase':imported,
                                  'hospitalizedHospital': hospitalizedHospital,
                                  'dischargedDt': dischargedDt,
                                  'lastUpdatedDttm': lastUpdatedDttm},
                                 ignore_index=True)

        print(df_case_sum['Date'][i])


place_file_path = Path('out/case.csv')
df_case.to_csv(place_file_path, index=False)


