#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path
import uuid

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
df_case = pd.DataFrame(
    columns=["caseId", "nric", "passType", "nationality", "race", "name", "birthDt", "age",
             "gender", "diagnosedDate", "active", "activeStatus", "importedCase", "importedFromCountry",
             "hospitalizedHospital", "admittedDt", "dischargedDt", "deceased", "deadthDt", "createdDttm",
             "lastUpdatedDttm"])

case_file_path = Path('in/case/covid-sg.json')
df_src = pd.read_json(case_file_path)

df_src = pd.json_normalize(df_src['features'])

# after 19 Apr, there is large number of missing fields, as the data is not available
df_src = df_src.head(6587)

for i in range(0, df_src.shape[0]):
    caseId = uuid.uuid4()
    nationality = df_src['properties.nationality'][i]


    age = df_src['properties.age'][i]
    gender = df_src['properties.gender'][i]
    diagnosedDt = df_src['properties.confirmed'][i]

    importedCase = df_src['properties.transmissionSource'][i]

    hospitalizedHospital = df_src['properties.hospital'][i]
    dischargedDt = df_src['properties.discharged'][i]
    deadthDt = df_src['properties.death'][i]


    if (nationality[0:9] == 'Singapore' and diagnosedDt == '2020-04-19'):
        nric = ls_sg.pop()
    elif(diagnosedDt == '2020-04-19'):
        nric = ls_fg.pop()
    else:
        nric=''

    df_case = df_case.append({'caseId': caseId,
                              'nric':nric,
                              'nationality': nationality,
                              'age': age,
                              'gender': gender,
                              'diagnosedDate': diagnosedDt,
                              'importedCase': importedCase,
                              'hospitalizedHospital': hospitalizedHospital,
                              'dischargedDt': dischargedDt,
                              'deadthDt': deadthDt
                              },
                             ignore_index=True)

place_file_path = Path('out/case.csv')
df_case.to_csv(place_file_path, index=False)


