#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path
import uuid
from datetime import datetime

# generate case
df_case = pd.DataFrame(
    columns=["caseId", "nric", "passType", "nationality", "race", "name", "birthDt", "age",
             "gender", "diagnosedDate", "active", "activeStatus", "importedCase", "importedFromCountry",
             "hospitalizedHospital", "admittedDt", "dischargedDt", "deceased", "deceasedDt", "createdDttm",
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

    df_case = df_case.append({'caseId': caseId,
                              'nationality': nationality,
                              'age': age,
                              'gender': gender,
                              'diagnosedDate': diagnosedDt,
                              'importedCase': importedCase,
                              'hospitalizedHospital': hospitalizedHospital,
                              'dischargedDt': dischargedDt,
                              'deceasedDt': deadthDt,
                              'lastUpdatedDttm': datetime.now()},
                             ignore_index=True)

place_file_path = Path('in/interim/case.csv')
df_case.to_csv(place_file_path, index=False)


