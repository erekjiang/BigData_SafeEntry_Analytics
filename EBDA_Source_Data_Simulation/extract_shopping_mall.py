# -*- coding: utf-8 -*-
"""
Created on Tue Apr 13 22:48:38 2021

@author: cmbsguser
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

mall_file_path = Path('in/one-map/database.json')
df_mall = pd.read_json(mall_file_path)

df_mall = df_mall[df_mall['BUILDING'].str.contains("MALL")]

df_mall = df_mall.drop_duplicates('BUILDING', keep=False)