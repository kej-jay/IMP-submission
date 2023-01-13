import pandas as pd
from time import sleep
import requests
import json
from datetime import datetime

#needs the two raw data files in the same folder 
df_lines = pd.read_csv("data/GazesMappingToLinesAndCols.csv")
df_gazes = pd.read_csv("data/P01_imotions.csv",skiprows=29)
df_merged = pd.merge(df_gazes, df_lines, on="Row", how='left')
df_filtered = df_merged[['Row','Timestamp_x','SourceStimuliName','ET_PupilLeft','ET_PupilRight','Timestamp_y','source_file_line','source_file_col']]
df_filtered = df_filtered.fillna(0)
df_filtered = df_filtered[df_filtered['SourceStimuliName'] == 't2_eclipse (B_11.java)']


URL = 'http://80.65.211.84:1112'

for i in range(10000):
    print(df_filtered.iloc[i]['source_file_line'])
    print(i)
    requests.post(
        url=URL + "/data",
        data=json.dumps({
            'timestamp': df_filtered.iloc[i]['Timestamp_x'],
            'type': 'test',
            'subject': 'subject01',
            'study': 'simulator',
            'diameter': {
                'left': df_filtered.iloc[i]['ET_PupilLeft'] ,
                'right':df_filtered.iloc[i]['ET_PupilRight'],
            },
            'line': int(df_filtered.iloc[i]['source_file_line']),
            'column': int(df_filtered.iloc[i]['source_file_col'])
        })
    )
    print("Sent")
    print(datetime.now())
    sleep(0.008)