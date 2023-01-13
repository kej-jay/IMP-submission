from kafka import KafkaProducer
import time
import json
import pandas as pd
from datetime import datetime
producer = KafkaProducer(bootstrap_servers=['localhost:9094'])
#producer = KafkaProducer(bootstrap_servers=['80.65.211.84:9094'])

if __name__=='__main__':
    df = pd.read_csv("./data.csv")
    for i in range(3000):
        if i%100==0:
            print("{} GAZEPOINTS SENT TO KAFKA".format((str(i))))
        #print("Sending data to Kafka")
        data = json.dumps({
            "Row": str(df.iloc[i]['Row']),
            "Timestamp": str(df.iloc[i]['Timestamp_x']),
            "Line": str(df.iloc[i]['source_file_line']),
            "Column": str(df.iloc[i]['source_file_col']),
            "LeftPupil": str(df.iloc[i]['ET_PupilLeft']),
            "RightPupil": str(df.iloc[i]['ET_PupilRight'])

        }).encode("utf-8")
        #print(data)
        producer.send('gazeData',data)
        time.sleep(0.008)
    print("#####DONE REPLAYING DATA####")
    print(datetime.now())
