from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
#imports for lhipa
import math
import pywt
import numpy as np
import uuid

def modmax(d):
    m = [0.0]*len(d)
    for i in iter(range(len(d))):
        m[i] = math.fabs(d[i])

        t = [0.0]*len(d)
        for i in iter(range(len(d))):
            ll = m[i-1] if i >= 1 else m[i]
            oo = m[i]
            rr = m[i+1] if i < len(d) - 2 else m[i]

            if (ll <= oo and oo >= rr) and (ll < oo or oo > rr):
                t[i] = math.sqrt(d[i]**2)
            else:
                t[i] = 0.0

    return t

def lhipa(d):
    w = pywt.Wavelet('sym16')
    maxlevel = pywt.dwt_max_level(len(d), filter_len=w.dec_len)
    print("maxlevel is")
    print(maxlevel)

    hif, lof = 1, int(maxlevel/2)

    cD_H = pywt.downcoef('d', d, 'sym16', 'per', level=hif)
    cD_L = pywt.downcoef('d', d, 'sym16', 'per', level=lof)

    cD_H[:] = [x/math.sqrt(2**hif) for x in cD_H]
    cD_L[:] = [x/math.sqrt(2**lof) for x in cD_L]

    cD_LH = cD_L
    for i in range(len(cD_L)):
        cD_LH[i] = cD_L[i] / cD_H[int(((2**lof)/(2**hif))*i)]

    cD_LHm = modmax(cD_LH)

    luniv = np.std(cD_LHm) * math.sqrt(2.0*np.log2(len(cD_LHm)))
    cD_LHt = pywt.threshold(cD_LHm, luniv, mode="less")

    tt = 10#d[-1].timestamp() - d[0].timestamp()

    ctr = 0
    for i in iter(range(len(cD_LHt))):
        if math.fabs(cD_LHt[i]) > 0: ctr += 1

    return float(ctr)/tt


def computeLHIPA(pdf):
    pdf["LeftPupil"] = pd.to_numeric(pdf["LeftPupil"])
    pdf["RightPupil"] = pd.to_numeric(pdf["RightPupil"])
    pdf['diameter'] = (pdf['LeftPupil'] + pdf['RightPupil']) / 2
    lhipa_score = lhipa(pdf['diameter'].to_numpy())
    df = pd.DataFrame(data = {'lhipa': [lhipa_score]})
    return df

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Kafka Pyspark Example")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    KAFKA_TOPIC_NAME = "gazeData"
    KAFKA_BOOTSTRAP_SERVER = "kafka:9092"
    sampleDataframe = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
            .option("subscribe", KAFKA_TOPIC_NAME)
            .option("startingOffsets", "latest")
            #consider setting the max trigger delay
            .option("minOffsetsPerTrigger",5)
            .option("maxOffsetsPerTrigger",500)
            .load()
        )
    base_df = sampleDataframe.selectExpr("CAST(value as STRING)","timestamp")
    sample_schema = (
        StructType()
        .add("Row", StringType())
        .add("Timestamp1", StringType())
        .add("Line", StringType())
        .add("Column", StringType())
        .add("LeftPupil",StringType())
        .add("RightPupil",StringType())

        
    )
    info_dataframe = base_df.select(from_json(col("value"), sample_schema).alias("sample"), "timestamp")
    info_df_fin = info_dataframe.select("sample.*", "timestamp")
    info_df_fin = info_df_fin.withColumn("timestamp", col("timestamp").cast(TimestampType()))
    
    #LHIPA calculation
    #This is the LHIPA implementation based on the tumbling window described in the report.
    #The window size of 10 minutes was chosen since this guarantees that all datapoints in one batch are considered as 10 minutes is way bigger than one batch
    #Since this implementatio was not performant enough, it is commented out.
    #windowed = info_df_fin.groupBy(window("timestamp", "10 minutes")).applyInPandas(computeLHIPA,schema = "lhipa long")
    #windowed.writeStream.format("console").outputMode("append").start().awaitTermination()
    
    
    #Line Aggregation
    line_count = info_df_fin.groupBy("Line").agg(count(col("Row")).alias("value"))
    
    
    #kafka sink
    line_count\
    .selectExpr("to_json(struct(*)) AS value")\
    .writeStream\
    .format("kafka")\
    .outputMode("complete")\
    .option("kafka.bootstrap.servers","kafka:9092")\
    .option("topic", "lineCounts")\
    .option("checkpointLocation","/tmp/"+str(uuid.uuid4()))\
    .start()\
    .awaitTermination()