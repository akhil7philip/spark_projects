import os
import time
import json
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler

from helper_funcs.producer import Producer
from helper_funcs.transform import transform_publish_records

import logging
logger = logging.getLogger(__name__)

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

class PySparkTransform():

    def __init__(self, batch_interval, source_topic, sink_topic, bootstrap_servers) -> None:
        self.batch_interval, self.source_topic, self.sink_topic, self.bootstrap_servers = batch_interval, source_topic, sink_topic, bootstrap_servers

    def main(self):

        # Create Spark Session
        global spark
        spark = SparkSession \
            .builder \
            .master('local[1]') \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
            .getOrCreate()

        print("INFO: Successful created Spark Session")

        print("INFO: Reading data from source topic")
        spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", self.source_topic) \
            .option("startingOffsets", "earliest") \
            .load()\
            .writeStream \
            .outputMode("append") \
            .trigger(processingTime=self.batch_interval) \
            .foreachBatch(self.foreach_batch_function) \
            .start() \
            .awaitTermination()

    def foreach_batch_function(self, df, epoch_id):
        # Transform and write batchDF
        print("INFO: Successfully received data")
        df = df.selectExpr("CAST(value AS STRING)") \
        .select(json_tuple(col("value"),'t', 'id', 'v')) \
        .toDF('t', 'id', 'v') \
        .filter((col('id') == 'Q') | (col('id') == 'W') | (col('id') == 'E') | (col('id') == 'R') | (col('id') == 'T')) \
        .select(
            from_unixtime(col("t")/1000,"yyyy-MM-dd").alias('date'),
            col('id'), 
            col('v').cast(IntegerType()).alias('v')) \
        .groupBy(['date','id']).agg({'v': 'sum'}).withColumnRenamed("sum(v)", "v") 
        
        print("INFO: Completed basic filtering and transformation of data")
        if df.head(1):
            transform_publish_records(df.toPandas())
        else:
            print("INFO: waiting for data")






if __name__ == '__main__':
    
    batch_interval      = '10 seconds'
    source_topic        = 'test_314'
    sink_topic          = 'test_159'
    bootstrap_servers   = 'localhost:9092'

    o = PySparkTransform(batch_interval, source_topic, sink_topic, bootstrap_servers)
    o.main()
