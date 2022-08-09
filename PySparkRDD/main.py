import os
spark_version = '3.1.2'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:{} --jars ./jars/spark-streaming-kafka-0-8-assembly_2.11-2.4.8.jar pyspark-shell'.format(spark_version)

import json
import numpy as np
import logging
from pyspark import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext 
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import *
from pyspark.sql.types import *

from helper_funcs.producer import Producer
from helper_funcs.transform import transform_publish_records

logger              = logging.getLogger(__name__)
n_secs              = 10

source_topic        = 'test_124'
sink_topic          = 'test_248'
bootstrap_servers   = 'localhost:9092'

conf                = SparkConf().setMaster('local[*]')
sc                  = SparkContext(conf=conf) 
sc.setLogLevel('WARN')
ssc                 = StreamingContext(sc,n_secs)
sqlContext          = SQLContext(sc)

kafkaStream         = KafkaUtils.createDirectStream(
    ssc,\
    topics          = [source_topic],\
    kafkaParams     = {'bootstrap.servers':bootstrap_servers}) 



if __name__ == '__main__':

    '''
    Steps:
    1. run producer function to generate random data to source_topic
    2. run main function to consume from source_topic
    3. call transform_public_records function to transform raw data into desired form
    4. finally publish data to sink_topic
    '''

    records = kafkaStream.map(lambda v: json.loads(v[1])) \
        .map(lambda dict: (dict['id'],dict['v'])) \
        .reduceByKey(lambda x,y: np.max([x,y])) \
        .foreachRDD(transform_publish_records)
        
    ssc.start()
    ssc.awaitTermination()
    # time.sleep(60) # run stream for 10 mins in case of no detection of producer
    # ssc.stop(stopSparkContext=True, stopGraceFully=True)