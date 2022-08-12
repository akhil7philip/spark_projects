# Spark project (using PySpark DataFrames, Kafka and Pandas)

## About:
This project uses Spark (using Python) to deal with streaming data from Kafka. Using Dataframes, we collect streaming JSON data from a source topic, convert it to a Pandas dataframe and publish into a sink topic using Kafka.

## Steps:

1. Create a conda env; install pyspark and dependencies:

* `conda create -n pyspark_3.3.0 python=3.7.0`
* `conda activate pyspark_3.3.0`
* `pip install pyspark==3.3.0`
* `pip install kafka-python`
* `pip install numpy`
* `pip install pandas`

* Here `pip freeze` gives:
    * `kafka-python==2.0.2`
    * `numpy==1.21.6`
    * `pandas==1.1.5`
    * `py4j==0.10.7`
    * `pyspark==3.3.0`
    * `python-dateutil==2.8.2`
    * `pytz==2022.2`
    * `six==1.16.0`

2. Run the docker YAML file to start zookeeper and kafka

* `cd PySparkDataFrames`
* `docker compose up`

3. Run the producer.py script to produce random data

* `python helper_funcs/producer.py`

4. Run the main.py script to read data from source topic, transform data and publish to sink topic

* `python main.py`

5. Run consumer.py to finally read transformed data from sink topic

* `python helper_funcs/consumer.py`

6. Check the `output.md` file for expected output

## References:

* https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
* https://spark.apache.org/docs/3.1.1/api/python/_modules/pyspark/sql/streaming.html
* https://spark.apache.org/docs/3.1.1/api/python/_modules/pyspark/sql/column.html
* https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html
* https://sparkbyexamples.com/ 
