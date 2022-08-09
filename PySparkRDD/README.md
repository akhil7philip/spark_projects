# Spark project (using PySpark RDD, Kafka and Pandas)

## About:
This project uses Spark (using Python) to deal with streaming data from Kafka. Using RDD, we collect streaming JSON data from a source topic, read it as a Pandas dataframe and publish into a sink topic.

## Steps:

1. Create a conda env and install pyspark

`conda create -n pyspark_2.4.6 python=3.7.0`
`conda activate pyspark_2.4.6`
`pip install pyspark==2.4.6`
`pip install kafka-python`
`pip install numpy`

2. Run the docker YAML file to start zookeeper and kafka

`docker compose up`

3. Run the producer.py script to produce random data

`python helper_funcs/producer.py`

4. Run the main.py script to read data from source topic, transform data and publish to sink topic

`python main.py`

5. Run consumer.py to finally read transformed data from sink topic

`python helper_funcs/consumer.py`

6. Check the `output.md` file for expected output

## References:

1. https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
2. https://spark.apache.org/docs/latest/streaming-programming-guide.html#transformations-on-dstreams
3. https://www.oreilly.com/library/view/learning-spark/9781449359034/ch04.html
4. https://sandeepkattepogu.medium.com/streaming-data-from-apache-kafka-topic-using-apache-spark-2-4-5-and-python-4073e716bdca
