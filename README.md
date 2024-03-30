# Visualizing-Streaming-IoT-Data-of-a-Smart-Building-System

# Dataset

https://www.kaggle.com/datasets/ranakrc/smart-building-system

![image](https://github.com/Sinan-Simsek/Visualizing-Streaming-IoT-Data-of-a-Smart-Building-System/assets/108238930/4b78cdc8-d6bc-4347-970d-b0b37bdebf85)

# Basic Architecture

![image](https://github.com/Sinan-Simsek/Visualizing-Streaming-IoT-Data-of-a-Smart-Building-System/assets/108238930/5c175735-f359-42d7-af3a-c70a11fd6e0b)


# Navigation

![image](https://github.com/Sinan-Simsek/Visualizing-Streaming-IoT-Data-of-a-Smart-Building-System/assets/108238930/a8597b49-fff5-4fa0-af31-8923c0a45da6)

# Project Plan

# Step 1 >> Preprocessing the Dataset with PySpark

## Importing Libraries:

The code imports necessary libraries including os, findspark, and various components from pyspark.sql and pyspark.sql.types.

## Initializing SparkSession:
It initializes a SparkSession, which is the entry point to programming Spark with the DataFrame and Dataset API. The SparkSession is configured to run locally using 2 cores.

## Reading Files and Creating DataFrames:
The code reads sensor data files from a specified directory path.
It creates separate DataFrames for each sensor type in each room. The data is read from CSV files, and each DataFrame consists of columns 'ts_min_bignt' (timestamp in bigint format) and one sensor reading column (e.g., 'co2', 'humidity', etc.).

## Creating Temporary Views:
Temporary views are created for each DataFrame, enabling Spark SQL queries to be performed on these DataFrames.

## SQL Query to Join DataFrames:
The code constructs a SQL query to join the individual sensor DataFrames on the timestamp column (ts_min_bignt). This query effectively merges the sensor readings into a single DataFrame per room.
Merging and Sorting DataFrames:

The code merges all room DataFrames into a single DataFrame (df_merged). It uses reduce function to perform a union operation on the DataFrames.
The merged DataFrame is sorted based on the timestamp values (ts_min_bignt) in ascending order.

## Converting Timestamp and Writing to CSV:
Timestamps are converted from ts_min_bignt to a human-readable format (yyyy-MM-dd HH:mm:ss) and added as a new column named event_ts_min.
Finally, the merged and sorted DataFrame is converted to a Pandas DataFrame and written to a CSV file named sensors.csv in a specified directory.

# Step 2 >> Producing Data to Kafka Topic

## Imports:
The script imports necessary libraries including pandas for data manipulation and KafkaProducer from the kafka library for interacting with Kafka.

## Argument Parsing:
The script defines command-line arguments using the argparse module. These arguments specify various parameters such as the input file path, delimiter, Kafka topic, bootstrap servers, etc.

## DataFrameToKafka Class:
This class encapsulates the functionality to read the source file, transform the DataFrame, and send rows to Kafka.
The constructor initializes the class attributes based on the provided arguments and reads the source file into a Pandas DataFrame.
The turn_df_to_str() method converts each row of the DataFrame into a string representation, with columns separated by kafka_sep.
The read_source_file() method reads the source file (either CSV or Parquet) based on the specified extension and returns a processed DataFrame.
The df_to_kafka() method iterates over each row of the DataFrame, sends it to Kafka, and sleeps for a specified duration (row_sleep_time) between each row.

## Main Execution:
In the main block, command-line arguments are parsed, and an instance of the DataFrameToKafka class is created with the provided arguments.
Finally, the df_to_kafka() method is called to send the DataFrame rows to Kafka.

# Step 3 >> Consuming Data with Spark Streaming and Write it to ElasticSearch

## Imports and Setup:
The script imports necessary libraries such as logging, findspark, Elasticsearch, helpers from elasticsearch, SparkSession, functions, and types from pyspark.sql.
It initializes logging configuration, suppresses warnings, and defines a checkpoint directory.

## Spark Session Creation:

It creates a Spark session with specific configurations such as memory allocation, serializer, and necessary packages for Kafka and Elasticsearch integration.
## Initial DataFrame Creation:
The script initializes a DataFrame by reading streaming data from a Kafka topic (office-input) using Spark's readStream API.

## DataFrame Transformation:
It modifies the initial DataFrame to extract relevant fields from the Kafka message value.
Timestamps are converted from string to Unix timestamp format.
A final DataFrame is created by applying transformations and defining a SQL view (df4).

## Elasticsearch Connection:
The script establishes a connection to Elasticsearch running on localhost:9200.

## Index Creation:
It checks if the Elasticsearch index named office_input exists. If not, it creates the index with specified mappings and settings.

## Streaming to Elasticsearch:
The script starts the streaming process, writing the data to the Elasticsearch index (office_input/_doc) using the writeStream API.
It specifies the output mode as append, sets options for Elasticsearch connection, and defines a checkpoint location for fault tolerance.
The streaming query is initiated and awaits termination.

# Step 4 >> Creating Kibana Visualizations based on streaming data
![image](https://github.com/Sinan-Simsek/Visualizing-Streaming-IoT-Data-of-a-Smart-Building-System/assets/108238930/3208382b-ab12-4fd2-903f-8400e3e2e607)
