import sys
import warnings
import traceback
import logging
import findspark
findspark.init("/opt/spark")
from elasticsearch import Elasticsearch, helpers
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_unixtime, unix_timestamp, year, month, dayofweek

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

warnings.filterwarnings('ignore')
checkpointDir = "file:///tmp/streaming/kafka_office_input"

office_input_index = {
    "settings": {
        "index": {
            "analysis": {
                "analyzer": {
                    "custom_analyzer":
                        {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": [
                                "lowercase", "custom_edge_ngram", "asciifolding"
                            ]
                        }
                },
                "filter": {
                    "custom_edge_ngram": {
                        "type": "edge_ngram",
                        "min_gram": 2,
                        "max_gram": 10
                    }
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "event_ts_min": {"type": "date",
            "ignore_malformed": "true"        
      },
            "co2": {"type": "float"},
            "humidity": {"type": "float"},
            "light": {"type": "float"},
            "temperature": {"type": "float"},
            "room": {"type": "keyword"},
            "pir": {"type": "float"},
            "if_movement": {"type": "keyword"}

        }
    }
}

try:
    spark = (SparkSession.builder
             .appName("Streaming Kafka-Spark") \
             .master("local[2]") \
             .config("spark.driver.memory", "2048m") \
             .config("spark.sql.shuffle.partitions", 4) \
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,io.delta:delta-core_2.12:2.4.0") \
             .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
             .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:7.12.1,commons-httpclient:commons-httpclient:3.1,io.delta:delta-core_2.12:2.4.0")   \
             .getOrCreate())

    logging.info('Spark session created successfully')
except Exception:
    traceback.print_exc(file=sys.stderr)
    logging.error("Couldn't create the spark session")

try:
    df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "172.18.0.12:9092") \
      .option("subscribe", "office-input") \
      .load()
    logging.info("Initial dataframe created successfully")
except Exception as e:
    logging.warning(f"Initial dataframe couldn't be created due to exception: {e}")

from pyspark.sql.types import IntegerType, FloatType, StringType
from pyspark.sql import functions as F
df2 = df.selectExpr("CAST(value AS STRING)")

df3 = df2.withColumn("id", F.split(F.col("value"), ",")[0].cast(IntegerType())) \
    .withColumn("ts_min_bignt", F.split(F.col("value"), ",")[1].cast(IntegerType())) \
    .withColumn("co2", F.split(F.col("value"), ",")[2].cast(FloatType())) \
    .withColumn("humidity", F.split(F.col("value"), ",")[3].cast(FloatType())) \
    .withColumn("light", F.split(F.col("value"), ",")[4].cast(FloatType())) \
    .withColumn("pir", F.split(F.col("value"), ",")[5].cast(FloatType())) \
    .withColumn("temperature", F.split(F.col("value"), ",")[6].cast(FloatType())) \
    .withColumn("room", F.split(F.col("value"), ",")[7].cast(IntegerType())) \
    .withColumn("event_ts_min", F.unix_timestamp(F.to_timestamp(F.split("value", ",")[8], "yyyy-MM-dd HH:mm:ss"))*1000) \
    .drop(F.col("value"))

df3.createOrReplaceTempView("df3")

df4 = spark.sql("""
select
  event_ts_min,
  co2,
  humidity,
  light,
  temperature,
  room,
  pir,
  case
    when pir > 0 then 'movement'
    else 'no_movement'
  end as if_movement
from df3

""")
logging.info("Final dataframe created successfully")

try:
    es = Elasticsearch("http://localhost:9200")
    logging.info(f"Connection {es} created succesfully")
except Exception as e:
    traceback.print_exc(file=sys.stderr)
    logging.error("Couldn't create the final dataframe")

if es.indices.exists(index="office_input"):
    es.indices.create(index="office_input", body=office_input_index)
    print("Index office_input created")
    logging.info("Index office_input created")
else:
    print("Index office_input already exists")
    logging.info("Index office_input already exists")

logging.info("Streaming is being started...")
my_query = (df4.writeStream
               .format("org.elasticsearch.spark.sql")
               .outputMode("append")
               .option("es.nodes", "localhost")
               .option("es.port", "9200")
               .option("es.resource", "office_input/_doc")
               .option("checkpointLocation", checkpointDir)
               .start())

my_query.awaitTermination()
