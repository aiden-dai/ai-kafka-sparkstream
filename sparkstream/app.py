'''Use Spark StructedStream to consume kafka topics'''
from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType

if __name__ == "__main__":

    bootstrapServers = '192.168.56.107:9092'
    topics = 'user_log'

    spark = SparkSession\
        .builder\
        .appName("test_kafka")\
        .getOrCreate()

    df = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option('subscribe', topics)\
        .option("startingOffsets", "earliest")\
        .load()\
        .selectExpr("CAST(value AS STRING)")

    df.printSchema()

    schema = StructType()\
        .add("user_id", "integer")\
        .add("item_id", "integer")\
        .add("behavior_type", "integer")\
        .add("item_category", "integer")\
        .add("date", "date")\
        .add("hour", "integer")

    # Convert String into Json into columns
    event = df.select(from_json(df.value, schema).alias(
        'event')).select('event.*')
    event.printSchema()

    count_by_hour = event.groupBy('hour').count().sort('hour')

    # Process Every 5 seconds.
    query = count_by_hour.writeStream\
        .outputMode("complete")\
        .format('console')\
        .trigger(processingTime='5 seconds')\
        .start()

    query.awaitTermination()
