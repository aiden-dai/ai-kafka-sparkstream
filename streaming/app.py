'''Use Spark StructedStream to consume kafka topics'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType


def main():

    servers = '192.168.56.107:9092'
    topics = 'user_log'

    spark = SparkSession\
        .builder\
        .appName("streaming")\
        .getOrCreate()

    df = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", servers)\
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

    # Process Every 30 seconds.
    query = count_by_hour.writeStream\
        .outputMode("complete")\
        .format('console')\
        .trigger(processingTime='30 seconds')\
        .start()

    query.awaitTermination()


if __name__ == "__main__":

    main()
