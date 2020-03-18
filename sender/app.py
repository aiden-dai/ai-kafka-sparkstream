'''Use Spark to process the csv file and send the records to a kafka topic.'''
import time
import json

from confluent_kafka import Producer

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')


def send(producer, row):

    producer.poll(0)

    data = {
        'user_id': row.user_id,
        'item_id': row.item_id,
        'behavior_type': row.behavior_type,
        'item_category': row.item_category,
        'date': '2019-{}'.format(row.time[5:10]),
        'hour': int(row.time[-2:])
    }


    # Asynchronously produce a message, the delivery report callback
    producer.produce('user_log', json.dumps(data).encode(
        'utf-8'), callback=delivery_report)

    # Add delay of 0.1 seconds per records.
    # time.sleep(0.1)


def main():
    spark = SparkSession.builder.appName('Sender').getOrCreate()

    schema = StructType()\
        .add("user_id", "integer")\
        .add("item_id", "integer")\
        .add("behavior_type", "integer")\
        .add('user_geohash', 'string')\
        .add("item_category", "integer")\
        .add("time", "string")

    df = spark.read.csv('data/sample.csv', header="true",
                        schema=schema).sort('time', ascending=True)
    # df = spark.read.csv('data/tianchi_mobile_recommend_train_user.csv', header="true", schema=schema)

    # Show schema
    df.printSchema()

    # Show total number of records
    count = df.count()
    print(count)

    p = Producer({'bootstrap.servers': '192.168.56.107:9092'})

    start = time.time()

    for row in df.collect():
        send(p, row)

    p.flush()

    end = time.time()
    total_time = end - start

    print(f'Total time: {str(total_time)}')

if __name__ == "__main__":
    main()
