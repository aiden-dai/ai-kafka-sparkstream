# ai-spark-kafka

This repo is a demo of using Spark and Kafka together.

Tested Version:
- Spark Version: 2.12-2.4.5
- Kafka: 2.12-2.4.0

The data file used in this demo is a sample of a public dataset from Alibaba which is about User Behavior Data on Taobao App.

> Orginal data file can be downloaded from: https://tianchi.aliyun.com/dataset/dataDetail?dataId=46


## Start Zookeeper and Kafka

Start zookeeper and kafka in docker
```
docker-compose up -d
```


## Sender task

Use Spark to process the csv and send the events to Kafka.

Open one terminal and run:
```
python sender/app.py
```


## Streaming task

Use spark structured streaming to consume the events from Kafka.
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 streaming/app.py
```


## Recommendation task

Use Spark ALS to do item recommendations based on the events. 

- Train the data
```
python recommendation/train.py
```

- Test recommendation
```
python recommendation/predict.py
```
