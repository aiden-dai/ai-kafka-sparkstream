# ai-kafka-sparkstream
Demo of using Spark Structured Stream to consume kafka topics

Tested Version:
- Spark Version: 2.4.5
- Kafka: 2.12-2.4.0

## Start Zookeeper and Kafka

Start zookeeper in docker
```
docker-compose up -d
```

## Run the Demo

Open one terminal and run:

```
cd sender
python app.py
```

Open another terminal and run
```
cd sparkstream
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 app.py
```