# ai-kafka-sparkstream
Demo of using Spark Structured Stream to consume kafka topics

Tested Version:
- Spark Version: 2.4.5
- Kafka: 2.12-2.4.0

## Start Kafka

Start zookeeper in docker
```
docker run --rm -d --name zookeeper \
-p 2181:2181 \
digitalwonderland/zookeeper
```

Start kafka in docker with a topic 'user_log'
```
docker run --rm -d --name kafka \
--link zookeeper -p 9092:9092 \
-e KAFKA_ADVERTISED_HOST_NAME=192.168.56.107 \
-e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
-e KAFKA_ADVERTISED_PORT=9092 \
-e KAFKA_BROKER_ID=1 \
-e KAFKA_CREATE_TOPICS="user_log" \
wurstmeister/kafka:2.12-2.4.0
```
> Or you can use docker-compose to start them together

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