#!/usr/bin/env bash

cd ~/workspace/kafka_bin/kafka_2.12-2.5.0/

# create topic
#./bin/kafka-topics.sh --create \
#  --bootstrap-server my-kafka:9092 \
#  --topic spark_practice

# producer
./bin/kafka-console-producer.sh --bootstrap-server my-kafka:9092 \
  --topic spark_practice

# consumer
./bin/kafka-console-consumer.sh --bootstrap-server my-kafka:9092 \
  --topic spark_practice
