#!/bin/bash

TOPIC_NAME=${1:-test-topic}

if MSYS_NO_PATHCONV=1 docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic "$TOPIC_NAME" \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
then
  echo "Topic '$TOPIC_NAME' created."
else
  echo "Failed to create topic '$TOPIC_NAME'."
fi