#!/bin/bash

TOPIC_NAME=${1:-test-topic}

MSYS_NO_PATHCONV=1 docker exec -it kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic "$TOPIC_NAME" \
  --from-beginning \
  --max-messages 20 \
  --property print.offset=true \
  --property print.timestamp=true \
  --bootstrap-server localhost:9092