package com.learnkafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProducer {

    private static final Logger log = LoggerFactory.getLogger(MessageProducer.class);

    String topicName = "my-topic";

    KafkaProducer<String, String> kafkaProducer;

    public MessageProducer(Map<String, Object> propsMap) {
        kafkaProducer = new KafkaProducer<String, String>(propsMap);
    }

    public static Map<String, Object> propsMap() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092,localhost:29093,localhost:29094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 10000);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);

        return props;
    }

    Callback callback = (metadata, exception) -> {
        if (exception != null) {
            log.error("Error in Callback is {}", exception.getMessage());
        } else {
            log.info("Callback is {} {}", metadata.offset(), metadata.partition());
        }
    };


    public void publishMessageASync(String value, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, value, message);

        kafkaProducer.send(record, callback);

    }

    public void pushMessageSync(String key, String value) {

        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

        log.info("Sending Kafka message. topic={}, key={}, value={}", topicName, key, value);

        try {

            RecordMetadata metadata = kafkaProducer.send(record).get();

            log.info("Kafka message sent. topic={}, partition={}, offset={}", metadata.topic(), metadata.partition(), metadata.offset());

        } catch (InterruptedException | ExecutionException e) {

            log.error("Kafka send failed. topic={}, key={}", topicName, key, e);
        }
    }

    public static void main(String[] args) throws InterruptedException {

        MessageProducer messageProducer = new MessageProducer(propsMap());

        //messageProducer.pushMessageSync(null, "value1");

        messageProducer.publishMessageASync("1", "2");
        Thread.sleep(3000);
    }

}
