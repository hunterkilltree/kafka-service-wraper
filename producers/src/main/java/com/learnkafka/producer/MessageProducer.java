package com.learnkafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MessageProducer {

    String topicName = "hello";

    KafkaProducer<String, String> kafkaProducer;

    public MessageProducer(Map<String, Object> propsMap) {
        kafkaProducer = new KafkaProducer<String, String>(propsMap);
    }

    public static Map<String, Object> propsMap() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    public void pushMessageSync(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

        try {
            RecordMetadata recordMetadata = kafkaProducer.send(record).get();
            System.out.println("partition " + recordMetadata.partition() + ", offset : " + recordMetadata.offset());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        MessageProducer messageProducer = new MessageProducer(propsMap());

        messageProducer.pushMessageSync("hello-0", "value1");
    }

}
