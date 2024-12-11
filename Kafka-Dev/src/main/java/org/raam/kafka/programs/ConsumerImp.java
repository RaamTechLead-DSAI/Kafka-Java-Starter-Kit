package org.raam.kafka.programs;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerImp {
    private static final Logger log = LoggerFactory.getLogger(ConsumerImp.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Consumer");

        String groupId = "java-application";
        String topic = "Dev_Topic_02";

        // Create Consumer Properties
        Properties properties = new Properties();

        // Connect to Local Host
        properties.setProperty("bootstrap.servers", "localhost:9092");

        //Create Consumer Configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset","earliest");

        // Create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        // Poll for data
        while (true) {
            log.info("Consumer: Polling .....");

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record:records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }
}
