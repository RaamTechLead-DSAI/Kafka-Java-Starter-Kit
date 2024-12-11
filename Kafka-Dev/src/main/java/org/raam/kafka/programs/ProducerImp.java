package org.raam.kafka.programs;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerImp {
    private static final Logger log = LoggerFactory.getLogger(ProducerImp.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Producer");

        // Create Producer Properties
        Properties properties = new Properties();

        // Connect to Local Host
        properties.setProperty("bootstrap.servers", "localhost:9092");

        //Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create a producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("Dev_Topic_01", "Message 1");

        // Send Data (Asynchronous)
        producer.send(producerRecord);

        // Tell producer to send all data and block until completion (Synchronous)
        producer.flush();

        // Close Producer
        producer.close();
    }
}
