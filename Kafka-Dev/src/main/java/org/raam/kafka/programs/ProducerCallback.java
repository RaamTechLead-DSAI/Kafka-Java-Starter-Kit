package org.raam.kafka.programs;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerCallback.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("Kafka Producer");

        // Create Producer Properties
        Properties properties = new Properties();

        // Connect to Local Host
        properties.setProperty("bootstrap.servers", "localhost:9092");

        //Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Sticky Partition
        for (int j=0; j<10; j++) {
            for (int i=0; i<30; i++) {
                // Create a producer record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("Dev_Topic_01", "Kafka Implementation Message");

                // Send Data (Asynchronous)
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // Executes every time a record successfully sent or exception is thrown
                        if (e == null) {
                            // The record was successfully sent
                            log.info("Received new metadata \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }
        }

        try {
            Thread.sleep(500);
        } catch (InterruptedException error) {
            error.printStackTrace();
        }

        // Tell producer to send all data and block until completion (Synchronous)
        producer.flush();

        // Close Producer
        producer.close();
    }
}
