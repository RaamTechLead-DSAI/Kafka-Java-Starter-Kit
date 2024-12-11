package org.raam.kafka.programs;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerWithKeys.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
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

        for (int j=0; j<2; j++) {
            for (int i = 0; i < 10; i++) {

                String topic = "Dev_Topic_02";
                String key = "id_" + i;
                String value = "Kafka Implementation with Keys " + i;

                // Create a producer record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                // Send Data (Asynchronous)
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // Executes every time a record successfully sent or exception is thrown
                        if (e == null) {
                            // The record was successfully sent
                            log.info("Key: " + key + " | Partition: " + recordMetadata.partition());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }
        }

        Thread.sleep(500);

        // Tell producer to send all data and block until completion (Synchronous)
        producer.flush();

        // Close Producer
        producer.close();
    }
}
