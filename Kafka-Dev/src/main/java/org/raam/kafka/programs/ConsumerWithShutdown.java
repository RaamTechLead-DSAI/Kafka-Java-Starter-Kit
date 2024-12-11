package org.raam.kafka.programs;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerWithShutdown.class.getSimpleName());

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
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        // Create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // Add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, consumer will shutdown by invoking wakeup().");
                consumer.wakeup();
                // Join the main thread to allow execution of main thread code
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            // Subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));
            // Poll for data
            while (true) {
                log.info("Consumer: Polling .....");

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException wakeupException) {
            log.info("Consumer is starting to shutdown");
        } catch (Exception exception) {
            log.error("Unexpected exception in the consumer", exception);
        } finally {
            consumer.close(); // Close the consumer, this will also commit offsets
            log.info("The consumer had shutdown gracefully");
        }
    }
}
