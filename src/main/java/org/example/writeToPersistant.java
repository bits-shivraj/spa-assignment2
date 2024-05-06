package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class writeToPersistant {

    public static void main(String[] args) {
        // Set Kafka brokers
        String brokers = "localhost:9092";
        String topic = "sensordata";

        // Set up Kafka consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorDataDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Start consumer
        KafkaConsumer<String, SensorData> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(topic));

      writeDataToPersistant wdt = new writeDataToPersistant();

        while (true) {
            ConsumerRecords<String, SensorData> records = consumer.poll(1000);
            for (ConsumerRecord<String, SensorData> record : records) {
                System.out.println("Received message: " + record.value());
                wdt.inserttoDB(record.value());

            }
        }
    }
}
