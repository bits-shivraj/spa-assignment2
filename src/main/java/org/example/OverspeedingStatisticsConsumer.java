package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OverspeedingStatisticsConsumer {

    public static void main(String[] args) {
        // Set up consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "overspeeding-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.example.StatsDeserializer");
        // Create Kafka consumer
        KafkaConsumer<String, Stats> consumer = new KafkaConsumer<>(props);

        // Subscribe to the overspeeding_statistics topic statistics_sensordata
        consumer.subscribe(Collections.singletonList("statistics_sensordata"));
        //TopicPartition partition = new TopicPartition("statistics_sensordata", 0);
       // consumer.seek(partition, 20);
        // Poll for new messages
        while (true) {
            try {
                ConsumerRecords<String, Stats> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Stats> record : records) {
                    System.out.printf("Received message: key = %sn", record.value());
                    // Process the message (e.g., store in a database, perform analytics, etc.)
                }
            }catch (Exception e)
            {
                System.out.println("Exception" + e.getMessage());
            }
        }
    }
}

