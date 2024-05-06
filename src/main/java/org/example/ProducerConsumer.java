package  org.example;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Collections;
import java.util.Properties;
import java.util.Random;

public class ProducerConsumer {

    public static void main(String[] args) {
        // Set Kafka brokers
        String brokers = "localhost:9092";
        String topic = "stastics_sensordata";

        // Set up Kafka producer properties
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Set up Kafka consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

//        // Start producer
//        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
//        Random random = new Random();
//        String[] sentences = new String[] {
//                "beauty is in the eyes of the beer holders",
//                "Apple is not a fruit",
//                "Cucumber is not vegetable",
//                "Potato is stem vegetable, not root",
//                "Python is a language, not a reptile"
//        };
//
//        for (int i = 0; i < 1000; i++) {
//            String sentence = sentences[random.nextInt(sentences.length)];
//            ProducerRecord<String, String> record = new ProducerRecord<>(topic, sentence);
//            producer.send(record, new TestCallback());
//        }
//
//        producer.flush();
//        producer.close();

        // Start consumer
        KafkaConsumer<String, SensorData> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, SensorData> records = consumer.poll(1000);
            for (ConsumerRecord<String, SensorData> record : records) {
                System.out.println("Received message: " + record.value());
            }
        }
    }

    private static class TestCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                System.err.println("Error while sending message: " + exception.getMessage());
            } else {
                System.out.println("Message sent successfully: " + metadata.toString());
            }
        }
    }
}
