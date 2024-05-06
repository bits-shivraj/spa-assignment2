package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

public class windowing {

        public static void main(String[] args) {
            Properties config = new Properties();
            config.put(StreamsConfig.APPLICATION_ID_CONFIG, "sensor-data-processor-windowing");
            config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            StreamsBuilder builder = new StreamsBuilder();

            // Define serde for SensorData
            Serde<SensorData> sensorDataSerde = Serdes.serdeFrom(new SensorDataSerializer(), new SensorDataDeserializer());
            // Consume data from the input topic
            KStream<String, SensorData> inputStream = builder.stream("sensordata", Consumed.with(Serdes.String(), sensorDataSerde));

            Duration inactivityGap = Duration.ofMillis(5);
            Duration gracePeriod = Duration.ofMillis(2  );

            SessionWindows windows = SessionWindows.ofInactivityGapAndGrace(inactivityGap, gracePeriod);

            KTable<Windowed<String>, Long> windowedCounts = inputStream
                    .groupByKey()
                    .windowedBy(windows)
                    .count();

            // Print the counts
            windowedCounts
                    .toStream()
                    .foreach((key, value) -> {
                        String windowStart = key.window().startTime().toString();
                        String windowEnd = key.window().endTime().toString();
                        System.out.printf("Key: %s, Window Start: %s, Window End: %s, Count: %d%n",
                                key.key(), windowStart, windowEnd, value);
                    });
            //inputStream.groupByKey().windowedBy(SessionWindows.ofInactivityGapAndGrace(windowsize,advancesize)).count();
            // Perform processing on the stream (Example: Printing to console)
//            inputStream.foreach((key, value) -> {
//                //  System.out.println("Received SensorData: " + value);
//                System.out.println("*****Speed: " + value.speed);
//                System.out.println("Truck Number: " + value.truckNumber);
//                if (value.speed > 80) {
//                    System.out.println("Overspeed for truck number " + value.truckNumber);
//                    // print overspeeding driver details
//                    FetchDriverDetails fd = new FetchDriverDetails();
//                    fd.fetchdriverrDetails(value.truckNumber);
//                }
//                System.out.println("end");
//            });


            // Publish processed data to another topic
            inputStream.to("stastics_sensordata", Produced.with(Serdes.String(), sensorDataSerde));

            KafkaStreams streams = new KafkaStreams(builder.build(), config);
            streams.start();

            // Add shutdown hook to gracefully close the streams application
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        }
    }



