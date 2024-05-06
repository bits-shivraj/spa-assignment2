package org.example;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;

public class OverspeedingStatistics {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "overspeeding-statistics-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        // Define serde for SensorData
        Serde<org.example.SensorData> sensorDataSerde = Serdes.serdeFrom(new SensorDataSerializer(), new SensorDataDeserializer());

        // Consume data from the input topic
        KStream<String, org.example.SensorData> inputStream = builder.stream("sensordata", Consumed.with(Serdes.String(), sensorDataSerde));

        // Define overspeed threshold
        final int speedThreshold = 80;

        // Filter messages where speed exceeds the threshold
        KStream<String, org.example.SensorData> overspeeds = inputStream
                .filter((key, value) -> value.speed > speedThreshold);

        // Group by truck ID and route
        KGroupedStream<Object, org.example.SensorData> groupedByTruckAndRoute = overspeeds
                .groupBy((key, value) -> value.truckNumber + "|" + value.route
                );

        // Define tumbling window of 1 hour
        TimeWindows windows = TimeWindows.of(java.time.Duration.ofSeconds(20));

        // Count overspeed incidents per truck and route within each window
        KTable<Windowed<Object>, Long> overspeedCounts = groupedByTruckAndRoute
                .windowedBy(windows)
                .count();


        // Print the counts
        overspeedCounts
                .toStream()
                .foreach((key, value) -> {
                    Date startTime = Date.from(key.window().startTime());
                    ZonedDateTime startTimeInIST = startTime.toInstant().atZone(ZoneId.of("Asia/Kolkata"));

                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    String windowStart = startTimeInIST.format(formatter);

                    Date endTime = Date.from(key.window().endTime());
                    ZonedDateTime endTimeInIST = endTime.toInstant().atZone(ZoneId.of("Asia/Kolkata"));
                    String windowEnd = endTimeInIST.format(formatter);

                    System.out.printf("Key: %s, Window Start: %s, Window End: %s, Count: %d%n",
                            key.key(), windowStart, windowEnd, value);
                    Stats st = new Stats(windowStart, windowEnd, value, key.toString());
                    overspeedCounts
                            .toStream()
                            .map((k, v) -> KeyValue.pair(key, st)) // Map key-value pairs to send both key and value
                            .to("statistics_sensordata"); // Assuming

                });
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();



        // Add shutdown hook to gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


}
