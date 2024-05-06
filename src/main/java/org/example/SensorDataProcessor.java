package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class SensorDataProcessor {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "sensor-data-processor");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        // Define serde for SensorData
        Serde<SensorData> sensorDataSerde = Serdes.serdeFrom(new SensorDataSerializer(), new SensorDataDeserializer());
        // Consume data from the input topic
        Serde<EnhancedSensorData> enhancedSensorDataSerde = Serdes.serdeFrom(new EnhancedSensorDataSerializer(), new EnhancedSensorDataDeserializer());

        KStream<String, SensorData> inputStream = builder.stream("sensordata", Consumed.with(Serdes.String(), sensorDataSerde));

//        KStream<String, EnhancedSensorData> enhancedStream = inputStream.mapValues(value -> {
//            if (value.latitude < -90 || value.latitude > 90 || value.longitude < -180 || value.longitude > 180) {
//                System.out.println("Invalid latitude or longitude value: " );
//                return null; // Skip invalid data
//            }
//            if (value.speed > 80) {
//                System.out.println("Overspeed for truck number " + value.truckNumber);
//                FetchDriverDetails fd = new FetchDriverDetails();
//                EnhancedSensorData edd = fd.fetchdriverrDetails(value.truckNumber);
//                return  edd;
//            }else{
//                return null;
//            }
//          //  return null; // Return null or some default EnhancedSensorData instance
//        });
//
//        // Filter out null values and publish the enriched data to another topic
//        enhancedStream.filter((key, value) -> value!= null)
//                .to("processed_sensordata", Produced.with(Serdes.String(), enhancedSensorDataSerde));
        KStream<String, EnhancedSensorData> enhancedStream = inputStream
                .filter((key, value) -> value.speed > 80 && isValidCoordinates(value.latitude, value.longitude))
                .mapValues(value -> {
                    FetchDriverDetails fd = new FetchDriverDetails();
                    return fd.fetchdriverrDetails(value.truckNumber);
                });

        // Ensure enhancedStream is not null before publishing
        enhancedStream.filter((key, value) -> value != null)
                .to("processed_sensordata", Produced.with(Serdes.String(), enhancedSensorDataSerde));
//        KafkaStreams streams = new KafkaStreams(builder.build(), config);
//        // Perform processing on the stream (Example: Printing to console)
//        inputStream.foreach((key, value) -> {
//            //  System.out.println("Received SensorData: " + value);
//            System.out.println("*****Speed: " + value.speed);
//            System.out.println("Truck Number: " + value.truckNumber);
//            // If else for lat and long
//            if (value.latitude < -90 || value.latitude > 90) {
//                System.out.println("Invalid latitude value: " + value.latitude);
//                return;
//            } else if (value.longitude < -180 || value.longitude > 180) {
//                System.out.println("Invalid longitude value: " + value.longitude);
//                return;
//            }
//            if (value.speed > 80) {
//                System.out.println("Overspeed for truck number " + value.truckNumber);
//                // print overspeeding driver details
//                FetchDriverDetails fd = new FetchDriverDetails();
//               EnhancedSensorData ed = fd.fetchdriverrDetails(value.truckNumber);
//
//            }
//            System.out.println("end");
//        });

// push driver details for enrich and
        // Publish processed data to another topic
       // inputStream1.to("processed_sensordata", Produced.with(Serdes.String(), enhancedSensorDataSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        // Add shutdown hook to gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static boolean isValidCoordinates(double latitude, double longitude) {
        return !(latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180);
    }
}

