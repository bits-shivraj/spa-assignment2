package org.example;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;

public class EnhancedSensorDataSerializer implements Serializer<EnhancedSensorData> {
    @Override
    public byte[] serialize(String topic, EnhancedSensorData data) {
        // Implement your serialization logic here
        // For example, convert EnhancedSensorData object to JSON string and then to byte array
        return data.toString().getBytes(StandardCharsets.UTF_8);
    }
}