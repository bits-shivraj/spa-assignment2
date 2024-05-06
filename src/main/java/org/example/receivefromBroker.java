
package org.example;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class receivefromBroker {

    public static void main(String[] args) {
        String broker = "tcp://localhost:1883";
        String clientId = "demo_client";
        String topic = "myTopic";
        int subQos = 1;
        int pubQos = 1;
        String msg = "Hello MQTT";
        // Set Kafka brokers
        String brokers = "localhost:9092";
        String kafka_topic = "sensordata";

        // Set up Kafka producer properties
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.example.SensorDataSerializer");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try {
            MqttClient client = new MqttClient(broker, clientId);
            MqttConnectionOptions options = new MqttConnectionOptions();
            // Start kafka producer
            KafkaProducer<String, SensorData> producer = new KafkaProducer<>(producerProps);

            client.setCallback(new MqttCallback() {
                public void connectComplete(boolean reconnect, String serverURI) {
                    System.out.println("connected to: " + serverURI);
                }

                public void disconnected(MqttDisconnectResponse disconnectResponse) {
                    System.out.println("disconnected: " + disconnectResponse.getReasonString());
                }

                public void deliveryComplete(IMqttToken token) {
                    System.out.println("deliveryComplete: " + token.isComplete());
                }

                public void messageArrived(String topic, MqttMessage message) throws Exception {
                  try {
                      System.out.println("topic: " + topic);
                      System.out.println("qos: " + message.getQos());
                      ByteArrayInputStream bais = new ByteArrayInputStream(message.getPayload());
                      ObjectInputStream ois = new ObjectInputStream(bais);
                      SensorData deserializedData = (SensorData) ois.readObject();
                      ProducerRecord<String, SensorData> record = new ProducerRecord<>(kafka_topic, deserializedData);
                      // ProducerRecord<String, String> record1 = new ProducerRecord<String, String>(kafka_topic, "Hello");
                      producer.send(record);
                      System.out.println("message content: " + new String(String.valueOf(deserializedData.timestamp)));
                  } catch (Exception ex){
                      System.out.println(ex.getMessage());
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


                public void mqttErrorOccurred(MqttException exception) {
                    System.out.println("mqttErrorOccurred: " + exception.getMessage());
                }

                public void authPacketArrived(int reasonCode, MqttProperties properties) {
                    System.out.println("authPacketArrived");
                }
            });

            client.connect(options);

            client.subscribe(topic, subQos);

            while (true) {
                Thread.sleep(1000);
            }
//            MqttMessage message = new MqttMessage(msg.getBytes());
//            message.setQos(pubQos);
//            client.publish(topic, message);

           // client.disconnect();
           // client.close();

        } catch (MqttException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
