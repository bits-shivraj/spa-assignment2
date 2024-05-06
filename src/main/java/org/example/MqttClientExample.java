package org.example;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Timer;
import java.util.TimerTask;

public class MqttClientExample {


    public static void main(String[] args) throws MqttException, InterruptedException {


        // Create an MQTT client
        MqttClient client = new MqttClient("tcp://localhost:1883", "clientId");

        // Connect to the broker
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        client.connect(options);

        Thread.sleep(100);

            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    SensorData data = new SensorData();
                    ByteArrayOutputStream baos = null;
                    try{
                     baos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(baos);
                    oos.writeObject(data);
                    oos.flush();
                } catch (Exception e){

                    }


                    // Create a message to publish
                    MqttMessage message = new MqttMessage(baos.toByteArray());

                    // Publish the message
                    try {

                        client.publish("myTopic", message);
                        System.out.println("Client is writing to mqtt broker" + message);
                    } catch (MqttException e) {
                        throw new RuntimeException(e);
                    }

                }
            }, 0, 3000); // schedule the task to run every 3 seconds



        // Disconnect from the broker

    }

}