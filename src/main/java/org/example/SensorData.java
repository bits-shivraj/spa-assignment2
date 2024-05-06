package org.example;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.Random;

public class SensorData implements Serializable {
    static int[] truckNumbers = { 1234, 5678, 9012, 3456, 7890,
            2345, 6789, 8901, 4567, 8902 };
    private static final long serialVersionUID = 1L; // Recommended to ensure version compatibility

    long timestamp;
    int truckNumber;
    double latitude;
    double longitude;
    String route;
    int speed;

    // Random generator as a static field, shared by all instances
    private static final Random random = new Random();

    // Constructor to initialize with random data
    public SensorData() {
        // Generate a timestamp in Unix epoch time (seconds since 1970-01-01 00:00:00 UTC)
        this.timestamp = System.currentTimeMillis() / 1000L;

        // Generate a random truck number (assuming truck numbers are between 1000 and 9999)
        this.truckNumber = (truckNumbers[ random.nextInt(10)]);

        // Generate random latitude and longitude
        this.latitude = -90 + 180 * random.nextDouble();
        this.longitude = -180 + 360 * random.nextDouble();

        // Random route (assuming routes are named Route-1, Route-2, ..., Route-5)
        this.route = "Route-" + (1 + random.nextInt(5));

        // Generate random speed (assuming speed range is 0 to 120 km/h)
        this.speed = random.nextInt(121);
    }

    // Method to return a formatted string of sensor data
    @Override
    public String toString() {
        return "Timestamp: " + timestamp +
                "\nTruckNumber: " + truckNumber +
                "\nLatitude: " + latitude +
                "\nLongitude: " + longitude +
                "\nRoute: " + route +
                "\nSpeed: " + speed +
                "\n--------------------------------------";
    }

    // Main method to test serialization

            // Serialize the object


}
