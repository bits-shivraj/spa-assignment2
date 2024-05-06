package org.example;

import java.sql.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
public class FetchDriverDetails {
    String url;
    String username;
    String password;

    public FetchDriverDetails() {
        url = "jdbc:postgresql://localhost:5432/driverdb";
        username = "postgres";
        password = "root";
    }

    public EnhancedSensorData fetchdriverrDetails(Integer truckNumber) {
        boolean result = false;

        try (Connection connection = DriverManager.getConnection(this.url, this.username, this.password)) {
            System.out.println("Connected to PostgreSQL database!");
            String sql = "SELECT * FROM drivertable WHERE TruckNumber = ?";

            // Prepare the statement
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setInt(1, truckNumber);

            // Execute the query
            ResultSet resultSet = statement.executeQuery();

            // Process the results
            while (resultSet.next()) {
                String truckNumber1 = resultSet.getString("TruckNumber");
                String name = resultSet.getString("Name");
                String phoneNumber = resultSet.getString("PhoneNumber");
                String truckType = resultSet.getString("TruckType");
                int truckAge = resultSet.getInt("TruckAge");
EnhancedSensorData ed = new EnhancedSensorData(truckNumber, name, phoneNumber,truckType,truckAge);
                // Print the driver details
                System.out.println("Truck Number: " + truckNumber);
                System.out.println("Name: " + name);
                System.out.println("Phone Number: " + phoneNumber);
                System.out.println("Truck Type: " + truckType);
                System.out.println("Truck Age: " + truckAge);
                return  ed;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return  null;
    }






}

