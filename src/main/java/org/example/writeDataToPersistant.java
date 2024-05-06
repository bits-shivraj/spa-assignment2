package org.example;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class writeDataToPersistant {

String url;
String username;
String password;
    public writeDataToPersistant(){
         url = "jdbc:postgresql://localhost:5432/ivms";
         username = "postgres";
         password = "root";
    }
    public boolean inserttoDB(SensorData sensorData){
        boolean result = false;
        try (Connection connection = DriverManager.getConnection(this.url, this.username, this.password)) {
            System.out.println("Connected to PostgreSQL database!");

            String insertQuery = "INSERT INTO truck_data (timestamp, truck_number, latitude, longitude, route,speed) VALUES (?,?,?,?,?,?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                preparedStatement.setLong(1, ((sensorData.timestamp)));
                preparedStatement.setString(2, String.valueOf(sensorData.truckNumber));
                preparedStatement.setDouble(3, ((sensorData.latitude)));
                preparedStatement.setDouble(4, sensorData.longitude);
                preparedStatement.setString(5, ((sensorData.route)));
                preparedStatement.setInt(6, (sensorData.speed));
                preparedStatement.executeUpdate();
                System.out.println("Data inserted successfully!");
            } catch (SQLException e) {
                System.out.println("Error inserting data.");
                e.printStackTrace();
                return  false;
            }
        } catch (SQLException e) {
            System.out.println("Connection failure.");
            e.printStackTrace();
            return  false;
        }
        return  true;
    }





}
