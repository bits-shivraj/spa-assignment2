package org.example;

public class EnhancedSensorData extends  SensorData{
    Long  truckNumber;
    String name;
    String  phoneNumber;
    String truckType;
    Integer truckAge;

    public EnhancedSensorData() {
        super();  // Calls the parent constructor to initialize inherited fields
      //  this.temperature = -20 + 40 * random.nextDouble();  // Generate a random temperature between -20 and +20 degrees Celsius
    }
    public EnhancedSensorData( long truckNumber, String name, String phoneNumber, String truckType, Integer truckAge) {
        super();
        this.name = name;
        this.phoneNumber = phoneNumber;
        this.truckType = truckType;
        this.truckAge = truckAge;
        this.truckNumber = truckNumber;
    }


}
