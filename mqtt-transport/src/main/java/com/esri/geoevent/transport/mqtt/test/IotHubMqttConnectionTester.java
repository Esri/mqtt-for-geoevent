package com.esri.geoevent.transport.mqtt.test;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

public class IotHubMqttConnectionTester {
  public static void main(String[] args) {
    // TODO: ??? "-Djavax.net.debug=all"
    // "https://community.esri.com/t5/arcgis-geoevent-server-blog/working-with-azure-event-hubs-using-kafka/ba-p/1235366"
    // "https://learn.microsoft.com/en-us/azure/iot-hub/iot-hub-dev-guide-sas?tabs=java"
    // String clientId = MqttClient.generateClientId();
    String deviceId = "device-0";
    String iotHubName = "Streetlights-IoT-Hub";
    String iotHubHostName = iotHubName + ".azure-devices.net";
    String port = "8883";
    String iotHubUri = "ssl://" + iotHubHostName + ":" + port;
    try {
      MqttClient mqttClient = new MqttClient(iotHubUri, deviceId); // uses default file persistence OR do MemoryPersistence persistence = new MemoryPersistence();
      // mqttClient.setTimeToWait(5000);

      IotHubMqttMessageConsumer consumer = new IotHubMqttMessageConsumer(mqttClient);
      IotHubMqttMessageProducer producer = new IotHubMqttMessageProducer(mqttClient);
      mqttClient.setCallback(producer);

      System.out.println("Connecting to broker: " + iotHubUri);
      String username = iotHubHostName + "/" + deviceId + "/api-version=2021-04-12";
      String resourceUri = iotHubHostName + "/devices/" + deviceId;
      String password = IotHubMqttUtil.generateSasToken(resourceUri, "I86WAEuSHmxb5U0Pr218rw==");
      MqttConnectOptions connOpts = IotHubMqttUtil.getMqttConnectOptions(username, password);
      mqttClient.connect(connOpts);

      if (mqttClient.isConnected()) {
        // once connection is established we need to subscribe the consumer
        consumer.subscribe();

        Thread consumerThread = new Thread(consumer);
//        Thread producerThread = new Thread(producer);

        consumerThread.start();
//        producerThread.start();

        consumerThread.join();
//        producerThread.join();

//        mqttClient.disconnect();
//        System.out.println("Client HAS DISCONNECTED !!!");
        // mqttClient.close();
      } else {
        System.out.println("Client HAS NOT CONNECTED !!!");
      }
    } catch (Exception error) {
      System.out.println("cause " + error.getCause());
      error.printStackTrace();
    }
  }
}
