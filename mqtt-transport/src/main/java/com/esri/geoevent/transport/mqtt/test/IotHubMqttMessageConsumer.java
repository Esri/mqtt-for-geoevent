package com.esri.geoevent.transport.mqtt.test;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;

public class IotHubMqttMessageConsumer extends IotHubMqttMessageBase {

  public IotHubMqttMessageConsumer(MqttClient mqttClient) {
    super(mqttClient);
    // C2D messages are sent through a service-facing endpoint
    topicName = "devices/" + mqttClient.getClientId() + "/messages/devicebound"; // devicebound/#
  }

  public void execute() throws MqttException {
     System.out.println("IotHubMqttMessageConsumer.execute()");
  }

  public void subscribe() {
    try {
      mqttClient.subscribe(topicName, getQos());
      canRun = true;
    } catch (Exception error) {
      System.out.println("IotHubMqttMessageConsumer.subscribe(): subscription failed -> we cannot run the consumer");
      canRun = false;
    }
  }
}
