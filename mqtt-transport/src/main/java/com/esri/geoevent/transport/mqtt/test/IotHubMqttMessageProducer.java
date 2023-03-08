package com.esri.geoevent.transport.mqtt.test;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import java.util.Random;

public class IotHubMqttMessageProducer extends IotHubMqttMessageBase {

  private final Random random;

  public IotHubMqttMessageProducer(MqttClient mqttClient) {
    super(mqttClient);
    random = new Random();
    topicName = "devices/" + mqttClient.getClientId() + "/messages/events/";
  }

  public void execute() throws MqttException {
    mqttClient.publish(topicName, sample());
  }

  private MqttMessage sample() {
    int value = random.nextInt(100);
    String content = "{\"AI01\": " + value + "}";
    MqttMessage message = new MqttMessage(content.getBytes());
    message.setQos(getQos());
    // mqttMessage.setRetained(true); TODO: ???
    return message;
  }
}
