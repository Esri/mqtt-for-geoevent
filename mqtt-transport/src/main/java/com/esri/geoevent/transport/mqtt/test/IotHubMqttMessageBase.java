package com.esri.geoevent.transport.mqtt.test;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import java.util.Arrays;

abstract public class IotHubMqttMessageBase implements MqttCallback, Runnable {
  protected final MqttClient mqttClient;
  protected boolean canRun;
  private volatile boolean isRunning;
  protected String topicName;

  public IotHubMqttMessageBase(MqttClient mqttClient) {
    this.mqttClient = mqttClient;
    this.canRun = true;
  }

  @Override
  public void run() {
    if (mqttClient.isConnected()) {
      isRunning = canRun;
      while (isRunning) {
        try
        {
          Thread.sleep(1000);
          execute();
        } catch (InterruptedException error) {
          try {
            mqttClient.disconnect();
          } catch (MqttException e) {
            // ignore
          }
        } catch (MqttException mqttError) {
          // ignore
        }
      }
    } else {
      System.out.println("Client HAS NOT CONNECTED !!!");
    }
  }

  public int getQos() {
    return 1;
  }

  public abstract void execute() throws MqttException;

  @Override
  public void connectionLost(Throwable cause) {
    System.out.println("Connection LOST !!!");
    cause.printStackTrace();
  }

  @Override
  public void messageArrived(String topic, MqttMessage message) throws Exception {
    System.out.println(
        "Message RECEIVED: '" + Arrays.toString(message.getPayload()) + "' on topic '" + topic + "' with QoS " + message.getQos()
    );
  }

  @Override
  public void deliveryComplete(IMqttDeliveryToken token) {
    try {
      StringBuilder sb = new StringBuilder();
      for (String topic : token.getTopics())
      {
        if (sb.length() > 0)
          sb.append(", ");
        sb.append(topic);
      }
      String topics = "[" + sb + "]";
      if (token.getMessage() == null)
        System.out.println("Published null on " + topics);
      else
        System.out.println("Published '" + token.getMessage().toString() + "' on " + topics);
    } catch (MqttException e) {
      System.out.println("Message HAS NOT published !!!");
    }
  }

  public void stop() {
    isRunning = false;
  }
}
