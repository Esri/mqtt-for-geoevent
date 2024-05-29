/*
  Copyright 1995-2019 Esri

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

  For additional information, contact:
  Environmental Systems Research Institute, Inc.
  Attn: Contracts Dept
  380 New York Street
  Redlands, California, USA 92373

  email: contracts@esri.com
*/
package com.esri.geoevent.transport.mqtt;

import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.transport.TransportException;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * Manages the MQTT properties, creating clients, connecting clients, and disconnecting clients.
 */
public class MqttClientManager
{
  private final BundleLogger LOGGER;

  private final MqttTransportConfig config;

  private MqttClient mqttClient;

  public MqttClientManager(MqttTransportConfig config, BundleLogger logger)
  {
    this.config = config;
    this.LOGGER = logger;

    if (LOGGER.isTraceEnabled())
      LOGGER.trace(this.toString());
  }

  public void connect(MqttCallback callback) throws MqttException
  {
    if (config.isUseCredentials())
      LOGGER.trace("Connecting to MQTT Broker using credentials. Username={0}", config.getUserName());

    if (config.isUseSSL())
      LOGGER.trace("Connecting to MQTT Broker using SSL. NOTE: Only TLS 1.0 to 1.2 are supported.");

    mqttClient = createMqttClient(callback);
    mqttClient.connect(config.getConnectOptions());
    LOGGER.info("mqttClient connected clientId=" + mqttClient.getClientId());
  }

  public boolean isConnected()
  {
    return mqttClient != null && mqttClient.isConnected();
  }

  /**
   * Checks to be sure the client is connected, then disconnects and closes the client.
   *
   */
  public void disconnect()
  {
    try
    {
      if (isConnected())
      {
        LOGGER.trace("Disconnecting MQTT client...");
        mqttClient.disconnect();
        mqttClient.close();
      }
    } catch (MqttException e) {
      LOGGER.debug("UNABLE_TO_CLOSE", e);
    } finally {
      mqttClient = null;
    }
  }

  public void ensureIsConnected(MqttCallback callback) throws MqttException
  {
    if (!isConnected())
    {
      disconnect();
      connect(callback);
    }
  }

  public void publish(byte[] bytes, GeoEvent geoEvent) throws Exception
  {
    String topicToPublish = config.getTopic();
    if (config.isTopicNameSubstituteRequired() && geoEvent != null)
    {
      LOGGER.trace("received geoEvent, creating output topic from field values using template {0}", topicToPublish);
      // Do field value substitution like "${field1}/${field2}"
      topicToPublish = geoEvent.formatString(topicToPublish);
    }
    if (isTopicValid(topicToPublish))
    {
      LOGGER.debug("Publishing outgoing bytes to topic {0}: {1}", topicToPublish, geoEvent);
      ensureIsConnected(null); // no callback function
      mqttClient.publish(topicToPublish, bytes, config.getQos(), config.isRetain());
    }
    else
    {
      throw new TransportException(LOGGER.translate("GeoEvent Topic {0} is not valid, GeoEvent not published to MQTT output: {1}", topicToPublish, geoEvent));
    }
  }

  public void subscribe(MqttCallback callback) throws Exception {
    ensureIsConnected(callback);
    mqttClient.subscribe(config.getTopic(), config.getQos());
    LOGGER.trace("Connecting to mqtt using {}", this);
  }

  /**
   * @param callback
   *          An MQTT Callback to handle received messages from subscriptions. Can be null for outbound connections.
   * @return a new MQTT Client that is NOT connected (you must call connect on it once you get it).
   * @throws MqttException
   */
  private MqttClient createMqttClient(MqttCallback callback) throws MqttException
  {
    LOGGER.trace("Creating new MQTT client...");
    LOGGER.debug("Creating MQTT Broker client at URL {0}", config.getUrl());
    /* MqttClient mqttClient = new MqttClient(config.getUrl(), MqttClient.generateClientId(), new MemoryPersistence()); */
    MqttClient mqttClient = new MqttClient(config.getUrl(), config.getClientId(), new MemoryPersistence());
    if (callback != null)
    {
      LOGGER.trace("Setting MQTT callback to receive messages");
      mqttClient.setCallback(callback);
    }
    // Let the caller connect, so they can handle connection failures and reconnects.
    return mqttClient;
  }

  /**
   * Each topic must contain at least 1 character and the topic string permits empty spaces. The forward slash alone is
   * a valid topic. The $-symbol topics are reserved for internal statistics of the MQTT broker ($ is not permitted).
   * <p>
   * Topics are case-sensitive. For example, "myhome/temperature" and "MyHome/Temperature" are two different topics.
   * 
   * @return True if the topic is valid. False if it cannot be used as a MQTT topic.
   */
  private boolean isTopicValid(String topic)
  {
    boolean result = false;
    // Can't be empty string
    if (topic.contains("$"))
    {
      // Can't contain the '$' char
      LOGGER.debug("GeoEvent TOPIC = {0}. ERROR, cannot contain the '$' symbol.", topic);
    } else {
      if (topic.length() > 1 || topic.equals("/"))
      {
        // Is longer than 1 character or equals '/'
        result = true;
      } else {
        LOGGER.debug("GeoEvent TOPIC = {0}. ERROR, the topic must be more than one character long or equal to '/'.", topic);
      }
    }
    return result;
  }

  @Override
  public String toString()
  {
    return "MqttClientManager [log=" + LOGGER + ", " + config.toString() + "]";
  }
}
