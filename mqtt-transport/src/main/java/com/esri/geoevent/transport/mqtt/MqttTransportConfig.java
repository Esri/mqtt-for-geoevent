package com.esri.geoevent.transport.mqtt;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import java.util.List;

public class MqttTransportConfig
{
  private final String host;
  private final int port;
  private final String topic;
  private final boolean isTopicNameSubstituteRequired;
  private final boolean isUseSSL;
  private final String username;
  private final String password;
  private final boolean isUseCredentials;
  private final int qos;
  private final boolean isRetain;
  private final String url;
  private final MqttConnectOptions connectOptions;
  private final List<String> errors;

  public MqttTransportConfig(
      String host,
      int port,
      String topic,
      boolean isUseSSL,
      String username,
      String password,
      int qos,
      boolean isRetain,
      List<String> errors)
  {
    this.host = host;
    this.port = port;
    this.isUseSSL = isUseSSL;
    this.topic = topic;
    this.isTopicNameSubstituteRequired = topic.contains("$");
    this.username = username;
    this.password = password;
    this.qos = qos;
    this.isRetain = isRetain;

    // initialize Mqtt connect options
    this.connectOptions = new MqttConnectOptions();

    // Connect with username and password if both are available
    this.isUseCredentials = StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password);
    if (isUseCredentials)
    {
      connectOptions.setUserName(username);
      connectOptions.setPassword(password.toCharArray());
    }

    if (isUseSSL)
    {
      // initialize url for ssl-enabled connection
      this.url = "ssl://" + host + ":" + port;

      // Support TLS only (1.0-1.2) as even SSL 3.0 has well known exploits
      java.util.Properties sslProperties = new java.util.Properties();
      sslProperties.setProperty("com.ibm.ssl.protocol", "TLS");
      connectOptions.setSSLProperties(sslProperties);
    } else {
      // initialize url for non-ssl connection
      this.url = "tcp://" + host + ":" + port;
    }
    connectOptions.setCleanSession(true);

    this.errors = errors;
  }

  public String getUrl() {
    return url;
  }

  public MqttConnectOptions getConnectOptions() {
    return connectOptions;
  }

  public boolean isUseSSL() {
    return isUseSSL;
  }

  public boolean isRetain() {
    return isRetain;
  }

  public boolean isTopicNameSubstituteRequired() {
    return isTopicNameSubstituteRequired;
  }

  public boolean isUseCredentials() {
    return isUseCredentials;
  }

  public String getTopic() {
    return topic;
  }

  public int getQos() {
    return qos;
  }

  public String getUserName() {
    return username;
  }

  public boolean hasErrors() {
    return errors.size() > 0;
  }

  public List<String> getErrors() {
    return errors;
  }

  @Override
  public String toString()
  {
    return "port=" + port + ", host=" + host + ", ssl=" + isUseSSL + ", topic=" + topic + ", qos=" + qos + ", username=" + username + ", password=" + password + ", retain=" + isRetain;
  }
}
