package com.esri.geoevent.transport.mqtt;

import com.esri.ges.core.property.Property;
import org.apache.commons.lang3.StringUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MqttTransportConfigReader
{
  private final MqttTransport mqttTransport;

  public MqttTransportConfigReader(MqttTransport mqttTransport) {
    this.mqttTransport = mqttTransport;
  }

  public MqttTransportConfig readConfig()
  {
    List<String> errors = new ArrayList<>();

    // default connection settings
    boolean isUseSSL = false;
    int port = 1883;
    String host = "iot.eclipse.org";

    if (mqttTransport.hasProperty("host")) {
      // mandatory property 'host' is present -> let us read and check its value
      String hostValue = getPropertyValueAsString("host", errors);
      if (StringUtils.isEmpty(hostValue))
      {
        // mandatory property value is null -> report this error
        errors.add("MQTT Host URL cannot be NULL or EMPTY.");
      } else {
        Matcher matcher = Pattern.compile("^(?:(tcp|ssl)://)?([-.a-z0-9]+)(?::([0-9]+))?$", Pattern.CASE_INSENSITIVE).matcher(hostValue);
        if (matcher.matches())
        {
          isUseSSL = "ssl".equalsIgnoreCase(matcher.group(1));
          host = matcher.group(2);
          port = matcher.start(3) > -1 ? Integer.parseInt(matcher.group(3)) : isUseSSL ? 8883 : 1883;
        } else {
          errors.add(mqttTransport.translate("Invalid MQTT Host URL '{0}'", hostValue));
        }
      }
    }

    String topic = "";
    if (mqttTransport.hasProperty("topic")) {
      topic = getPropertyValueAsString("topic", errors);
      if (StringUtils.isEmpty(topic))
      {
        // mandatory property value is null or empty -> report this error
        errors.add("GeoEvent TOPIC cannot be NULL or EMPTY.");
      } else if (topic.length() == 1 && !topic.equals("/")) {
        errors.add(mqttTransport.translate("GeoEvent TOPIC = {0}. ERROR, the topic must be more than one character long or equal to '/'.", topic));
      }
    }

    String username = mqttTransport.hasProperty("username") ? getPropertyValueAsString("username", errors) : "";
    username = (username == null) ? "" : username;

    // Get the password as a DecryptedValue
    String password = "";
    if (mqttTransport.hasProperty("password") && StringUtils.isNotEmpty(getPropertyValueAsString("password", errors)))
    {
      try
      {
        password = mqttTransport.getProperty("password").getDecryptedValue();
      }
      catch (Exception error)
      {
        errors.add("Password property value is invalid.");
      }
    }

    // qos is an integer from the set [0, 1, or 2]
    int qos = 0;
    if (mqttTransport.hasProperty("qos"))
    {
      String qosValue = getPropertyValueAsString("qos", errors);
      if (StringUtils.isEmpty(qosValue))
      {
        // mandatory property value is null or empty -> report this error
        errors.add("Mandatory property value for QOS cannot be NULL or EMPTY.");
      } else {
        try {
          qos = Integer.parseInt(qosValue);
          if (qos < 0 || qos > 2)
            errors.add(mqttTransport.translate("Property value for QOS is not valid ({0}). Using default of 0 instead.", qosValue));
        } catch (NumberFormatException error) {
          errors.add(mqttTransport.translate("Property value for QOS is not valid ({0}). Using default of 0 instead.", qosValue));
        }
      }
    }

    // retain is a boolean value
    boolean isRetain = false;
    if (mqttTransport.hasProperty("retain"))
    {
      String retainValue = getPropertyValueAsString("retain", errors);
      if (StringUtils.isEmpty(retainValue))
      {
        // mandatory property value is null or empty -> report this error
        errors.add("Property value instructing broker to retain last message sent on each distinct topic cannot be NULL or EMPTY.");
      } else {
        isRetain = Boolean.parseBoolean(retainValue);
      }
    }

    return new MqttTransportConfig(host, port, (topic == null) ? "": topic, isUseSSL, username, password, qos, isRetain, errors);
  }

  /**
   * Checks if a property is valid, then makes sure it isn't null or empty.
   *
   * @param propertyName
   *          The name of the property to get
   * @return Null if the property is not valid, null, or empty string. The property string value otherwise.
   */
  private String getPropertyValueAsString(String propertyName, List<String> errors)
  {
    Property property = mqttTransport.getProperty(propertyName);
    if (property.isValid())
    {
      if (property.getValue() == null)
      {
        // this is a valid property with value null
        mqttTransport.trace("Property {0} is NULL.", propertyName);
      } else {
        String propertyValue = property.getValueAsString();
        if (propertyValue.isEmpty())
        {
          // this is a valid property with value of empty String
          mqttTransport.trace("Property {0} is EMPTY.", propertyName);
        }
        // return property value or empty String for valid property
        return propertyValue;
      }
    } else {
      mqttTransport.trace("Property {0} is NOT VALID.", propertyName);
      errors.add(mqttTransport.translate("Property {0} is NOT VALID.", propertyName));
    }
    // return null when either property value is invalid, or null
    return null;
  }
}
