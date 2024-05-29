package com.esri.geoevent.transport.mqtt;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.property.Property;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.TransportBase;
import com.esri.ges.transport.TransportDefinition;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class MqttTransportBase extends TransportBase implements MqttCallback, Runnable
{
  protected static final BundleLogger LOGGER = BundleLoggerFactory.getLogger(MqttTransportBase.class.getName(), "com.esri.geoevent.transport.mqtt-transport");
  private MqttTransportConfig config;
  private MqttClientManager mqttClientManager;
  private ScheduledExecutorService executor;

  public MqttTransportBase(TransportDefinition definition) throws ComponentException
  {
    super(definition);
  }

  public String getName()
  {
    return "MQTT " + getType().toString() + " transport";
  }

  @Override
  public void afterPropertiesSet()
  {
    super.afterPropertiesSet();
    // disconnect the transport before applying configuration changes
    disconnect();
    // time to read transport configuration
    readConfig();
  }

  @Override
  public void validate() throws ValidationException
  {
    StringBuilder sb = new StringBuilder();
    try {
      // validate property collection
      super.validate();
    } catch (ValidationException error) {
      sb.append(error.getMessage()).append("\n");
    }
    // validate config
    if (config != null) {
      if (config.hasErrors())
      {
        for (String error : config.getErrors())
          sb.append(error).append("\n");
      } else {
        // test connection using MqttTransportConfig
        MqttClientManager manager = new MqttClientManager(config, LOGGER);
        try {
          manager.connect(null); // no callback function
        } catch (Exception error) {
          String errorMsg = LOGGER.translate("CONNECTION_TEST_FAILED", config.getUrl(), error.getMessage());
          sb.append(errorMsg).append("\n");
        }
        manager.disconnect();
      }
    }
    // report validation errors if they exist
    if (sb.length() > 0)
      throw new ValidationException(sb.toString());
  }

  @Override
  public void start() {
    switch (getRunningState()) {
      case STARTED:
      case STARTING:
        break;
      case STOPPED:
      case STOPPING:
      case ERROR:
        LOGGER.trace("Starting " + getName() + "...");
        setRunningState(RunningState.STARTING);
        try
        {
          startTransport();
        } catch (Exception error) {
          String errorMsg = LOGGER.translate("{0} {1}", LOGGER.translate("INIT_ERROR", getType().toString()), error.getMessage());
          setErrorState(errorMsg, error);
        }
        break;
      default:
        LOGGER.trace("Cannot start " + getName() + ": transport is unavailable.");
    }
  }

  @Override
  public void run()
  {
    switch (getRunningState()) {
      case STARTING:
      case ERROR:
        try
        {
          connect();
          setErrorMessage("");
          setRunningState(RunningState.STARTED);
          LOGGER.trace(getName() + " has started successfully. Transport state is set to STARTED.");
        } catch (Throwable error) {
          String errorMsg = LOGGER.translate("UNEXPECTED_ERROR", error.getMessage());
          setErrorState(errorMsg, error);
        }
        break;
      default:
    }
  }

  @Override
  public synchronized void stop()
  {
    switch (getRunningState()) {
      case STOPPED:
      case STOPPING:
        break;
      case STARTED:
      case STARTING:
      case ERROR:
        setRunningState(RunningState.STOPPING);
        LOGGER.trace("Stopping " + getName() + "...");
        stopTransport();
        setErrorMessage("");
        setRunningState(RunningState.STOPPED);
        LOGGER.trace(getName() + " has stopped. Transport state is set to STOPPED.");
        break;
      default:
        LOGGER.trace("Cannot stop " + getName() + ": transport is unavailable.");
    }
  }

  protected void publish(byte[] bytes, GeoEvent geoEvent) throws Exception {
    mqttClientManager.publish(bytes, geoEvent);
  }

  @Override
  public void connectionLost(Throwable cause)
  {
    LOGGER.debug("CONNECTION_LOST", cause, cause.getLocalizedMessage());
  }

  @Override
  public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken)
  {
    // not used
  }

  @Override
  public void messageArrived(String topic, MqttMessage message)
  {
    System.out.println("messageArrived(..)");
    // not used
  }

  protected void setErrorState(String errorMsg, Throwable error) {
    setRunningState(RunningState.ERROR);
    setErrorMessage(errorMsg);
    disconnect();
    // log an error
    LOGGER.error(errorMsg);
    LOGGER.debug(errorMsg, error);
  }

  private void readConfig()
  {
    List<String> errors = new ArrayList<>();

    // default connection settings
    boolean isUseSSL = false;
    int port = 1883;
    String host = "iot.eclipse.org";

    if (hasProperty("host")) {
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
          errors.add(LOGGER.translate("Invalid MQTT Host URL '{0}'", hostValue));
        }
      }
    }

    String topic = "";
    if (hasProperty("topic")) {
      topic = getPropertyValueAsString("topic", errors);
      if (StringUtils.isEmpty(topic))
      {
        // mandatory property value is null or empty -> report this error
        errors.add("GeoEvent TOPIC cannot be NULL or EMPTY.");
      } else if (topic.length() == 1 && !topic.equals("/")) {
        errors.add(LOGGER.translate("GeoEvent TOPIC = {0}. ERROR, the topic must be more than one character long or equal to '/'.", topic));
      }
    }

    String username = hasProperty("username") ? getPropertyValueAsString("username", errors) : "";
    username = (username == null) ? "" : username;

    // Get the password as a DecryptedValue
    String password = "";
    if (hasProperty("password") && StringUtils.isNotEmpty(getPropertyValueAsString("password", errors)))
    {
      try
      {
        password = getProperty("password").getDecryptedValue();
      }
      catch (Exception error)
      {
        errors.add("Password property value is invalid.");
      }
    }

    // qos is an integer from the set [0, 1, or 2]
    int qos = 1;
    if (hasProperty("qos"))
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
            errors.add(LOGGER.translate("Property value for QOS is not valid ({0}). Using default of 0 instead.", qosValue));
        } catch (NumberFormatException error) {
          errors.add(LOGGER.translate("Property value for QOS is not valid ({0}). Using default of 0 instead.", qosValue));
        }
      }
    }
    LOGGER.info("Property value qos = " + qos);

    // retain is a boolean value
    boolean isRetain = false;
    if (hasProperty("retain"))
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

    //clientId is a string value
    String clientId = "";
    if (hasProperty("clientId")) {
      clientId = getPropertyValueAsString("clientId", errors).trim();
    }
    if (StringUtils.isEmpty(clientId))
    {
      // mandatory property value is null or empty -> report this error
      errors.add("Property value clientId cannot be NULL or EMPTY.");
    }
    LOGGER.info("Property value clientId = " + clientId);

    // cleanSession is a boolean value
    boolean cleanSession = false;
    if (hasProperty("cleanSession"))
    {
      String cleanSessionValue = getPropertyValueAsString("cleanSession", errors);
      if (StringUtils.isEmpty(cleanSessionValue))
      {
        // mandatory property value is null or empty -> report this error
        errors.add("Property value cleanSession cannot be NULL or EMPTY.");
      } else {
        cleanSession = Boolean.parseBoolean(cleanSessionValue);
      }
    }
    LOGGER.info("Property value cleanSession = " + cleanSession);

    // keepAliveInterval is an integer
    int keepAliveInterval = 60;
    if (hasProperty("keepAliveInterval")) {
      String keepAliveIntervalValue = getPropertyValueAsString("keepAliveInterval", errors);
      if (StringUtils.isEmpty(keepAliveIntervalValue)) {
        // mandatory property value is null or empty -> report this error
        errors.add("Property value keepAliveInterval cannot be NULL or EMPTY.");
      } else {
        try {
          keepAliveInterval = Integer.parseInt(keepAliveIntervalValue);
          if (keepAliveInterval < 0)
            errors.add("Property value keepAliveInterval must be >= 0.");
        } catch (NumberFormatException error) {
          errors.add("Property value keepAliveInterval is not valid.");
        }
      }
    }
    LOGGER.info("Property value keepAliveInterval = " + keepAliveInterval);

    // autoReconnect is a boolean value
    boolean autoReconnect = true;
    if (hasProperty("autoReconnect"))
    {
      String autoReconnectValue = getPropertyValueAsString("autoReconnect", errors);
      if (StringUtils.isEmpty(autoReconnectValue))
      {
        // mandatory property value is null or empty -> report this error
        errors.add("Property value autoReconnect cannot be NULL or EMPTY.");
      } else {
        autoReconnect = Boolean.parseBoolean(autoReconnectValue);
      }
    }
    LOGGER.info("Property value autoReconnect = " + autoReconnect);

    config = new MqttTransportConfig(host, port, (topic == null) ? "": topic, isUseSSL, username, password, qos, isRetain, errors, clientId, cleanSession, keepAliveInterval, autoReconnect);
  }

  private void startTransport()
  {
    startExecutor();
  }

  private void stopTransport()
  {
    disconnect();
    stopExecutor();
  }

  private void connect() throws Exception
  {
    if (mqttClientManager == null)
      mqttClientManager = new MqttClientManager(config, LOGGER);
    if (!mqttClientManager.isConnected()) {
      switch(getType()) {
        case INBOUND:
          mqttClientManager.subscribe(this); // inbound transport is a callback
          break;
        case OUTBOUND:
          mqttClientManager.connect(null); // no callback is needed for outbound transport
          break;
      }
    }
  }

  private void disconnect()
  {
    if (mqttClientManager != null)
    {
      mqttClientManager.disconnect();
      mqttClientManager = null;
    }
  }

  private void startExecutor() {
    if (executor == null) {
      executor = Executors.newSingleThreadScheduledExecutor();
      executor.scheduleAtFixedRate(this, 1, 3, TimeUnit.SECONDS);
    }
  }

  private void stopExecutor() {
    if (executor != null)
    {
      try
      {
        executor.shutdownNow();
        executor.awaitTermination(3, TimeUnit.SECONDS);
      }
      catch (Throwable e)
      {
        // pass
      }
      finally
      {
        executor = null;
      }
    }
  }

  /**
   * Checks if a property is valid, then makes sure it isn't null or empty.
   *
   * @param propertyName
   *          The name of the property to get
   * @return  null if the property value is invalid or null, or empty string.
   *          The property string value otherwise.
   */
  private String getPropertyValueAsString(String propertyName, List<String> errors)
  {
    Property property = getProperty(propertyName);
    if (property.isValid())
    {
      if (property.getValue() == null)
      {
        // this is a valid property with value null
        LOGGER.trace("Property {0} is NULL.", propertyName);
      } else {
        String propertyValue = property.getValueAsString();
        if (propertyValue.isEmpty())
        {
          // this is a valid property with value of empty String
          LOGGER.trace("Property {0} is EMPTY.", propertyName);
        }
        // return property value or empty String for valid property
        return propertyValue;
      }
    } else {
      LOGGER.trace("Property {0} is NOT VALID.", propertyName);
      errors.add(LOGGER.translate("Property {0} is NOT VALID.", propertyName));
    }
    // return null when either property value is invalid, or null
    return null;
  }
}
