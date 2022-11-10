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

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.GeoEventAwareTransport;
import com.esri.ges.transport.OutboundTransportBase;
import com.esri.ges.transport.TransportDefinition;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MqttOutboundTransport extends OutboundTransportBase implements MqttTransport, GeoEventAwareTransport, Runnable
{
  private static final BundleLogger LOGGER = BundleLoggerFactory.getLogger(MqttOutboundTransport.class);

  private MqttTransportConfig config;

  private MqttClientManager mqttClientManager;
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

  public MqttOutboundTransport(TransportDefinition definition) throws ComponentException
  {
    super(definition);
  }

  @Override
  public void start()
  {
    switch (getRunningState()) {
      case STARTED:
      case STARTING:
        break;
      case STOPPED:
      case STOPPING:
      case ERROR:
        LOGGER.trace("Starting MQTT outbound transport...");
        setRunningState(RunningState.STARTING);
        try
        {
          if (mqttClientManager == null)
            mqttClientManager = new MqttClientManager(config, LOGGER);
          if (!mqttClientManager.isConnected())
            mqttClientManager.connect(null); // no callback function
          setRunningState(RunningState.STARTED);
          LOGGER.trace("Transport started mqtt client. Transport state set to STARTED.");
        } catch (Exception e) {
          setRunningState(RunningState.ERROR);
          disconnectClient();
          // report an error
          String errorMsg = LOGGER.translate("{0} {1}", LOGGER.translate("INIT_ERROR", "outbound"), e.getMessage());
          LOGGER.error(errorMsg, e);
          setErrorMessage(errorMsg);
        }
        break;
      default:
        LOGGER.trace("Cannot start MQTT outbound transport: transport is unavailable.");
    }
  }

  @Override
  public void receive(ByteBuffer buffer, String channelId)
  {
    receive(buffer, channelId, null);
  }

  @Override
  public void receive(ByteBuffer buffer, String channelID, GeoEvent geoEvent)
  {
    byte[] b = new byte[buffer.remaining()];
    buffer.get(b);
    try
    {
      mqttClientManager.publish(b, geoEvent);
      setErrorMessage(null);
    } catch (Exception e) {
      String errorMsg = LOGGER.translate("ERROR_PUBLISHING", e.getMessage());
      LOGGER.debug(errorMsg, e);
      setErrorMessage(errorMsg);
      setRunningState(RunningState.ERROR);
      executor.schedule(this, 3, TimeUnit.SECONDS);
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
        LOGGER.trace("Stopping MQTT outbound transport...");
        disconnectClient();
        setRunningState(RunningState.STOPPED);
        LOGGER.trace("MQTT outbound transport stopped mqtt client. Transport state is set to STOPPED.");
        break;
      default:
        LOGGER.trace("Cannot stop MQTT outbound transport: transport is unavailable.");
    }
  }

  @Override
  public void run()
  {
    if (getRunningState() == RunningState.ERROR)
      setRunningState(RunningState.STARTED);
  }

  /*
   * (non-Javadoc)
   *
   * @see com.esri.ges.transport.TransportBase#afterPropertiesSet()
   */
  @Override
  public void afterPropertiesSet()
  {
    super.afterPropertiesSet();
    // time to read transport configuration
    config = new MqttTransportConfigReader(this).readConfig();
  }

  @Override public void validate() throws ValidationException
  {
    StringBuilder sb = new StringBuilder();
    // validate parent classes
    try {
      super.validate();
    } catch (ValidationException e) {
      sb.append(e.getMessage()).append("\n");
    }
    // validate transport config next
    if (config != null) {
      if (config.hasErrors())
      {
        for (String error : config.getErrors())
          sb.append(error).append("\n");
      } else {
        // test connection
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

  @Override public BundleLogger getLogger()
  {
    return LOGGER;
  }

  private void disconnectClient()
  {
    if (mqttClientManager != null)
      mqttClientManager.disconnect();
  }
}
