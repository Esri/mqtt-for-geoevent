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
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.InboundTransportBase;
import com.esri.ges.transport.TransportDefinition;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MqttInboundTransport extends InboundTransportBase implements MqttTransport, Runnable
{
  private static final BundleLogger LOGGER = BundleLoggerFactory.getLogger(MqttInboundTransport.class);

  private MqttTransportConfig config;
  private MqttClientManager mqttClientManager;
  private ScheduledExecutorService executor;

  public MqttInboundTransport(TransportDefinition definition) throws ComponentException
  {
    super(definition);
  }

  public void start()
  {
    switch (getRunningState())
    {
      case STARTED:
      case STARTING:
        break;
      case STOPPED:
      case STOPPING:
      case ERROR:
        LOGGER.trace("Starting MQTT inbound transport...");
        this.setRunningState(RunningState.STARTING);
        try
        {
          mqttClientManager = new MqttClientManager(config, LOGGER);
          executor = Executors.newSingleThreadScheduledExecutor();
          executor.scheduleAtFixedRate(this, 1, 3, TimeUnit.SECONDS);
          // STARTED state is set in the thread
        } catch (Exception e) {
          setRunningState(RunningState.ERROR);
          disconnectClient();
          // report an error
          String errorMsg = LOGGER.translate("{0} {1}", LOGGER.translate("INIT_ERROR", "inbound"), e.getMessage());
          LOGGER.error(errorMsg, e);
          setErrorMessage(errorMsg);
        }
      default:
    }
  }

  @Override
  public void run()
  {
    try
    {
      mqttClientManager.subscribe(new MqttCallback() {
        @Override
        public void messageArrived(String topic, MqttMessage message) throws Exception
        {
          try
          {
            LOGGER.debug("Message arrived on topic {0}: ( {1} )", topic, message);
            receive(message.getPayload());
          }
          catch (RuntimeException e)
          {
            LOGGER.debug("ERROR_PUBLISHING", e);
          }
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token)
        {
          // not used
        }

        @Override
        public void connectionLost(Throwable cause)
        {
          LOGGER.debug("CONNECTION_LOST", cause, cause.getLocalizedMessage());
        }
      });
      setRunningState(RunningState.STARTED);
      LOGGER.trace("MQTT inbound transport has started mqtt client. Transport state is set to STARTED.");
    }
    catch (Throwable ex)
    {
      setRunningState(RunningState.ERROR);
      disconnectClient();
      // report an error
      String errorMsg = LOGGER.translate("UNEXPECTED_ERROR", ex.getMessage());
      LOGGER.debug(errorMsg, ex);
      setErrorMessage(errorMsg);
    }
  }

  private void receive(byte[] bytes)
  {
    if (bytes != null && bytes.length > 0)
    {
      LOGGER.trace("Received {0} bytes", bytes.length);

      String str = new String(bytes);
      str = str + '\n';

      LOGGER.trace("Byte String received {0}", str);

      byte[] newBytes = str.getBytes();

      ByteBuffer bb = ByteBuffer.allocate(newBytes.length);
      try
      {
        bb.put(newBytes);
        bb.flip();
        byteListener.receive(bb, "");
        bb.clear();

        LOGGER.trace("{0} received bytes sent on to the adaptor.", newBytes.length);
      }
      catch (BufferOverflowException boe)
      {
        LOGGER.debug("BUFFER_OVERFLOW_ERROR", boe);
        bb.clear();
      }
      catch (Exception e)
      {
        LOGGER.debug("UNEXPECTED_ERROR2", e);
        disconnectClient();
        setRunningState(RunningState.ERROR);
        setErrorMessage("Unexpected Error: " + e.getMessage());
      }
    }
  }

  public synchronized void stop()
  {
    switch (getRunningState()) {
      case STOPPED:
      case STOPPING:
      case ERROR:
        break;
      default:
        setRunningState(RunningState.STOPPING);
        LOGGER.trace("Stopping MQTT inbound transport...");
        if (executor != null)
        {
          try
          {
            executor.shutdownNow();
            executor.awaitTermination(3, TimeUnit.SECONDS);
          }
          catch (Throwable e)
          { // pass
          }
          finally
          {
            executor = null;
          }
        }
        disconnectClient();
        setRunningState(RunningState.STOPPED);
        LOGGER.trace("MQTT inbound transport has stopped mqtt client and worker thread. Transport state is set to STOPPED.");
    }
  }

  @Override
  public boolean isClusterable()
  {
    return false;
  }

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
          manager.connect();
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
