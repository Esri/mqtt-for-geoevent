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

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.client.mqttv3.MqttClient;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.GeoEventAwareTransport;
import com.esri.ges.transport.OutboundTransportBase;
import com.esri.ges.transport.TransportDefinition;

public class MqttOutboundTransport extends OutboundTransportBase implements GeoEventAwareTransport, Runnable
{

  private static final BundleLogger log               = BundleLoggerFactory.getLogger(MqttOutboundTransport.class);

  private final MqttClientManager   mqttClientManager = new MqttClientManager(log);
  private MqttClient                mqttClient;
  private ScheduledExecutorService  executor          = Executors.newSingleThreadScheduledExecutor();
  private boolean                   isStarted         = false;

  public MqttOutboundTransport(TransportDefinition definition) throws ComponentException
  {
    super(definition);
  }

  @Override
  public void start() throws RunningException
  {
    isStarted = true;
    if (getRunningState() == RunningState.STOPPED)
    {
      log.trace("Starting MQTT Outbound Transport");
      setRunningState(RunningState.STARTING);

      try
      {
        mqttClientManager.applyProperties(this);
        mqttClient = mqttClientManager.createMqttClient();
        mqttClient.connect();

        setRunningState(RunningState.STARTED);
        log.trace("Transport started mqtt client. Transport state set to STARTED.");
      }
      catch (Exception e)
      {
        String errormsg = log.translate("{0} {1}", log.translate("INIT_ERROR", "outbound"), e.getMessage());
        log.error(errormsg, e);
        setRunningState(RunningState.ERROR);
        setErrorMessage(errormsg);
      }
    }
    else
    {
      log.trace("Cannot start transport: Not in STOPPED state.");
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
    String topic = mqttClientManager.getTopic();

    if (geoEvent != null && topic.contains("$"))
    {
      log.trace("received geoEvent, creating output topic from field values using template {0}", topic);
      // Do field value substitution like "${field1}/${field2}"
      topic = geoEvent.formatString(topic);
    }
    log.debug("Publishing outgoing bytes to topic {0}: {1}", topic, geoEvent);

    if (mqttClientManager.isTopicValid(topic))
    {
      try
      {
        byte[] b = new byte[buffer.remaining()];
        buffer.get(b);

        if (mqttClient == null || !mqttClient.isConnected())
        {
          mqttClientManager.disconnectMqtt(mqttClient);
          mqttClientManager.applyProperties(this);
          mqttClient = mqttClientManager.createMqttClient();
          mqttClient.connect();
        }
        mqttClient.publish(topic, b, mqttClientManager.getQos(), mqttClientManager.isRetain());
        setErrorMessage(null);
      }
      catch (Exception e)
      {
        try
        {
          String errormsg = log.translate("ERROR_PUBLISHING", e.getMessage());
          log.debug(errormsg, e);
          setErrorMessage(errormsg);
          mqttClientManager.disconnectMqtt(mqttClient);
          setRunningState(RunningState.ERROR);
          executor.schedule(this, 3, TimeUnit.SECONDS);
        }
        finally
        {
          mqttClient = null;
        }
      }
    }
    else
    {
      log.debug("GeoEvent Topic {0} is not valid, GeoEvent not published to MQTT output: {1}", topic, geoEvent);
    }
  }

  @Override
  public synchronized void stop()
  {
    isStarted = false;
    log.trace("Stopping Transport");
    if (getRunningState() != RunningState.STOPPING && getRunningState() != RunningState.STOPPED)
    {
      setRunningState(RunningState.STOPPING);

      disconnectClient();

      log.trace("Transport stopped mqtt client. Transport state set to STOPPED.");
    }
    setRunningState(RunningState.STOPPED);
  }

  private void disconnectClient()
  {
    try
    {
      mqttClientManager.disconnectMqtt(mqttClient);
    }
    catch (Throwable e)
    { // pass
    }
    finally
    {
      mqttClient = null;
    }
  }

  @Override
  public void run()
  {
    if (getRunningState() == RunningState.ERROR)
    {
      setRunningState(RunningState.STARTED);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.esri.ges.transport.TransportBase#afterPropertiesSet()
   */
  @Override
  public void afterPropertiesSet()
  {
    log.trace("Setting Prpoerties, resetting client, and updating state");
    super.afterPropertiesSet();
    disconnectClient();
    setErrorMessage("");
    if (isStarted)
    {
      setRunningState(RunningState.STARTED);
    }
    else
    {
      setRunningState(RunningState.STOPPED);
    }
  }

}
