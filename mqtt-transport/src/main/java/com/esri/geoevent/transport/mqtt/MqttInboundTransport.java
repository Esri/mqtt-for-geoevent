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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.InboundTransportBase;
import com.esri.ges.transport.TransportDefinition;

public class MqttInboundTransport extends InboundTransportBase implements Runnable
{

	private static final BundleLogger log = BundleLoggerFactory.getLogger(MqttInboundTransport.class);

  private MqttClientManager         mqttClientManager      = new MqttClientManager(log);
	private MqttClient mqttClient;
  private ScheduledExecutorService  executor;
  private boolean                   isStarted = false;

	public MqttInboundTransport(TransportDefinition definition) throws ComponentException
	{
		super(definition);
	}

	public void start() throws RunningException
	{
    isStarted = true;
    if (getRunningState() == RunningState.STOPPED)
    {
      log.trace("Starting Transport");

      this.setRunningState(RunningState.STARTING);
		try
		{
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(this, 1, 3, TimeUnit.SECONDS);

        // STARTED state is set in the thread
			}
      catch (Exception e)
			{
        String errormsg = log.translate("{0} {1}", log.translate("INIT_ERROR", "inbound"), e.getMessage());
        log.error(errormsg, e);
        setErrorMessage(errormsg);
        stop();
        setRunningState(RunningState.ERROR);
			}
		}
	}

	@Override
	public void run()
	{
    if (mqttClient == null || !mqttClient.isConnected())
    {
      log.info("Creating new MQTT Client");
		try
		{
        if (mqttClient != null)
        {
          try
          {
            log.info("Disconnecting previous MQTT Client");
            mqttClientManager.disconnectMqtt(mqttClient);
          }
          finally
          {
            mqttClient = null;
          }
        }

        mqttClientManager.applyProperties(this);
        mqttClient = mqttClientManager.createMqttClient(new MqttCallback()
			{

				@Override
				public void messageArrived(String topic, MqttMessage message) throws Exception
				{
					try
					{
                log.trace("Message arrived on topic {0}: ( {1} )", topic, message);
                receive(message.getPayload());
						}
              catch (RuntimeException e)
					{
                log.warn("ERROR_PUBLISHING", e);
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
              log.warn("CONNECTION_LOST", cause, cause.getLocalizedMessage());
				}
			});

        mqttClient.connect();
        mqttClient.subscribe(mqttClientManager.getTopic(), mqttClientManager.getQos());

			setRunningState(RunningState.STARTED);
        log.info("Transport started mqtt client. Transport state set to STARTED.");

      }
      catch (Throwable ex)
		{
        String errormsg = log.translate("UNEXPECTED_ERROR", ex.getMessage());
        log.error(errormsg, ex);
        setErrorMessage(errormsg);
        disconnectClient();
			setRunningState(RunningState.ERROR);
      }
		}
	}

	private void receive(byte[] bytes)
	{
		if (bytes != null && bytes.length > 0)
		{
      log.debug("Received {0} bytes", bytes.length);

			String str = new String(bytes);
			str = str + '\n';

      log.trace("Byte String received {0}", str);

			byte[] newBytes = str.getBytes();

			ByteBuffer bb = ByteBuffer.allocate(newBytes.length);
			try
			{
				bb.put(newBytes);
				bb.flip();
				byteListener.receive(bb, "");
				bb.clear();

        log.trace("{0} received bytes sent on to the adaptor.", newBytes.length);
				}
      catch (BufferOverflowException boe)
			{
				log.error("BUFFER_OVERFLOW_ERROR", boe);
				bb.clear();
      }
      catch (Exception e)
			{
				log.error("UNEXPECTED_ERROR2", e);
        disconnectClient();
				setRunningState(RunningState.ERROR);
				setErrorMessage("Unexcpected Error: " + e.getMessage());
			}
		}
	}

	public synchronized void stop()
	{
    isStarted = false;
    if (getRunningState() != RunningState.STOPPING && getRunningState() != RunningState.STOPPED)
		{
      setRunningState(RunningState.STOPPING);
      log.trace("Stopping Transport");

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

      log.info("Transport stopped mqtt client and worker thread. Transport state set to STOPPED.");
    }
    setRunningState(RunningState.STOPPED);
		}

  @Override
  public boolean isClusterable()
  {
    return false;
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
  public void afterPropertiesSet()
  {
    log.info("Setting Prpoerties, resetting client, and updating state");
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
